package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
	"github.com/asheshgoplani/agent-deck/internal/session"
	"github.com/asheshgoplani/agent-deck/internal/testutil"
)

func TestDevbox17CodexNativeBusyQueue(t *testing.T) {
	for _, mode := range []string{"success", "ambiguous", "delayed", "unsupported", "guarded", "expired"} {
		t.Run(mode, func(t *testing.T) { testDevbox17NativeQueue(t, mode) })
	}
}

func testDevbox17NativeQueue(t *testing.T, mode string) {
	t.Helper()
	testutil.RequireTmux(t)
	root := t.TempDir()
	home := filepath.Join(root, "codex")
	t.Setenv("CODEX_HOME", home)
	t.Setenv("AGENTDECK_SEND_WORKER_POLL", "30ms")
	t.Setenv("AGENTDECK_SEND_LAND_WINDOW", "300ms")
	profile := "_test_devbox17_native_" + mode
	inst, storage := newIssue2549Instance(t, root, profile, "native-busy")
	inst.CodexSessionID = issue2549NewThread
	startRotatedCodexPane(t, root, home, inst, inst.CodexSessionID, inst.CodexSessionID)
	writeLegacyCodexRollout(t, home, inst.CodexSessionID, "23")
	rollout := filepath.Join(home, "sessions", "2026", "09", "23", "rollout-test-"+inst.CodexSessionID+".jsonl")
	bin := filepath.Join(root, "queue-bin", "codex")
	if err := os.MkdirAll(filepath.Dir(bin), 0o700); err != nil {
		t.Fatal(err)
	}
	inst.Command = "CODEX_HOME=" + home + " " + bin
	t.Setenv("CODEX_HOME", filepath.Join(root, "sender-codex-home"))
	if err := storage.SaveWithGroups([]*session.Instance{inst}, nil); err != nil {
		t.Fatal(err)
	}
	seedDeferHookFile(t, inst.ID, "UserPromptSubmit", inst.CodexSessionID, "running")
	if status, err := fetchHookDrivenStatus(profile, inst.ID); err != nil || status != "running" {
		t.Fatalf("busy fixture status = %q, %v", status, err)
	}
	prev := loadUserConfigForSend
	loadUserConfigForSend = func() (*session.UserConfig, error) {
		return &session.UserConfig{SendTransport: "auto"}, nil
	}
	t.Cleanup(func() { loadUserConfigForSend = prev })
	forbidSendChild(t)

	const script = `#!/bin/sh
if [ "$1" = queue ] && [ "$2" = --help ]; then
    [ "$DEVBOX17_MODE" != unsupported ] || exit 1
    printf 'Usage: codex queue --thread THREAD --message TEXT\n'
    exit 0
fi
cat "$DEVBOX17_RECORD" > "$DEVBOX17_CLAIM" || exit 1
printf '%s\000' "$CODEX_HOME" "$@" >> "$DEVBOX17_CALLS"
[ "$DEVBOX17_MODE" != ambiguous ] || exit 7
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	budget := 5 * time.Second
	if mode == "unsupported" || mode == "guarded" {
		budget = 500 * time.Millisecond
	}
	if mode == "expired" {
		budget = -time.Second
	}
	f := retryFixture{profile: profile, dir: sendQueueDir(storage), target: inst}
	if err := os.MkdirAll(f.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	rec := f.queue(t, budget, "cli")
	rec.Tool, rec.Message = "codex", "follow  up\nwith this\tspacing"
	rec.RequireInputPrompt = mode == "guarded"
	appendInput := func(id, text string) {
		t.Helper()
		body, err := json.Marshal(text)
		if err != nil {
			t.Fatal(err)
		}
		issue2836Append(t, rollout, fmt.Sprintf(`{"timestamp":%q,"type":"response_item","payload":{"id":%q,"type":"message","role":"user","content":[{"type":"input_text","text":%s}]}}`, time.Now().UTC().Format(time.RFC3339Nano), id, body))
	}
	appendInput("before-send", rec.Message)
	if err := sendqueue.Save(f.dir, rec); err != nil {
		t.Fatal(err)
	}
	claimPath, callPath := filepath.Join(root, "claim.json"), filepath.Join(root, "calls")
	t.Setenv("DEVBOX17_MODE", mode)
	t.Setenv("DEVBOX17_RECORD", filepath.Join(f.dir, rec.SendID+".json"))
	t.Setenv("DEVBOX17_CLAIM", claimPath)
	t.Setenv("DEVBOX17_CALLS", callPath)
	deliverQueuedAsync(profile, f.dir, rec)
	if mode == "unsupported" || mode == "guarded" || mode == "expired" {
		if _, err := os.Stat(callPath); !os.IsNotExist(err) || rec.State != sendqueue.StateFailed || rec.Attempts != 0 {
			t.Fatalf("busy target should stay untyped: state=%s attempts=%d queue log=%v", rec.State, rec.Attempts, err)
		}
		return
	}
	want := strings.Join([]string{home, "queue", "--thread", inst.CodexSessionID, "--message", rec.Message, ""}, "\x00")
	assertOneCall := func() {
		t.Helper()
		calls, err := os.ReadFile(callPath)
		if err != nil || string(calls) != want {
			t.Fatalf("expected one native enqueue to the exact thread: state=%s attempts=%d args=%q err=%v", rec.State, rec.Attempts, calls, err)
		}
	}
	assertOneCall()
	wantDelivery := "queued_native"
	if mode == "ambiguous" {
		wantDelivery = "no_evidence"
	}
	if rec.State != sendqueue.StateTyped || rec.Verdict != "unknown" || rec.Attempts != 1 || rec.DeliveryEvidence["delivery"] != wantDelivery || rec.DeliveryEvidence["submitted"] != false {
		t.Fatalf("enqueue is not landed: %+v", rec)
	}
	claimData, err := os.ReadFile(claimPath)
	if err != nil {
		t.Fatal(err)
	}
	var claim sendqueue.Record
	if err := json.Unmarshal(claimData, &claim); err != nil || claim.State != sendqueue.StateTyping || claim.Attempts != 1 ||
		claim.TranscriptPath != rollout || claim.TranscriptFrom <= 0 || claim.DeliveryEvidence["transport"] != "codex_queue" {
		t.Fatalf("native attempt was not claimed before enqueue: %+v, %v", claim, err)
	}

	appendInput("similar-text", "follow up with this spacing")
	issue2836Append(t, rollout,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"unrelated"}}`,
		`{"type":"queue-operation","operation":"enqueue","content":"follow  up\nwith this\tspacing"}`)
	if guard, err := acquireCodexAcceptanceGuard(inst, time.Second); err == nil {
		guard.Release()
		t.Fatal("a different turn or different text cleared the submission marker")
	}
	if mode == "ambiguous" {
		// Recreate the state a dead worker leaves before saving the result.
		if err := sendqueue.Save(f.dir, &claim); err != nil {
			t.Fatal(err)
		}
		deliverQueuedAsync(profile, f.dir, &claim)
	}
	if mode != "success" {
		watchQueuedSend(profile, f.dir, rec.SendID)
		settled, err := sendqueue.Load(f.dir, rec.SendID)
		if err != nil || settled.State != sendqueue.StateTyped || !settled.Settled || settled.Verdict != "unknown" || settled.LandedRowID != "" {
			t.Fatalf("nonmatching user input counted as landed: %+v, %v", settled, err)
		}
	}
	assertOneCall()

	// In the delayed cases, the watcher has already settled. send-status
	// must upgrade the original receipt when the exact input arrives.
	if mode == "delayed" {
		// Put the exact input more than 64 MiB after the attempt's offset.
		padding := `{"type":"event_msg","padding":"` + strings.Repeat("x", 64<<10) + `"}`
		rows := make([]string, 1024)
		for index := range rows {
			rows[index] = padding
		}
		issue2836Append(t, rollout, rows...)
	}
	appendInput("exact-input", rec.Message)
	if mode == "success" {
		watchQueuedSend(profile, f.dir, rec.SendID)
	}
	output := captureStdout(t, func() { handleSessionSendStatus(profile, []string{rec.SendID, "--json"}) })
	var receipt sendqueue.Record
	if err := json.Unmarshal([]byte(output), &receipt); err != nil || receipt.SendID != rec.SendID || receipt.SessionID != inst.ID ||
		receipt.State != sendqueue.StateLanded || receipt.Verdict != "delivered" || receipt.Settled || receipt.Attempts != 1 || receipt.LandedRowID != "exact-input" {
		t.Fatalf("send-status receipt: %s (%v)", output, err)
	}
	persisted, err := sendqueue.Load(f.dir, rec.SendID)
	if err != nil || persisted.State != sendqueue.StateLanded || persisted.LandedRowID != "exact-input" {
		t.Fatalf("landed receipt was not saved: %+v, %v", persisted, err)
	}
	guard, err := acquireCodexAcceptanceGuard(inst, time.Second)
	if err != nil {
		t.Fatalf("exact user input did not resolve the submission marker: %v", err)
	}
	guard.Release()
	assertOneCall()
}
