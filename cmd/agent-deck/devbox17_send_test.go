package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/health"
	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
	"github.com/asheshgoplani/agent-deck/internal/session"
	"github.com/asheshgoplani/agent-deck/internal/testutil"
)

func TestDevbox17QueuedBusyFailureNotice(t *testing.T) {
	t.Cleanup(session.SetCommsLedgerForTest(true))
	f := newRetryFixture(t, "_test_devbox17_failure")
	starts := stubChild(t, -1)
	storage, instances, groups, err := loadSessionData(f.profile)
	if err != nil {
		t.Fatal(err)
	}
	target := instanceByID(instances, f.target.ID)
	target.Tool, target.CodexSessionID = "codex", issue2549OldThread
	if err := saveSessionData(storage, instances, groups); err != nil {
		t.Fatal(err)
	}
	_ = storage.Close()
	seedDeferHookFile(t, target.ID, "UserPromptSubmit", target.CodexSessionID, "running")
	rec := f.queue(t, -time.Second, f.senderID)
	deliverQueued(f.profile, f.dir, rec)
	if rec.State != sendqueue.StateFailed || rec.Attempts != 0 || len(starts()) != 0 {
		t.Fatalf("expired send: %+v, starts=%d", rec, len(starts()))
	}
	// Repeated dispatch must keep one durable directed urgent notice.
	if !notifySenderOfFailedSend(f.profile, rec) {
		t.Fatal("inbox did not own notice")
	}
	events, err := session.ReadInboxEvents(f.senderID)
	if err != nil || len(events) != 1 {
		t.Fatalf("inbox: %+v, %v", events, err)
	}
	e := events[0]
	if e.TargetSessionID != f.senderID || e.TargetKind != session.InboxTargetKindReply || e.Tier != session.TurnTierUrgent || e.LastOutputHash != "send:"+rec.SendID || !strings.Contains(e.Text, rec.Reason) || !strings.Contains(e.Text, rec.SessionTitle) {
		t.Fatalf("failure must address the actual sender/send/target/reason: %+v", e)
	}
	if strings.Contains(e.Text, "resending is safe") {
		t.Fatalf("notice must not promise resend safety: %s", e.Text)
	}
	spool, err := session.ReadCommsSpool(rec.SessionID)
	if err != nil || len(spool) != 1 || spool[0].Event != "async-inbox" || spool[0].From != f.senderID {
		t.Fatalf("inbox ownership lost: %+v %v", spool, err)
	}
	dir, err := session.HealthLogDir(f.profile)
	if err != nil {
		t.Fatal(err)
	}
	journal, _, err := health.ReadEvents(dir, time.Time{}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	finals := 0
	for _, event := range journal {
		if event.Kind == health.KindSend && event.Detail["final"] == true && event.Detail["send_id"] == rec.SendID {
			finals++
		}
	}
	if finals != 1 {
		t.Fatalf("final journal entries=%d, want 1", finals)
	}
}

// This fixture owns a real private tmux pane but never starts a provider.
// The fake codex command records the native queue arguments and home.
func TestDevbox17CodexNativeBusyQueue(t *testing.T) {
	for _, mode := range []string{"success", "target_home", "ambiguous", "unsupported", "stale_thread", "foreign_thread", "guarded", "slash", "tmux", "start_failed"} {
		t.Run(mode, func(t *testing.T) { devbox17NativeQueue(t, mode) })
	}
}

func devbox17NativeQueue(t *testing.T, mode string) {
	testutil.RequireTmux(t)
	root := t.TempDir()
	home := filepath.Join(root, "codex")
	t.Setenv("CODEX_HOME", home)
	t.Setenv("AGENTDECK_SEND_WORKER_POLL", "10ms")
	profile := "_test_devbox17_native_" + mode
	inst, storage := newIssue2549Instance(t, root, profile, "native-busy")
	inst.CodexSessionID = issue2549NewThread
	startRotatedCodexPane(t, root, home, inst, issue2549NewThread, issue2549NewThread)
	writeLegacyCodexRollout(t, home, issue2549NewThread, "23")
	instances := []*session.Instance{inst}
	if mode == "target_home" {
		inst.Command = "CODEX_HOME=" + home + " codex"
		t.Setenv("CODEX_HOME", filepath.Join(root, "sender-codex-home"))
	}
	if mode == "foreign_thread" {
		peer := session.NewInstanceWithTool("foreign-owner", root, "codex")
		peer.CodexSessionID = inst.CodexSessionID
		startRotatedCodexPane(t, filepath.Join(root, "peer"), home, peer, inst.CodexSessionID, inst.CodexSessionID)
		instances = append(instances, peer)
	}
	if err := storage.SaveWithGroups(instances, nil); err != nil {
		t.Fatal(err)
	}
	seedDeferHookFile(t, inst.ID, "UserPromptSubmit", inst.CodexSessionID, "running")
	log := filepath.Join(root, "queue-call")
	bin := filepath.Join(root, "bin", "codex")
	script := "#!/bin/sh\nif [ \"$1 $2\" = 'queue --help' ]; then echo 'Usage: codex queue --thread <THREAD> --message <TEXT>'; exit 0; fi\nprintf '%s\\n' \"$CODEX_HOME\" \"$@\" >> '" + log + "'\n"
	if mode == "ambiguous" {
		script += "exit 7\n"
	}
	if mode == "unsupported" {
		script = "#!/bin/sh\nexit 1\n"
	}
	if mode == "start_failed" {
		script = "#!/bin/sh\necho 'queue --thread --message'\nprintf '#!/devbox17-missing-interpreter\\n' > \"$0\"\n"
	}
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", filepath.Join(root, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	prev := loadUserConfigForSend
	loadUserConfigForSend = func() (*session.UserConfig, error) {
		value := "auto"
		if mode == "tmux" {
			value = "tmux"
		}
		return &session.UserConfig{SendTransport: value}, nil
	}
	t.Cleanup(func() { loadUserConfigForSend = prev })
	starts := stubChild(t, -1)
	f := retryFixture{profile: profile, dir: t.TempDir(), target: inst}
	rec := f.queue(t, 300*time.Millisecond, "cli")
	rec.Tool, rec.Message = "codex", "follow up while the draft stays intact"
	if mode == "guarded" {
		rec.RequireInputPrompt = true
	}
	if mode == "slash" {
		rec.Message = "/help"
	}
	if mode == "stale_thread" {
		inst.CodexSessionID = issue2549OldThread
		if err := storage.SaveWithGroups([]*session.Instance{inst}, nil); err != nil {
			t.Fatal(err)
		}
		seedDeferHookFile(t, inst.ID, "UserPromptSubmit", inst.CodexSessionID, "running")
	}
	if err := sendqueue.Save(f.dir, rec); err != nil {
		t.Fatal(err)
	}
	if err := inst.GetTmuxSession().SendKeys("operator draft"); err != nil {
		t.Fatal(err)
	}
	before, err := inst.GetTmuxSession().CapturePaneFresh()
	until := time.Now().Add(2 * time.Second)
	for err == nil && !strings.Contains(before, "operator draft") && time.Now().Before(until) {
		time.Sleep(10 * time.Millisecond)
		before, err = inst.GetTmuxSession().CapturePaneFresh()
	}
	if err != nil || !strings.Contains(before, "operator draft") {
		t.Fatalf("draft fixture: %q %v", before, err)
	}
	deliverQueuedMode(profile, f.dir, rec, false)
	data, err := os.ReadFile(log)
	if mode != "success" && mode != "target_home" && mode != "ambiguous" {
		wantAttempts := 0
		if mode == "start_failed" {
			wantAttempts = 1
		}
		if len(data) != 0 || len(starts()) != 0 || rec.Attempts != wantAttempts || rec.State != sendqueue.StateFailed {
			t.Fatalf("pre-write refusal: mode=%s record=%+v keys=%d args=%q", mode, rec, len(starts()), data)
		}
		return
	}
	want := home + "\nqueue\n--thread\n" + inst.CodexSessionID + "\n--message\n" + rec.Message + "\n"
	if err != nil || string(data) != want || len(starts()) != 0 || rec.Attempts != 1 || rec.State != sendqueue.StateTyped || rec.Verdict != "unknown" {
		t.Fatalf("native pending enqueue: state=%s verdict=%s attempts=%d keys=%d args=%q err=%v", rec.State, rec.Verdict, rec.Attempts, len(starts()), data, err)
	}
	after, _ := inst.GetTmuxSession().CapturePaneFresh()
	if before != after {
		t.Fatalf("native queue changed pane: before=%q after=%q", before, after)
	}
	// No uptake: a second sender must not cross the unresolved enqueue.
	if guard, err := acquireCodexAcceptanceGuard(inst, time.Second); err == nil {
		guard.Release()
		t.Fatal("enqueue alone cleared unresolved ownership")
	}
	// A completed old turn and unrelated input cannot resolve this message.
	path := rec.TranscriptPath
	issue2836Append(t, path,
		`{"timestamp":"`+time.Now().UTC().Format(time.RFC3339Nano)+`","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"unrelated input"}]}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-existing","last_agent_message":"old completion"}}`,
	)
	if guard, err := acquireCodexAcceptanceGuard(inst, time.Second); err == nil {
		guard.Release()
		t.Fatal("unrelated input or old completion cleared queued message ownership")
	}
	// An unrelated new turn cannot be this queued message's receipt.
	issue2836Append(t, path, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"unrelated"}}`)
	if guard, err := acquireCodexAcceptanceGuard(inst, time.Second); err == nil {
		guard.Release()
		t.Fatal("unrelated turn cleared queued message ownership")
	}
	line, _ := json.Marshal(map[string]any{"timestamp": time.Now().UTC().Format(time.RFC3339Nano), "type": "response_item", "payload": map[string]any{"type": "message", "role": "user", "content": []map[string]string{{"type": "input_text", "text": rec.Message}}}})
	issue2836Append(t, path, string(line))
	guard, err := acquireCodexAcceptanceGuard(inst, time.Second)
	if err != nil {
		t.Fatalf("matching uptake did not resolve enqueue: %v", err)
	}
	guard.Release()
}

func TestDevbox17FailureInboxCommitErrorKeepsFallback(t *testing.T) {
	f := newRetryFixture(t, "_test_devbox17_failure_fallback")
	if err := os.MkdirAll(session.InboxPathFor(f.senderID), 0o700); err != nil {
		t.Fatal(err)
	}
	rec := f.queue(t, time.Second, f.senderID)
	rec.State, rec.Reason = sendqueue.StateFailed, "busy deadline"
	if notifySenderOfFailedSend(f.profile, rec) {
		t.Fatal("failed inbox commit suppressed ledger fallback")
	}
}
