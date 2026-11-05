package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
	"github.com/asheshgoplani/agent-deck/internal/session"
	"github.com/asheshgoplani/agent-deck/internal/testutil"
)

func TestDevbox17NativeQueueLandingPreservesWhitespace(t *testing.T) {
	for _, tc := range []struct {
		name, queued, other string
	}{
		{"spaces", "Print exactly \"a  b\"", "Print exactly \"a b\""},
		{"tab", "Print exactly \"a\tb\"", "Print exactly \"a b\""},
		{"newline", "Print exactly \"a\nb\"", "Print exactly \"a b\""},
		{"blank-line", "Print exactly \"a\n\nb\"", "Print exactly \"a\nb\""},
		{"leading", " prompt", "prompt"},
		{"trailing", "prompt\n", "prompt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profile := "_test_devbox17_landing_whitespace"
			inst := setupDeferTest(t, profile)
			seedDeferHookFile(t, inst.ID, "UserPromptSubmit", inst.ClaudeSessionID, "running")
			t.Setenv("AGENTDECK_SEND_LAND_WINDOW", "100ms")
			now := time.Now().UTC()
			path := filepath.Join(t.TempDir(), "rollout.jsonl")
			input := func(id, text string) string {
				t.Helper()
				line, err := json.Marshal(map[string]any{"timestamp": now.Format(time.RFC3339Nano), "type": "response_item", "payload": map[string]any{"id": id, "type": "message", "role": "user", "content": []map[string]string{{"type": "input_text", "text": text}}}})
				if err != nil {
					t.Fatal(err)
				}
				return string(line)
			}
			wrong := input("different-prompt", tc.other)
			for _, mode := range []string{"native-different", "native-ambiguous", "native-exact-later", "legacy-codex", "legacy-claude"} {
				t.Run(mode, func(t *testing.T) {
					data := wrong + "\n"
					wantID := "different-prompt"
					if mode == "native-exact-later" {
						data += input("exact-prompt", tc.queued) + "\n"
						wantID = "exact-prompt"
					}
					tool := "codex"
					if mode == "legacy-claude" {
						tool = "claude"
						data = claudeUserLine(wantID, tc.other, now)
					}
					if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
						t.Fatal(err)
					}
					dir := t.TempDir()
					rec := seedTyping(t, dir, tool, path, 0, now)
					rec.SessionID, rec.Message = inst.ID, tc.queued
					rec.State, rec.Verdict = sendqueue.StateTyped, "unknown"
					if mode == "native-different" || mode == "native-ambiguous" || mode == "native-exact-later" {
						// Use the persisted transport tag, including ambiguous enqueue.
						delivery := "queued_native"
						if mode == "native-ambiguous" {
							delivery = deliveryNoEvidence
						}
						rec.DeliveryEvidence = map[string]interface{}{"transport": "codex_queue", "delivery": delivery}
					}
					if err := sendqueue.Save(dir, rec); err != nil {
						t.Fatal(err)
					}
					rec, err := sendqueue.Load(dir, rec.SendID)
					if err != nil {
						t.Fatal(err)
					}
					set := func(update func(*sendqueue.Record)) error {
						update(rec)
						return sendqueue.Save(dir, rec)
					}
					watchLanded(profile, rec, set)
					saved, err := sendqueue.Load(dir, rec.SendID)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "native-different" || mode == "native-ambiguous" {
						if saved.State != sendqueue.StateTyped || saved.Verdict != "unknown" || saved.LandedRowID != "" || !saved.Settled {
							t.Fatalf("different whitespace confirmed the durable native send: %+v", saved)
						}
					} else if saved.State != sendqueue.StateLanded || saved.Verdict != "delivered" || saved.LandedRowID != wantID {
						t.Fatalf("landing lost exact native or legacy terminal matching: %+v; want %q", saved, wantID)
					}
				})
			}
		})
	}
}

// Exercise the CLI producer and its default wake wiring. The fake send child
// cannot finish until this test releases it. No provider process is started.
func TestDevbox17FailureProducerWaitsForWakeDispatch(t *testing.T) {
	f := newRetryFixture(t, "_test_devbox17_wake_lifetime")
	storage, instances, _, err := loadSessionData(f.profile)
	if err != nil {
		t.Fatal(err)
	}
	_ = storage.Close()
	sender := instanceByID(instances, f.senderID)
	pane := sender.GetTmuxSession()
	if err := pane.Start("bash"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pane.Kill() })
	seedDeferHookFile(t, sender.ID, "Stop", "", "waiting")

	root := t.TempDir()
	pipes := make([]*os.File, 2)
	for i, name := range []string{"started", "release"} {
		path := filepath.Join(root, name)
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		pipes[i], err = os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pipes[i].Close() })
	}
	t.Setenv("DEVBOX17_WAKE_STARTED", filepath.Join(root, "started"))
	t.Setenv("DEVBOX17_WAKE_RELEASE", filepath.Join(root, "release"))
	t.Setenv("DEVBOX17_WAKE_DONE", filepath.Join(root, "done"))
	script := "#!/bin/sh\nprintf 'started\\n' > \"$DEVBOX17_WAKE_STARTED\"\nread -r token < \"$DEVBOX17_WAKE_RELEASE\"\nprintf 'done\\n' > \"$DEVBOX17_WAKE_DONE\"\n"
	if err := os.WriteFile(filepath.Join(root, "agent-deck"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	failedSendNotifierOnce, failedSendNotifier = sync.Once{}, nil
	t.Cleanup(func() { failedSendNotifierOnce, failedSendNotifier = sync.Once{}, nil })
	rec := f.queue(t, time.Second, sender.ID)
	rec.State, rec.Reason = sendqueue.StateFailed, "busy deadline"
	returned := make(chan struct{})
	var owned bool
	var dispatchErr error
	go func() {
		owned = notifySenderOfFailedSend(f.profile, rec)
		_, dispatchErr = os.Stat(filepath.Join(root, "done"))
		close(returned)
	}()
	started := make(chan struct{})
	var startErr error
	go func() {
		_, startErr = bufio.NewReader(pipes[0]).ReadString('\n')
		close(started)
	}()
	// Always release and join both test goroutines, including on a failure.
	t.Cleanup(func() {
		_, _ = pipes[1].WriteString("go\n")
		_ = pipes[0].Close()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Error("wake-start reader did not stop")
		}
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			t.Error("failure producer did not stop")
		}
	})
	select {
	case <-started:
		if startErr != nil {
			t.Fatal(startErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failure notice did not reach the wake child")
	}
	events, err := session.ReadInboxEvents(sender.ID)
	if err != nil || len(events) != 1 || events[0].TargetKind != session.InboxTargetKindReply {
		t.Fatalf("notice must be durable before dispatch finishes: %+v %v", events, err)
	}
	select {
	case <-returned:
		t.Fatal("short-lived producer returned while its wake child was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := pipes[1].WriteString("go\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-returned:
		if !owned {
			t.Fatal("completed dispatch lost durable inbox ownership")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("producer did not return after dispatch finished")
	}
	if dispatchErr != nil {
		t.Fatalf("producer returned before child completion: %v", dispatchErr)
	}
}

// Resolve happens before acceptance/send locks in the worker. An earlier
// matching input may land before this send reserves them, in the same turn.
func TestDevbox17NativeQueueExcludesPreReservationInput(t *testing.T) {
	testutil.RequireTmux(t)
	root := t.TempDir()
	home := filepath.Join(root, "codex")
	t.Setenv("CODEX_HOME", home)
	inst, _ := newIssue2549Instance(t, root, "_test_devbox17_fresh_fence", "fresh-fence")
	inst.CodexSessionID = issue2549NewThread
	startRotatedCodexPane(t, root, home, inst, inst.CodexSessionID, inst.CodexSessionID)
	writeLegacyCodexRollout(t, home, inst.CodexSessionID, "24")
	bin := filepath.Join(root, "bin", "codex")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nif [ \"$1 $2\" = 'queue --help' ]; then echo 'queue --thread --message'; fi\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", filepath.Dir(bin)+string(os.PathListSeparator)+os.Getenv("PATH"))
	target, err := session.ResolveCodexQueueTarget(inst)
	if err != nil {
		t.Fatal(err)
	}
	rec := &sendqueue.Record{SendID: sendqueue.NewID(time.Now()), SessionID: inst.ID, Tool: "codex", Message: "same burst message", IgnoreBudget: true}
	appendInput := func() {
		line, err := json.Marshal(map[string]any{"timestamp": time.Now().UTC().Format(time.RFC3339Nano), "type": "response_item", "payload": map[string]any{"type": "message", "role": "user", "content": []map[string]string{{"type": "input_text", "text": rec.Message}}}})
		if err != nil {
			t.Fatal(err)
		}
		issue2836Append(t, target.Rollout, string(line))
	}
	// This timestamp also passes the old two-second tolerance. The byte
	// boundary must exclude it, independently of timestamp precision.
	appendInput()
	prior, err := os.Stat(target.Rollout)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	set := func(update func(*sendqueue.Record)) error {
		update(rec)
		return sendqueue.Save(dir, rec)
	}
	if !typeQueued("_test_devbox17_fresh_fence", dir, rec, inst, []*session.Instance{inst}, "running", target.Rollout, target.From, set, &target) {
		t.Fatal("cannot record native send")
	}
	if rec.State != sendqueue.StateTyped || rec.Verdict != "unknown" || rec.DeliveryEvidence["delivery"] != "queued_native" {
		t.Fatalf("enqueue must remain pending: %+v", rec)
	}
	if guard, err := acquireCodexAcceptanceGuard(inst, time.Second); err == nil {
		guard.Release()
		t.Fatal("matching input from before reservation resolved the new marker")
	}
	saved, err := sendqueue.Load(dir, rec.SendID)
	if err != nil || saved.TranscriptFrom != prior.Size() {
		t.Fatalf("queue landing scan must use the same fresh boundary: %+v %v", saved, err)
	}
	// The same text after reservation is actual uptake, even in the same turn.
	appendInput()
	guard, err := acquireCodexAcceptanceGuard(inst, time.Second)
	if err != nil {
		t.Fatalf("own later input did not resolve native marker: %v", err)
	}
	guard.Release()
}
