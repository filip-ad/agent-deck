package session

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestIssue93AgentWakeTargets(t *testing.T) {
	withNoopStatusProbe(t)
	for _, tool := range []string{"claude", "codex", "shell"} {
		for _, title := range []string{"ordinary-parent", "conductor-parent"} {
			p := &Instance{ID: "p", Tool: tool, Title: title, Status: StatusIdle}
			if got, want := parentIsNudgeableIdle(p, "parent"), tool != "shell"; got != want {
				t.Errorf("tool=%s title=%s wake=%v want=%v", tool, title, got, want)
			}
		}
	}
}

func TestIssue93BusyParentEventuallyWakesOnce(t *testing.T) {
	n, parentID, event := newWakeNudgeFixture(t)
	event.Timestamp = time.Now()
	busy, sent := true, 0
	n.wake = &wakeNudgeWiring{
		nudger: NewWakeNudger(0),
		isIdle: func(*Instance, string) bool { return !busy },
		send: func(_ *Instance, profile, line string) error {
			if profile != event.Profile {
				t.Errorf("profile=%q", profile)
			}
			sent++
			return nil
		},
	}
	n.NotifyFinished(event)
	if sent != 0 {
		t.Fatal("busy parent was woken")
	}
	parent := &Instance{ID: parentID, Tool: "codex", Title: "ordinary", Status: StatusIdle}
	d := &TransitionDaemon{notifier: n}
	busy = false
	for range 3 {
		d.wakePendingInboxes(event.Profile, map[string]*Instance{
			parentID:             parent,
			event.ChildSessionID: {ID: event.ChildSessionID, ParentSessionID: parentID},
		}, map[string]string{parentID: string(StatusIdle)})
	}
	if sent != 1 {
		t.Fatalf("busy-to-idle wakes=%d want=1", sent)
	}
}

func TestIssue93CodexWakeTellsParentToDrain(t *testing.T) {
	n, parentID, event := newWakeNudgeFixture(t)
	var line string
	n.wake = &wakeNudgeWiring{nudger: NewWakeNudger(0), isIdle: func(*Instance, string) bool { return true }, send: func(_ *Instance, _, message string) error { line = message; return nil }}
	n.fireWakeNudge(&Instance{ID: parentID, Tool: "codex"}, event)
	if !strings.Contains(line, "agent-deck inbox drain") || strings.Contains(line, "details are in this turn's context") {
		t.Fatalf("Codex wake lacks manual drain instruction: %q", line)
	}
}

func TestIssue93UrgentParentPaths(t *testing.T) {
	for _, tool := range []string{"claude", "codex"} {
		for _, kind := range []string{"finish", "wait"} {
			t.Run(tool+"/"+kind, func(t *testing.T) {
				n, parentID, event := newWakeNudgeFixture(t)
				withNoopStatusProbe(t)
				storage, err := NewStorageWithProfile(event.Profile)
				if err != nil {
					t.Fatal(err)
				}
				defer storage.Close()
				instances, _, err := storage.LoadWithGroups()
				if err != nil {
					t.Fatal(err)
				}
				for _, inst := range instances {
					if inst.ID == parentID {
						inst.Tool = tool
						inst.Title = "ordinary-parent"
					}
				}
				if err := storage.SaveWithGroups(instances, nil); err != nil {
					t.Fatal(err)
				}
				sent := 0
				n.wake = &wakeNudgeWiring{nudger: NewWakeNudger(0), isIdle: parentIsNudgeableIdle, send: func(_ *Instance, _, _ string) error { sent++; return nil }}
				if kind == "finish" {
					n.NotifyFinished(event)
				} else {
					event.FromStatus = "running"
					event.ToStatus = "waiting"
					event.DoneStatus = ""
					event.DoneSummary = ""
					n.NotifyTransition(event)
				}
				if sent != 1 {
					t.Fatalf("ordinary %s parent %s wakes=%d want=1", tool, kind, sent)
				}
			})
		}
	}
}

func TestIssue93FailedWakeRemainsEligible(t *testing.T) {
	w := NewWakeNudger(defaultWakeNudgeDebounce)
	now := time.Now()
	calls := 0
	send := func() error { calls++; return errors.New("dispatch failed") }
	w.Nudge("parent", now, func() bool { return true }, send)
	w.Nudge("parent", now.Add(time.Second), func() bool { return true }, send)
	if calls != 2 {
		t.Fatalf("failed wake calls=%d want=2; failure must remain eligible after debounce", calls)
	}
}

func TestIssue93SenderWakeHasNoParentLink(t *testing.T) {
	for _, tool := range []string{"claude", "codex"} {
		for _, status := range []string{"send_failed", "send_typed_not_submitted"} {
			t.Run(tool+"/"+status, func(t *testing.T) {
				n, parentID, event := newWakeNudgeFixture(t)
				event.ToStatus = status
				event.FromStatus = "queued"
				event.Tier = TurnTierUrgent
				event.Timestamp = time.Now()
				if err := CommitToInbox(parentID, event); err != nil {
					t.Fatal(err)
				}
				sent := 0
				n.wake = &wakeNudgeWiring{nudger: NewWakeNudger(0), isIdle: func(*Instance, string) bool { return true }, send: func(_ *Instance, profile, _ string) error {
					if profile != event.Profile {
						t.Errorf("profile=%s", profile)
					}
					sent++
					return nil
				}}
				d := &TransitionDaemon{notifier: n}
				sender := &Instance{ID: parentID, Title: "ordinary-sender", Tool: tool, Status: StatusIdle}
				for range 3 {
					d.wakePendingInboxes(event.Profile, map[string]*Instance{parentID: sender}, map[string]string{parentID: string(StatusIdle)})
				}
				if sent != 1 {
					t.Fatalf("sender wakes=%d want=1", sent)
				}
			})
		}
	}
}

func TestIssue93RetryIsBoundedAndStopsAfterDrain(t *testing.T) {
	n, parentID, event := newWakeNudgeFixture(t)
	clock := time.Now()
	calls := 0
	n.wake = &wakeNudgeWiring{nudger: NewWakeNudger(0), now: func() time.Time { return clock }, isIdle: func(*Instance, string) bool { return true }, send: func(*Instance, string, string) error { calls++; return errors.New("dispatch failed") }}
	n.NotifyFinished(event)
	d := &TransitionDaemon{notifier: n}
	parent := &Instance{ID: parentID, Tool: "codex", Status: StatusIdle}
	scan := func() {
		d.wakePendingInboxes(event.Profile, map[string]*Instance{parentID: parent}, map[string]string{parentID: string(StatusIdle)})
	}
	for range 4 {
		clock = clock.Add(time.Second)
		scan()
	}
	if calls != 1 {
		t.Fatalf("retry storm: %d attempts before retry window", calls)
	}
	clock = clock.Add(wakeNudgeRetryInterval)
	scan()
	if calls != 2 {
		t.Fatalf("failed dispatch was not retried: attempts=%d", calls)
	}
	if _, err := DrainInboxForParent(parentID); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(wakeNudgeRetryInterval)
	scan()
	if calls != 2 {
		t.Fatalf("consumed record still wakes: attempts=%d", calls)
	}
}

func TestIssue93WakeDebounceSeparatesProfiles(t *testing.T) {
	calls := 0
	w := &wakeNudgeWiring{nudger: NewWakeNudger(time.Minute), now: func() time.Time { return time.Unix(1000, 0) }, isIdle: func(*Instance, string) bool { return true }, send: func(*Instance, string, string) error { calls++; return nil }}
	target := &Instance{ID: "same-id", Tool: "codex"}
	for _, profile := range []string{"profile-a", "profile-b", "profile-a"} {
		w.nudge(target, "parent", profile, "wake")
	}
	if calls != 2 {
		t.Fatalf("profile-scoped debounce attempts=%d want=2", calls)
	}
}

func TestIssue93PendingWakeChecksProfileAndShell(t *testing.T) {
	n, parentID, event := newWakeNudgeFixture(t)
	event.Profile = "other-profile"
	if err := CommitToInbox(parentID, event); err != nil {
		t.Fatal(err)
	}
	calls := 0
	n.wake = &wakeNudgeWiring{nudger: NewWakeNudger(0), isIdle: func(*Instance, string) bool { return true }, send: func(*Instance, string, string) error { calls++; return nil }}
	d := &TransitionDaemon{notifier: n}
	owner := &Instance{ID: parentID, Tool: "codex", Title: "ordinary", Status: StatusIdle}
	scan := func(profile string) {
		d.wakePendingInboxes(profile, map[string]*Instance{parentID: owner}, map[string]string{parentID: string(StatusIdle)})
	}
	scan("wrong-profile")
	if calls != 0 {
		t.Fatal("wrong profile woke owner")
	}
	owner.Tool = "shell"
	owner.Title = "conductor-shell"
	scan("other-profile")
	if calls != 0 {
		t.Fatal("pending record woke shell")
	}
	owner.Tool = "codex"
	scan("other-profile")
	if calls != 1 {
		t.Fatalf("matching profile agent wakes=%d want=1", calls)
	}
}

func TestIssue93NotificationWakeUsesDirectSendAndDedups(t *testing.T) {
	for _, tool := range []string{"claude", "codex"} {
		t.Run(tool, func(t *testing.T) {
			_, ownerID, event := newWakeNudgeFixture(t)
			withNoopStatusProbe(t)
			event.Tier = TurnTierUrgent
			event.ToStatus = "send_failed"
			event.LastOutputHash = "send:failure-id"
			original := wakeNudgeExec
			t.Cleanup(func() { wakeNudgeExec = original })
			dispatched := make(chan []string, 2)
			wakeNudgeExec = func(_ context.Context, _ string, args ...string) error {
				dispatched <- args
				return errors.New("wake failed")
			}
			owner := &Instance{ID: ownerID, Tool: tool, Title: "ordinary", Status: StatusIdle}
			if err := CommitInboxNotification(owner, event); err != nil {
				t.Fatal(err)
			}
			var args []string
			select {
			case args = <-dispatched:
			case <-time.After(2 * time.Second):
				t.Fatal("notice did not attempt a wake")
			}
			if len(args) < 6 || args[0] != "-p" || args[1] != event.Profile || args[2] != "session" || args[3] != "send" || args[4] != ownerID {
				t.Fatalf("wake route: %v", args)
			}
			for _, arg := range args {
				if arg == "--queue" || arg == "--json" {
					t.Fatalf("wake can create a recursive queued failure: %v", args)
				}
			}
			if tool == "codex" && !strings.Contains(args[5], "agent-deck inbox drain") {
				t.Fatalf("Codex instruction: %s", args[5])
			}
			if err := CommitInboxNotification(owner, event); err != nil {
				t.Fatal(err)
			}
			select {
			case extra := <-dispatched:
				t.Fatalf("duplicate notice woke again: %v", extra)
			case <-time.After(50 * time.Millisecond):
			}
			records, err := ReadInboxEvents(ownerID)
			if err != nil || len(records) != 1 {
				t.Fatalf("failed wake lost or duplicated its record: %d %v", len(records), err)
			}
		})
	}
}

func TestIssue93LedgerEnrollmentDoesNotSuppressInboxRetry(t *testing.T) {
	n, parentID, event := newWakeNudgeFixture(t)
	t.Cleanup(SetCommsLedgerForTest(true))
	if err := writeCommsEnrollment(parentID, CommsEnrollment{Profile: event.Profile, Dir: t.TempDir(), Tool: "claude", At: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	if !commsDeliversTo(parentID) {
		t.Fatal("fixture is not enrolled")
	}
	event.ToStatus = "send_failed"
	event.Tier = TurnTierUrgent
	event.Timestamp = time.Now()
	if err := CommitToInbox(parentID, event); err != nil {
		t.Fatal(err)
	}
	calls := 0
	n.wake = &wakeNudgeWiring{nudger: NewWakeNudger(0), isIdle: func(*Instance, string) bool { return true }, send: func(*Instance, string, string) error { calls++; return nil }}
	d := &TransitionDaemon{notifier: n}
	owner := &Instance{ID: parentID, Tool: "claude", Status: StatusIdle}
	d.wakePendingInboxes(event.Profile, map[string]*Instance{parentID: owner}, map[string]string{parentID: string(StatusIdle)})
	if calls != 1 {
		t.Fatalf("inbox-owned notice was suppressed by enrollment: wakes=%d", calls)
	}
}
