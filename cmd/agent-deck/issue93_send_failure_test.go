package main

import (
	"strings"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
	"github.com/asheshgoplani/agent-deck/internal/session"
)

func TestIssue93TypedNotSubmittedIsTerminal(t *testing.T) {
	r := &sendqueue.Record{State: sendqueue.StateTyping}
	result := map[string]interface{}{"success": false, "delivery": deliveryTypedNotSubmitted, "error": "typed but not submitted"}
	applyChildResult(r, result, 1, true, func(f func(*sendqueue.Record)) error { f(r); return nil })
	if r.State != sendqueue.StateTyped || !r.Final() {
		t.Fatalf("typed-not-submitted must stop without retry: %+v", r)
	}
	if r.State == sendqueue.StateFailed {
		t.Fatal("typed text must never become safe-to-resend failed")
	}
}

func TestIssue93TypedNoticeDoesNotPermitResend(t *testing.T) {
	profile := "_test_issue93_typed"
	storage, err := session.NewStorageWithProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	sender := session.NewInstanceWithTool("ordinary-sender", t.TempDir(), "codex")
	sender.GroupPath = session.DefaultGroupPath
	if err := saveSessionData(storage, []*session.Instance{sender}, nil); err != nil {
		t.Fatal(err)
	}
	r := &sendqueue.Record{SendID: sendqueue.NewID(time.Now()), State: sendqueue.StateTyped, Settled: true, Sender: sender.ID, SessionID: "target", Reason: "typed but not submitted", DeliveryEvidence: map[string]interface{}{"delivery": deliveryTypedNotSubmitted}}
	finishQueuedSend(profile, r, true)
	events, err := session.ReadInboxEvents(sender.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("typed sender notices=%d want=1", len(events))
	}
	text := events[0].Text
	if strings.Contains(text, "resending is safe") || strings.Contains(text, "Nothing was typed") || !strings.Contains(text, "Do not resend") {
		t.Fatalf("unsafe typed notice: %q", text)
	}
}

func TestIssue93FailureNoticeExcludesShellSender(t *testing.T) {
	profile := "_test_issue93_shell"
	storage, err := session.NewStorageWithProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	sender := session.NewInstanceWithTool("conductor-shell", t.TempDir(), "shell")
	sender.GroupPath = session.DefaultGroupPath
	if err := saveSessionData(storage, []*session.Instance{sender}, nil); err != nil {
		t.Fatal(err)
	}
	r := &sendqueue.Record{SendID: sendqueue.NewID(time.Now()), State: sendqueue.StateFailed, Sender: sender.ID, SessionID: "target", Reason: "not running"}
	if notifySenderOfFailedSend(profile, r) {
		t.Fatal("shell sender accepted an agent failure notice")
	}
	if events, _ := session.ReadInboxEvents(sender.ID); len(events) != 0 {
		t.Fatalf("shell notices=%d", len(events))
	}
}

func TestIssue93TypedInboxNoticeKeepsTypedLedgerState(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Cleanup(session.SetCommsLedgerForTest(true))
	r := &sendqueue.Record{SendID: "typed-request", Sender: "sender", SessionID: "target", State: sendqueue.StateTyped, Settled: true, Reason: "typed but not submitted"}
	ledgerQueuedSend(r, true)
	entries, err := session.ReadCommsSpool("target")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].State != "typed" {
		t.Fatalf("typed outcome mislabeled: %+v", entries)
	}
}
