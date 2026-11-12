package session

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"
)

// Inbox records are durable; wakes are best-effort attempts to start a turn.
// Claude drains through its hooks. Codex must run inbox drain explicitly.
// Busy targets keep their records for the daemon's bounded retry pass.

// Collapse a burst of child completions into one immediate wake. The turn
// drains all pending records; the daemon retries records that remain pending.
const defaultWakeNudgeDebounce = 500 * time.Millisecond

// Pending records may outlive a failed or busy wake attempt. Retry at most
// twice a minute per profile/parent in this daemon, until they are consumed.
const wakeNudgeRetryInterval = 30 * time.Second

// wakeNudgeMessage is the fallback instruction for an idle agent parent.
const wakeNudgeMessage = "[INBOX] A child just committed a completion to your inbox — drain it and act on each item now."

// wakeNudgeWiring carries the platform hooks the Tier-2 wake-nudge needs. It is
// kept out of the WakeNudger policy core so the idle-only/debounced policy stays
// testable without tmux: tests inject spy probes, production wires the live
// status probe + a best-effort no-wait pane send.
type wakeNudgeWiring struct {
	nudger *WakeNudger
	now    func() time.Time
	isIdle func(target *Instance, targetKind string) bool
	send   func(parent *Instance, profile, message string) error
}

// defaultWakeNudgeWiring is the production wiring: a debounced nudger, the wall
// clock, the agent-scoped idle probe, and a best-effort non-blocking pane
// send.
func defaultWakeNudgeWiring() *wakeNudgeWiring {
	return &wakeNudgeWiring{
		nudger: NewWakeNudger(defaultWakeNudgeDebounce),
		now:    time.Now,
		isIdle: parentIsNudgeableIdle,
		send:   sendWakeNudge,
	}
}

// nudge runs one debounced, idle-gated wake through the injected hooks.
func (w *wakeNudgeWiring) nudge(parent *Instance, targetKind, profile, message string) (bool, error) {
	now := time.Now()
	if w.now != nil {
		now = w.now()
	}
	isIdle := func() bool { return w.isIdle != nil && w.isIdle(parent, targetKind) }
	send := func() error {
		if w.send == nil {
			return nil
		}
		return w.send(parent, profile, message)
	}
	return w.nudger.Nudge(wakeNudgeKey(profile, parent.ID), now, isIdle, send)
}

// CommitInboxNotification writes a notice for a known agent and attempts an
// idle wake. A duplicate or consumed notice causes no extra wake. A busy or
// failed wake leaves the durable record for the daemon's retry pass.
func CommitInboxNotification(target *Instance, event TransitionNotificationEvent) error {
	if !isWakeTarget(target, event.TargetKind) {
		return fmt.Errorf("inbox notification target is not a supported agent")
	}
	profile, err := ResolveProfileForStorage(event.Profile)
	if err != nil {
		return err
	}
	event.Profile = profile
	event.TargetSessionID = target.ID
	if event.TurnFingerprint == "" {
		event.TurnFingerprint = TurnFingerprint(event)
	}
	if turnAlreadyConsumed(target.ID, event.TurnFingerprint) || pendingSameRecord(target.ID, event) {
		return nil
	}
	stored, outcome, err := commitToInbox(target.ID, event)
	if err != nil {
		return err
	}
	if outcome != inboxCommitUnchanged {
		n := &TransitionNotifier{wake: defaultWakeNudgeWiring()}
		n.wakeCommittedInbox(target, stored)
	}
	return nil
}

// fireWakeNudge invokes the Tier-2 wake-nudge for a parent that just had a
// completion durably committed. It is best-effort and MUST NOT affect the commit
// result: a nil wiring, an unsupported/busy target, or a send error are all
// swallowed (the durable record still drains on the next turn/heartbeat). A
// panic in the injected probe/send is recovered so a wake bug can never take
// down the producer.
func (n *TransitionNotifier) fireWakeNudge(parent *Instance, event TransitionNotificationEvent) {
	w := n.wake
	if w == nil || w.nudger == nil || parent == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			commsLog.Warn("wake_nudge_panic_recovered",
				slog.String("parent", parent.ID), slog.Any("panic", r))
		}
	}()

	// Issue #2469: the wake line names the record it is for; the record
	// itself (text included) is injected by the parent's prompt-time drain
	// into the turn this line starts.
	line := inboxWakeMessage(parent, NudgeHeadline(event))
	sent, err := w.nudge(parent, event.TargetKind, event.Profile, line)
	if err != nil {
		// Best-effort: a failed wake is harmless. Log once at debug-ish level so
		// the operator can see WHY a pane wasn't woken without it being an error.
		commsLog.Warn("wake_nudge_send_failed",
			slog.String("parent", parent.ID), slog.String("error", err.Error()))
		return
	}
	if sent {
		// Comms Ledger measurement: one wake record per machine wake, so
		// `msg stats` compares this path with the ledger's own.
		SpoolCommsWake(parent.ID, "inbox", "tmux", line, "")
	}
}

// parentIsNudgeableIdle admits Claude/Codex agents at an idle or waiting
// prompt, regardless of title. Re-probe status within the daemon's budget;
// never type a wake into a shell or a pane known to be busy.
func parentIsNudgeableIdle(parent *Instance, targetKind string) bool {
	if parent == nil || !isWakeTarget(parent, targetKind) {
		return false
	}
	if refreshStatusBounded(parent, statusProbeBudget) {
		return false
	}
	switch parent.Status {
	case StatusIdle, StatusWaiting:
		return true
	default:
		return false
	}
}

// isWakeTarget admits agent parents independent of title. Sibling reply
// routing keeps its existing Claude-only capability gate.
func isWakeTarget(target *Instance, targetKind string) bool {
	if target == nil {
		return false
	}
	if targetKind == InboxTargetKindReply {
		return IsClaudeCompatible(target.Tool)
	}
	return IsClaudeCompatible(target.Tool) || IsCodexCompatible(target.Tool)
}

func wakeNudgeKey(profile, targetID string) string {
	return profile + "\x00" + targetID
}

func (w *wakeNudgeWiring) retryDue(profile, targetID string) bool {
	if w == nil || w.nudger == nil {
		return false
	}
	now := time.Now()
	if w.now != nil {
		now = w.now()
	}
	return !w.nudger.recent(wakeNudgeKey(profile, targetID), now, wakeNudgeRetryInterval)
}

// Codex has no prompt-time injection hook. Give it the command that reads the
// actual records instead of claiming they are already in the turn's context.
func inboxWakeMessage(target *Instance, message string) string {
	if target != nil && IsCodexCompatible(target.Tool) {
		message = strings.TrimSuffix(message, " · details are in this turn's context")
		return CapTurnText(message, 160) + " · run agent-deck inbox drain self --json and act on each item now"
	}
	return message
}

// sendWakeNudge fires one best-effort wake into the agent parent's pane and
// returns immediately. The actual `session send --no-wait` runs detached so a
// slow/stuck send never blocks the producer commit path; the gate that this is
// only reached for an IDLE pane means the send won't sit in tmux's busy-queue.
// A failed send is harmless (the record still drains on the next turn).
func sendWakeNudge(parent *Instance, profile, message string) error {
	if parent == nil {
		return nil
	}
	if strings.TrimSpace(message) == "" {
		message = wakeNudgeMessage
	}
	go func(profile, ref, message string) {
		if err := sendWakeNudgeNoWait(profile, ref, message); err != nil {
			commsLog.Warn("wake_nudge_dispatch_failed",
				slog.String("parent", ref), slog.String("error", err.Error()))
		}
	}(profile, parent.ID, message)
	return nil
}

// fireDigestNudge wakes an idle parent once for info that waited past the
// digest window (issue #2469). Same gate and debounce as the urgent wake;
// the records themselves arrive through the prompt-time drain.
func (n *TransitionNotifier) fireDigestNudge(parent *Instance, profile, message string) bool {
	w := n.wake
	if w == nil || w.nudger == nil || parent == nil {
		return false
	}
	message = inboxWakeMessage(parent, message)
	sent, err := w.nudge(parent, "parent", profile, message)
	if err != nil {
		commsLog.Warn("digest_nudge_send_failed",
			slog.String("parent", parent.ID), slog.String("error", err.Error()))
		return false
	}
	if sent {
		SpoolCommsWake(parent.ID, "inbox", "tmux", message, "")
	}
	return sent
}

// wakeNudgeDeliveryBudget is the time the nudge subprocess gets for the send
// itself once it holds the target's send lock: the --no-wait pipeline's
// composer preflight, settle, guard hold, paste and verification add up to
// well under this.
const wakeNudgeDeliveryBudget = 15 * time.Second

// wakeNudgeSendTimeout bounds the detached wake-nudge subprocess. The bound
// exists so a wedged agent-deck binary (e.g. stuck on SQLite/tmux) is reaped
// instead of leaking the dispatch goroutine indefinitely (PR #1230 audit). It
// must cover the per-target send lock wait (review round 2, P3): the nudge
// queues behind a heartbeat or sibling send holding the target, and a shorter
// timeout killed it while it was still waiting on the flock, so the nudge was
// silently dropped. A timed-out send is still harmless like any dropped nudge:
// the record drains on the next turn/heartbeat.
const wakeNudgeSendTimeout = SendTargetLockWait + wakeNudgeDeliveryBudget

// wakeNudgeExec runs the resolved command under ctx. It is a package var so a
// test can substitute a spy and assert the deadline/args without spawning a real
// process; production runs the real bounded subprocess.
var wakeNudgeExec = func(ctx context.Context, bin string, args ...string) error {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(cmd.Environ(), MachineSendEnv+"=1")
	return cmd.Run()
}

// MachineSendEnv marks a `session send` agent-deck itself runs to type a
// wake line: the Comms Ledger records it as a wake, never as a send. An
// environment variable, not a flag, so an older binary simply ignores it.
const MachineSendEnv = "AGENTDECK_SEND_MACHINE"

// sendWakeNudgeNoWait shells out to `agent-deck [-p profile] session send <ref>
// <msg> --no-wait -q`. --no-wait keeps it fire-and-forget: it neither blocks for
// the agent's ready state nor waits for a reply, so it returns fast even if the
// pane is wedged. The context deadline is a belt-and-suspenders backstop for the
// case where even the subprocess itself hangs.
func sendWakeNudgeNoWait(profile, ref, message string) error {
	ctx, cancel := context.WithTimeout(context.Background(), wakeNudgeSendTimeout)
	defer cancel()
	bin := agentDeckBinaryPath()
	args := []string{}
	if profile != "" {
		args = append(args, "-p", profile)
	}
	// --no-tag: the daemon may have inherited a session's AGENTDECK_INSTANCE_ID;
	// a wake line is never a send from that session.
	args = append(args, "session", "send", ref, message, "--no-wait", "--no-tag", "-q")
	return wakeNudgeExec(ctx, bin, args...)
}
