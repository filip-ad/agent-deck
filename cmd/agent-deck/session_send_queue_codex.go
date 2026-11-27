package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/recall/query"
	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
	"github.com/asheshgoplani/agent-deck/internal/session"
)

const (
	codexQueueTransport = "codex_queue"
	codexQueueDelivery  = "queued_native"
)

// queuedCodexNativeTarget is deliberately local to queued busy sends. Global
// busy capability, synchronous sends, drafts, slash commands and guarded
// releases retain their existing transport and readiness rules.
func queuedCodexNativeTarget(inst *session.Instance, rec *sendqueue.Record, status string, peers []*session.Instance) *session.CodexQueueTarget {
	if status != "running" || rec.RequireInputPrompt || isBareSlashCommand(rec.Message) ||
		!session.IsCodexCompatible(rec.Tool) || !session.IsCodexCompatible(inst.Tool) || !inst.CodexRolloutIsResolvableLocally() {
		return nil
	}
	value, _ := sendTransportFromConfig()
	if value != "auto" {
		return nil
	}
	target, err := session.ResolveCodexQueueTarget(inst)
	if err != nil {
		return nil
	}
	for _, peer := range peers {
		if peer != nil && peer.ID != inst.ID && session.IsCodexCompatible(peer.Tool) && peer.CodexRolloutIsResolvableLocally() &&
			peer.Exists() && peer.LiveCodexUserThreadID() == target.ThreadID {
			return nil
		}
	}
	return &target
}

func isNativeCodexQueuedSend(rec *sendqueue.Record) bool {
	return rowsHarness(rec.Tool) == "codex" && rec.DeliveryEvidence["transport"] == codexQueueTransport
}

func canReconcileNativeLanding(rec *sendqueue.Record) bool {
	return rec.State == sendqueue.StateTyped && rec.Settled && rec.TranscriptPath != "" && isNativeCodexQueuedSend(rec)
}

// reconcileSettledNativeLanding gives send-status one exact read after the
// watcher has settled. It shares the watcher lock and never starts delivery.
func reconcileSettledNativeLanding(profile, dir string, rec *sendqueue.Record) *sendqueue.Record {
	lock, ok, err := sendqueue.TryLock(dir, "watch-"+rec.SendID)
	if err != nil || !ok {
		return rec
	}
	defer releaseQueueLock(lock)
	current, err := sendqueue.Load(dir, rec.SendID)
	if err != nil {
		return rec
	}
	if !canReconcileNativeLanding(current) {
		return current
	}
	sentAt, err := time.Parse(time.RFC3339Nano, current.SentAt)
	if err != nil {
		return current
	}
	digest := sha256.Sum256([]byte(current.Message))
	id, ts, err := query.FindCodexLandedExact(context.Background(), current.TranscriptPath, current.TranscriptFrom, digest, sentAt)
	if err != nil || id == "" {
		return current
	}
	landed, err := sendqueue.Update(dir, current.SendID, time.Now(), func(r *sendqueue.Record) {
		markQueuedLanded(r, id, ts)
	})
	if err != nil {
		return current
	}
	queuedSendChanged(profile, current, landed)
	return landed
}

// sendQueuedCodexNative runs only after the durable queue record says typing.
// Both ownership locks span the reservation and command. The marker survives
// success (enqueue is pending) and every ambiguous command outcome.
func sendQueuedCodexNative(inst *session.Instance, rec *sendqueue.Record, target session.CodexQueueTarget, set func(func(*sendqueue.Record)) error) (map[string]interface{}, int) {
	result := map[string]interface{}{
		"success": false, "delivery": deliveryAcceptanceRefused,
		"transport": codexQueueTransport, "send_id": rec.SendID, "codex_session_id": target.ThreadID,
		"submitted": false, "confirmation": "unknown", "completion": "pending",
	}
	refuse := func(err error) (map[string]interface{}, int) {
		result["error"] = err.Error()
		return result, 1
	}
	guard, err := acquireCodexAcceptanceGuard(inst, codexAcceptanceLockWait(time.Second))
	if err != nil {
		result["acceptance_unavailable"] = codexIdentityProvablyUnavailable(err)
		return refuse(err)
	}
	defer guard.Release()
	lock, err := session.AcquireSendLock(inst.ID, sendTargetLockWait)
	if err != nil {
		result["delivery"] = deliveryTargetBusy
		return refuse(err)
	}
	defer lock.Release()
	if err := validateCodexAcceptanceFence(inst, guard.fence); err != nil {
		return refuse(err)
	}
	// Target resolution predates both locks. Exclude input from earlier peer
	// sends before reserving this attempt, and give the landing scan the same
	// fresh boundary. Only the cheap stat runs here, not queue --help again.
	info, err := os.Stat(target.Rollout)
	if err != nil {
		return refuse(fmt.Errorf("refresh native queue rollout offset: %w", err))
	}
	target.From = info.Size()
	sentAt := time.Now().UTC()
	if err := set(func(r *sendqueue.Record) {
		r.TranscriptPath, r.TranscriptFrom = target.Rollout, target.From
		r.SentAt = sentAt.Format(time.RFC3339Nano)
	}); err != nil {
		return refuse(fmt.Errorf("record native queue rollout offset: %w", err))
	}
	if err := guard.Prepare(inst.ID, sentAt); err != nil {
		return refuse(err)
	}
	if err := guard.marker.PrepareQueue(target, rec.Message); err != nil {
		// No enqueue started. Clear only this reservation.
		if clearErr := session.ClearCodexSubmissionMarker(guard.marker); clearErr != nil {
			result["delivery"] = deliveryNoEvidence
			return refuse(fmt.Errorf("prepare native queue marker: %v; clear reservation: %w", err, clearErr))
		}
		return refuse(err)
	}
	attempted, enqueueErr := target.Enqueue(rec.Message)
	if !attempted {
		if err := session.ClearCodexSubmissionMarker(guard.marker); err != nil {
			result["delivery"] = deliveryNoEvidence
			return refuse(err)
		}
		result["delivery"] = deliveryTargetBusy
		return refuse(enqueueErr)
	}
	result["delivery"] = codexQueueDelivery
	if err := guard.RecordTransportOutcome(codexQueueDelivery, false, time.Now()); err != nil {
		result["delivery"] = deliveryNoEvidence
		return refuse(err)
	}
	if enqueueErr != nil {
		result["delivery"] = deliveryNoEvidence
		return refuse(enqueueErr)
	}
	result["success"] = true
	return result, 0
}
