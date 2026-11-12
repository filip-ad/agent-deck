package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/health"
	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
	"github.com/asheshgoplani/agent-deck/internal/session"
)

// Issue #2481: a send record says who sent what and how it ended, so resend
// loops can be told from new messages and a queued send's outcome is never
// only in a queue file that is pruned after a week.

// sendSenderCLI is the sender of a send made outside any agent-deck session.
const sendSenderCLI = "cli"

// queueSendIDEnv carries the queued send a --queue-worker child delivers.
const queueSendIDEnv = "AGENTDECK_QUEUE_SEND_ID"

// sendTextHashLen is how many hex digits of the text's keyed hash a send
// record keeps: enough to tell resends of the same text from new ones.
const sendTextHashLen = 16

// sendTextKeyFile holds the per-profile random key the text hash is keyed
// with, next to the journal. Without it a short text ("continue", "yes")
// would be recoverable from a plain hash by trying common messages.
const sendTextKeyFile = "send-text.key"

// sendJournalSender is the sender a send record carries: the calling
// session's id, or "cli" from a plain shell.
func sendJournalSender() string {
	if id := sendSenderID(); id != "" {
		return id
	}
	return sendSenderCLI
}

// sendJournalMeta is who sent which text, for one send record.
type sendJournalMeta struct {
	sender   string // calling session id, or sendSenderCLI
	textHash string // keyed hash prefix of the text as delivered; "" if no key
	textLen  int    // bytes of the text as delivered
	sendID   string // queued send being delivered, "" for a direct send
	attempt  int    // the queued send's delivery attempt (1-based)
}

// addTo writes the meta's additive keys into a send record's detail. The
// text itself is never journaled.
func (m sendJournalMeta) addTo(detail map[string]any) {
	detail["sender"] = m.sender
	detail["text_len"] = m.textLen
	if m.textHash != "" {
		detail["text_hmac"] = m.textHash
	}
	if m.sendID != "" {
		detail["send_id"] = m.sendID
		detail["attempt"] = m.attempt
	}
}

// sendJournalMetaFor is the journal identity of one send: the caller as
// sender, or, for a --queue-worker delivery, the queued record's sender,
// send_id and attempt (the worker's environment belongs to whoever started
// the worker, not necessarily to who queued this send).
func sendJournalMetaFor(profile string, storage *session.Storage, queueWorker bool, message string) sendJournalMeta {
	meta := sendJournalMeta{sender: sendJournalSender(), textHash: sendTextHash(profile, message), textLen: len(message)}
	sendID := os.Getenv(queueSendIDEnv)
	if !queueWorker || sendID == "" || storage == nil {
		return meta
	}
	rec, err := sendqueue.Load(sendQueueDir(storage), sendID)
	if err != nil {
		return meta
	}
	meta.sendID, meta.attempt = sendID, rec.Attempts
	if rec.Sender != "" {
		meta.sender = rec.Sender
	}
	return meta
}

var sendTextKeys sync.Map // profile -> []byte

// sendTextHash is the first sendTextHashLen hex digits of HMAC-SHA256 of
// text under the profile's key: equal texts hash equal within one install,
// and nothing outside it can test a guess. "" when the key is unavailable.
func sendTextHash(profile, text string) string {
	key := sendTextKey(profile)
	if key == nil {
		return ""
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(text))
	return hex.EncodeToString(mac.Sum(nil))[:sendTextHashLen]
}

func sendTextKey(profile string) []byte {
	if k, ok := sendTextKeys.Load(profile); ok {
		return k.([]byte)
	}
	dir, err := session.HealthLogDir(profile)
	if err != nil {
		return nil
	}
	key, err := loadOrCreateKey(filepath.Join(dir, sendTextKeyFile))
	if err != nil {
		return nil
	}
	sendTextKeys.Store(profile, key)
	return key
}

// loadOrCreateKey reads a 32-byte key, creating it once. The new key is
// written to a temp file and linked into place, so a concurrent reader sees
// either no key or the whole key, and two creators agree on the winner.
func loadOrCreateKey(path string) ([]byte, error) {
	if k, err := os.ReadFile(path); err == nil && len(k) == 32 {
		return k, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), sendTextKeyFile+".tmp*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	_, werr := tmp.Write(k)
	if cerr := tmp.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return nil, werr
	}
	if err := os.Chmod(tmp.Name(), 0o600); err != nil {
		return nil, err
	}
	if err := os.Link(tmp.Name(), path); err != nil && !os.IsExist(err) {
		return nil, err
	}
	got, err := os.ReadFile(path)
	if err != nil || len(got) != 32 {
		return nil, fmt.Errorf("send text key %s unreadable", path)
	}
	return got, nil
}

// queuedOutcomeCancelled is the final journal outcome of a send removed by
// `session queue cancel` before any typing: neither delivered nor failed.
const queuedOutcomeCancelled = "cancelled"

// queuedOutcome maps a final queue state onto the journal's send outcomes.
func queuedOutcome(r *sendqueue.Record) string {
	switch {
	case r.State == sendqueue.StateCancelled:
		return queuedOutcomeCancelled
	case r.State == sendqueue.StateFailed:
		return health.SendFailed
	case r.State == sendqueue.StateLanded, r.State == sendqueue.StateSubmitted:
		return health.SendConfirmed
	}
	return health.SendUnconfirmed
}

// finishQueuedSend runs once when a queued send becomes final. It journals
// one terminal record for the send_id (detail.final = true; the per-attempt
// records stay as they were). A terminal failure or typed-not-submitted
// result also notifies a known agent sender when notify is set. The sender
// already returned from queueing, so it needs this later outcome.
func finishQueuedSend(profile string, r *sendqueue.Record, notify bool) {
	detail := map[string]any{
		"outcome": queuedOutcome(r), "delivery": r.State, "final": true,
		"attempts": r.Attempts, "reason": r.Reason,
	}
	sendJournalMeta{sender: r.Sender, textHash: sendTextHash(profile, r.Message), textLen: len(r.Message), sendID: r.SendID, attempt: r.Attempts}.addTo(detail)
	delete(detail, "attempt") // "attempts" is the total for a final record
	if r.Sender == "" {
		detail["sender"] = "unknown"
	}
	recordSendEvent(profile, r.SessionID, detail)
	inboxOwned := notify && notifySenderOfFailedSend(profile, r)
	ledgerQueuedSend(r, inboxOwned)
}

// notifySenderOfFailedSend reports terminal send failures to a known agent.
// A failed record guarantees no typing; a typed-not-submitted record does not
// permit automatic resend. Plain shells and unknown senders keep ledger fallback.
// True means the inbox durably owns the notice, not that its wake was delivered.
func notifySenderOfFailedSend(profile string, r *sendqueue.Record) bool {
	if r.Sender == "" || r.Sender == sendSenderCLI {
		return false
	}
	title := r.SessionTitle
	if title == "" {
		title = r.SessionID
	}
	status, text := "", ""
	switch {
	case r.State == sendqueue.StateFailed:
		status = "send_failed"
		text = fmt.Sprintf("Your queued send %s to '%s' was not delivered: %s. Nothing was typed, so resending is safe.", r.SendID, title, r.Reason)
	case r.State == sendqueue.StateTyped && r.Final() && r.DeliveryEvidence["delivery"] == deliveryTypedNotSubmitted:
		status = "send_typed_not_submitted"
		text = fmt.Sprintf("Your queued send %s to '%s' was typed, but submission was not confirmed: %s. Do not resend automatically. Check session send-status %s and the receiver before retrying.", r.SendID, title, r.Reason, r.SendID)
	default:
		return false
	}
	profile, err := session.ResolveProfileForStorage(profile)
	if err != nil {
		return false
	}
	storage, instances, _, err := loadSessionData(profile)
	if err != nil {
		return false
	}
	defer storage.Close()
	sender := instanceByID(instances, r.Sender)
	if sender == nil || !(session.IsClaudeCompatible(sender.Tool) || session.IsCodexCompatible(sender.Tool)) {
		return false
	}
	return session.CommitInboxNotification(sender, session.TransitionNotificationEvent{
		ChildSessionID: r.SessionID,
		ChildTitle:     title,
		Profile:        profile,
		FromStatus:     "queued",
		ToStatus:       status,
		Tier:           session.TurnTierUrgent,
		Timestamp:      time.Now(),
		LastOutputHash: "send:" + r.SendID,
		Text:           text,
	}) == nil
}
