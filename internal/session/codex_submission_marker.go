package session

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/childenv"
)

const (
	codexSubmissionMarkerVersion = 1
	codexSubmissionMarkerMaxSize = 8 << 10

	CodexSubmissionPhasePrepared  = "prepared"
	CodexSubmissionPhaseSubmitted = "submitted"
	codexSubmissionPhaseAmbiguous = "transport_ambiguous"
)

// CodexSubmissionMarker is the content-free durable handoff between
// cooperating session-send processes. It records only which acceptance fence
// a send owns; prompts and responses never enter this file. Native queue
// markers also keep a message hash and exact rollout offset to require
// message-specific uptake.
type CodexSubmissionMarker struct {
	Version             int       `json:"version"`
	InstanceID          string    `json:"instance_id"`
	CodexSessionID      string    `json:"codex_session_id"`
	AttemptID           string    `json:"attempt_id"`
	PriorTurnGeneration string    `json:"prior_turn_generation"`
	Phase               string    `json:"phase"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	QueueMessageHash    string    `json:"queue_message_hash,omitempty"`
	QueueRollout        string    `json:"queue_rollout,omitempty"`
	QueueFrom           int64     `json:"queue_from,omitempty"`
}

// PrepareCodexSubmissionMarker durably publishes an unresolved attempt before
// any bytes are sent to the pane. The caller must hold the matching acceptance
// lock from before reconciliation until this write completes.
func PrepareCodexSubmissionMarker(instanceID, codexSessionID, priorGeneration string, now time.Time) (*CodexSubmissionMarker, error) {
	instanceID = strings.TrimSpace(instanceID)
	codexSessionID = strings.TrimSpace(codexSessionID)
	if instanceID == "" || len(instanceID) > 512 {
		return nil, fmt.Errorf("Codex submission marker: invalid instance id")
	}
	if err := validateExactSessionID(codexSessionID); err != nil {
		return nil, fmt.Errorf("Codex submission marker: invalid session identity: %w", err)
	}
	if err := validateCodexMarkerGeneration(codexSessionID, priorGeneration, true); err != nil {
		return nil, err
	}
	attemptID, err := newCodexSubmissionAttemptID()
	if err != nil {
		return nil, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	marker := &CodexSubmissionMarker{
		Version:             codexSubmissionMarkerVersion,
		InstanceID:          instanceID,
		CodexSessionID:      codexSessionID,
		AttemptID:           attemptID,
		PriorTurnGeneration: priorGeneration,
		Phase:               CodexSubmissionPhasePrepared,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	if err := writeCodexSubmissionMarker(marker, true); err != nil {
		return nil, err
	}
	return marker, nil
}

// MarkSubmitted records positive transport-level submission evidence. A crash
// before this update safely leaves the more conservative prepared phase.
func (m *CodexSubmissionMarker) MarkSubmitted(now time.Time) error {
	return m.updatePhase(CodexSubmissionPhaseSubmitted, now)
}

// MarkTransportAmbiguous records that transport was attempted without exact
// submission evidence. It deliberately remains unresolved until a new rollout
// generation appears, the running turn takes up later input, or an operator
// proves transport never occurred.
func (m *CodexSubmissionMarker) MarkTransportAmbiguous(now time.Time) error {
	return m.updatePhase(codexSubmissionPhaseAmbiguous, now)
}

func (m *CodexSubmissionMarker) updatePhase(phase string, now time.Time) error {
	if m == nil {
		return fmt.Errorf("Codex submission marker: nil marker")
	}
	if now.IsZero() {
		now = time.Now()
	}
	next := *m
	next.Phase = phase
	next.UpdatedAt = now.UTC()
	if err := writeCodexSubmissionMarker(&next, false); err != nil {
		return err
	}
	*m = next
	return nil
}

// ReconcileCodexSubmissionMarker resolves a crashed or completed prior sender
// while the matching acceptance lock is held. A different exact generation is
// assigned to that prior attempt and makes its marker safe to remove. Codex
// starts no turn for input sent while a turn runs (issue #2836), so
// tookInputSince (nil for none) may instead show that the unchanged turn took
// up user input recorded after the attempt began. Without either, delivery
// remains unknowable and all later sends fail closed.
func ReconcileCodexSubmissionMarker(
	instanceID, codexSessionID, currentGeneration string,
	tookInputSince func(generation string, since time.Time) (bool, error),
) (string, error) {
	marker, found, err := readCodexSubmissionMarker(codexSessionID)
	if err != nil || !found {
		return "", err
	}
	if marker.InstanceID != strings.TrimSpace(instanceID) || marker.CodexSessionID != strings.TrimSpace(codexSessionID) {
		return "", fmt.Errorf("Codex submission marker ownership does not match this instance")
	}
	if marker.QueueMessageHash != "" {
		landed, err := codexQueueInputLanded(marker)
		if err != nil || !landed {
			path, _ := codexSubmissionMarkerPath(codexSessionID)
			refusal := fmt.Errorf("unresolved native Codex queue submission %s at %s: matching input is not durable; inspect the exact rollout and native queue before recovery; refusing a second send", marker.AttemptID, path)
			if err != nil {
				return "", fmt.Errorf("%w (cannot read queued input: %v)", refusal, err)
			}
			return "", refusal
		}
		if err := ClearCodexSubmissionMarker(marker); err != nil {
			return "", err
		}
		return currentGeneration, nil
	}
	if currentGeneration == marker.PriorTurnGeneration {
		took, evidenceErr := false, error(nil)
		if currentGeneration != "" && tookInputSince != nil {
			took, evidenceErr = tookInputSince(currentGeneration, marker.CreatedAt)
		}
		if !took || evidenceErr != nil {
			path, pathErr := codexSubmissionMarkerPath(codexSessionID)
			if pathErr != nil {
				return "", pathErr
			}
			refusal := fmt.Errorf(
				"unresolved Codex submission %s at %s: no newer rollout generation is durable and the running turn took up no later input; inspect the exact Codex rollout and target pane, then manually remove this marker only if no turn was submitted; refusing a second send",
				marker.AttemptID, path,
			)
			if evidenceErr != nil {
				return "", fmt.Errorf("%w (cannot read the running turn's input: %v)", refusal, evidenceErr)
			}
			return "", refusal
		}
	} else if err := validateCodexMarkerGeneration(marker.CodexSessionID, currentGeneration, false); err != nil {
		return "", fmt.Errorf("cannot reconcile Codex submission marker: %w", err)
	}
	if err := ClearCodexSubmissionMarker(marker); err != nil {
		return "", err
	}
	return currentGeneration, nil
}

// ClearCodexSubmissionMarker removes only the exact attempt supplied by the
// caller. Callers use this after an exact accepted generation or definitive
// proof that transport never occurred, never after an ambiguous failure.
func ClearCodexSubmissionMarker(marker *CodexSubmissionMarker) error {
	if err := validateCodexSubmissionMarker(marker); err != nil {
		return err
	}
	current, found, err := readCodexSubmissionMarker(marker.CodexSessionID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Codex submission marker %s is missing", marker.AttemptID)
	}
	if current.InstanceID != marker.InstanceID || current.CodexSessionID != marker.CodexSessionID || current.AttemptID != marker.AttemptID {
		return fmt.Errorf("Codex submission marker ownership changed; refusing removal")
	}
	path, err := codexSubmissionMarkerPath(marker.CodexSessionID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove Codex submission marker: %w", err)
	}
	if err := fsyncDirStrict(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync Codex submission marker directory: %w", err)
	}
	return nil
}

func newCodexSubmissionAttemptID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("create Codex submission attempt id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func codexSubmissionMarkerDir() (string, error) {
	locks, err := resolveLocksDirForSpawnLock()
	if err != nil {
		return "", fmt.Errorf("resolve Codex submission marker directory: %w", err)
	}
	dir := filepath.Join(locks, "codex-submissions")
	_, statErr := os.Lstat(dir)
	created := os.IsNotExist(statErr)
	if statErr != nil && !created {
		return "", statErr
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create Codex submission marker directory: %w", err)
	}
	if created {
		if err := fsyncDirStrict(filepath.Dir(dir)); err != nil {
			return "", fmt.Errorf("sync Codex submission marker parent directory: %w", err)
		}
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if err := validatePrivateCodexMarkerPath(info, true); err != nil {
		return "", fmt.Errorf("unsafe Codex submission marker directory: %w", err)
	}
	return dir, nil
}

func codexSubmissionMarkerPath(codexSessionID string) (string, error) {
	codexSessionID = strings.TrimSpace(codexSessionID)
	if err := validateExactSessionID(codexSessionID); err != nil {
		return "", fmt.Errorf("Codex submission marker: invalid session identity: %w", err)
	}
	dir, err := codexSubmissionMarkerDir()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(codexSessionID))
	return filepath.Join(dir, hex.EncodeToString(digest[:])+".json"), nil
}

func readCodexSubmissionMarker(codexSessionID string) (*CodexSubmissionMarker, bool, error) {
	path, err := codexSubmissionMarkerPath(codexSessionID)
	if err != nil {
		return nil, false, err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("open Codex submission marker: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	if err := validatePrivateCodexMarkerPath(info, false); err != nil {
		return nil, false, fmt.Errorf("unsafe Codex submission marker: %w", err)
	}
	if info.Size() > codexSubmissionMarkerMaxSize {
		return nil, false, fmt.Errorf("Codex submission marker exceeds %d bytes", codexSubmissionMarkerMaxSize)
	}
	decoder := json.NewDecoder(io.LimitReader(f, codexSubmissionMarkerMaxSize+1))
	decoder.DisallowUnknownFields()
	var marker CodexSubmissionMarker
	if err := decoder.Decode(&marker); err != nil {
		return nil, false, fmt.Errorf("parse Codex submission marker: %w", err)
	}
	var extra interface{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, false, fmt.Errorf("parse Codex submission marker: trailing data")
	}
	if err := validateCodexSubmissionMarker(&marker); err != nil {
		return nil, false, err
	}
	if marker.CodexSessionID != strings.TrimSpace(codexSessionID) {
		return nil, false, fmt.Errorf("Codex submission marker session identity does not match its path")
	}
	return &marker, true, nil
}

func writeCodexSubmissionMarker(marker *CodexSubmissionMarker, create bool) error {
	if err := validateCodexSubmissionMarker(marker); err != nil {
		return err
	}
	path, err := codexSubmissionMarkerPath(marker.CodexSessionID)
	if err != nil {
		return err
	}
	current, found, err := readCodexSubmissionMarker(marker.CodexSessionID)
	if err != nil {
		return err
	}
	if create && found {
		return fmt.Errorf("unresolved Codex submission marker already exists")
	}
	if !create && (!found || current.InstanceID != marker.InstanceID || current.AttemptID != marker.AttemptID) {
		return fmt.Errorf("Codex submission marker ownership changed; refusing update")
	}
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(filepath.Dir(path), ".codex-submission-*.tmp")
	if err != nil {
		return fmt.Errorf("create Codex submission marker temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync Codex submission marker: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("publish Codex submission marker: %w", err)
	}
	if err := fsyncDirStrict(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync Codex submission marker directory: %w", err)
	}
	return nil
}

func validateCodexSubmissionMarker(marker *CodexSubmissionMarker) error {
	if marker == nil || marker.Version != codexSubmissionMarkerVersion {
		return fmt.Errorf("unsupported Codex submission marker version")
	}
	if strings.TrimSpace(marker.InstanceID) == "" || marker.InstanceID != strings.TrimSpace(marker.InstanceID) || len(marker.InstanceID) > 512 {
		return fmt.Errorf("invalid Codex submission marker instance identity")
	}
	if err := validateExactSessionID(marker.CodexSessionID); err != nil || marker.CodexSessionID != strings.TrimSpace(marker.CodexSessionID) {
		return fmt.Errorf("invalid Codex submission marker session identity")
	}
	attempt, err := hex.DecodeString(marker.AttemptID)
	if err != nil || len(attempt) != 16 {
		return fmt.Errorf("invalid Codex submission marker attempt identity")
	}
	if err := validateCodexMarkerGeneration(marker.CodexSessionID, marker.PriorTurnGeneration, true); err != nil {
		return err
	}
	if marker.QueueMessageHash != "" {
		digest, err := hex.DecodeString(marker.QueueMessageHash)
		if err != nil || len(digest) != sha256.Size || !filepath.IsAbs(marker.QueueRollout) || !strings.Contains(filepath.Base(marker.QueueRollout), marker.CodexSessionID) || marker.QueueFrom < 0 {
			return fmt.Errorf("invalid native Codex queue marker")
		}
	}
	switch marker.Phase {
	case CodexSubmissionPhasePrepared, CodexSubmissionPhaseSubmitted, codexSubmissionPhaseAmbiguous:
	default:
		return fmt.Errorf("invalid Codex submission marker lifecycle phase")
	}
	if marker.CreatedAt.IsZero() || marker.UpdatedAt.IsZero() || marker.UpdatedAt.Before(marker.CreatedAt) {
		return fmt.Errorf("invalid Codex submission marker timestamps")
	}
	return nil
}

func validateCodexMarkerGeneration(codexSessionID, generation string, allowEmpty bool) error {
	if generation == "" && allowEmpty {
		return nil
	}
	if generation == "" || len(generation) > 2048 || strings.ContainsRune(generation, '\x00') ||
		!strings.HasPrefix(generation, codexSessionID+":") || len(generation) == len(codexSessionID)+1 {
		return fmt.Errorf("invalid Codex submission marker turn generation")
	}
	return nil
}

func validatePrivateCodexMarkerPath(info os.FileInfo, wantDir bool) error {
	if info == nil || info.Mode()&os.ModeSymlink != 0 || info.IsDir() != wantDir || (!wantDir && !info.Mode().IsRegular()) {
		return fmt.Errorf("path has the wrong file type")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("path permissions %o are not private", info.Mode().Perm())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("path is not owned by the current user")
	}
	return nil
}

// CodexQueueTarget is a local, pane-owned thread with a supported native queue
// command. It never uses resume, a model override, or the target's composer.
type CodexQueueTarget struct {
	ThreadID, Home, Binary, Rollout string
	From                            int64
	instance                        *Instance
}

func codexQueueCommand(ctx context.Context, target CodexQueueTarget, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, target.Binary, args...)
	cmd.WaitDelay = time.Second
	for _, kv := range childenv.ForLaunch("") {
		if !strings.HasPrefix(kv, "CODEX_HOME=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "CODEX_HOME="+target.Home)
	return cmd
}

// ResolveCodexQueueTarget refuses unsupported commands and incomplete ownership
// before any queue write. The caller keeps the existing idle path on refusal.
func ResolveCodexQueueTarget(inst *Instance) (CodexQueueTarget, error) {
	var target CodexQueueTarget
	if inst == nil || !IsCodexCompatible(inst.Tool) || !inst.CodexRolloutIsResolvableLocally() {
		return target, fmt.Errorf("native Codex queue needs a local target")
	}
	thread := inst.LiveCodexUserThreadID()
	if thread == "" || thread != inst.CodexSessionID {
		return target, fmt.Errorf("native Codex queue has no exact pane-owned thread")
	}
	// Reuse launch parsing for a bare executable or a CODEX_HOME assignment.
	// Shell wrappers and extra launch arguments keep the existing idle path.
	command := inst.resolveCodexCommand(inst.Command)
	var executable string
	for command != "" {
		word, rest, ok := nextShellWord(command)
		if !ok {
			return target, fmt.Errorf("native Codex queue command is unsupported")
		}
		if isShellEnvAssignment(word) {
			if !strings.HasPrefix(word, "CODEX_HOME=") {
				return target, fmt.Errorf("native Codex queue command environment is unsupported")
			}
			command = strings.TrimSpace(rest)
			continue
		}
		if strings.TrimSpace(rest) != "" {
			return target, fmt.Errorf("native Codex queue launch arguments are unsupported")
		}
		executable = word
		break
	}
	binary, err := exec.LookPath(executable)
	if err != nil {
		return target, fmt.Errorf("resolve native Codex queue command: %w", err)
	}
	path, err := inst.codexRolloutPath()
	if err != nil {
		return target, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return target, err
	}
	target = CodexQueueTarget{ThreadID: thread, Home: ExpandPath(inst.ResolvedCodexHome()), Binary: binary, Rollout: path, From: info.Size(), instance: inst}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := codexQueueCommand(ctx, target, "queue", "--help").Output()
	if err != nil || !strings.Contains(string(out), "--thread") || !strings.Contains(string(out), "--message") {
		return CodexQueueTarget{}, fmt.Errorf("native Codex queue command is unsupported")
	}
	return target, nil
}

// Enqueue returns attempted=false only when the child never started. Once it
// starts, any error is ambiguous: callers must retain the marker and never
// fall back to keys or retry. Success proves enqueue only.
func (target CodexQueueTarget) Enqueue(message string) (attempted bool, err error) {
	if target.instance == nil || target.instance.LiveCodexUserThreadID() != target.ThreadID || target.instance.CodexSessionID != target.ThreadID || ExpandPath(target.instance.ResolvedCodexHome()) != target.Home {
		return false, fmt.Errorf("native Codex queue target changed before enqueue")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := codexQueueCommand(ctx, target, "queue", "--thread", target.ThreadID, "--message", message)
	if err := cmd.Start(); err != nil {
		return false, fmt.Errorf("start native Codex queue: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return true, fmt.Errorf("native Codex enqueue outcome is unknown: %w", err)
	}
	return true, nil
}

func codexQueueMessageHash(text string) string {
	// Native queue input is plain text. Whitespace is part of its identity.
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// PrepareQueue binds this marker to the exact message and rollout offset before
// enqueue. Only matching user input can resolve it; another turn cannot.
func (m *CodexSubmissionMarker) PrepareQueue(target CodexQueueTarget, message string) error {
	if m == nil || m.CodexSessionID != target.ThreadID {
		return fmt.Errorf("native Codex queue marker target mismatch")
	}
	next := *m
	next.QueueMessageHash = codexQueueMessageHash(message)
	next.QueueRollout, next.QueueFrom = target.Rollout, target.From
	if err := writeCodexSubmissionMarker(&next, false); err != nil {
		return err
	}
	*m = next
	return nil
}

// codexQueueInputLanded reads only appended user input in the exact rollout.
// A completed old turn, unrelated input, or enqueue acknowledgement is no proof.
func codexQueueInputLanded(m *CodexSubmissionMarker) (bool, error) {
	f, err := os.Open(m.QueueRollout)
	if err != nil {
		return false, err
	}
	defer f.Close()
	if _, err := f.Seek(m.QueueFrom, io.SeekStart); err != nil {
		return false, err
	}
	// Reuse the acceptance scan budget. Beyond this window, retain the
	// unresolved marker for inspection instead of claiming uptake.
	sc := bufio.NewScanner(io.LimitReader(f, codexTurnGenerationScanMaxBytes))
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		var row struct {
			Timestamp time.Time `json:"timestamp"`
			Type      string    `json:"type"`
			Payload   struct {
				Type    string `json:"type"`
				Role    string `json:"role"`
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"payload"`
		}
		if json.Unmarshal(sc.Bytes(), &row) != nil || row.Type != "response_item" || row.Payload.Type != "message" || row.Payload.Role != "user" || row.Timestamp.Before(m.CreatedAt.Add(-2*time.Second)) {
			continue
		}
		var text strings.Builder
		for _, part := range row.Payload.Content {
			if part.Type == "input_text" {
				text.WriteString(part.Text)
			}
		}
		if codexQueueMessageHash(text.String()) == m.QueueMessageHash {
			return true, nil
		}
	}
	return false, sc.Err()
}
