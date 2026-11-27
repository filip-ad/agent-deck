package session

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/childenv"
)

// CodexQueueTarget is a local, pane-owned thread with a supported native queue
// command. It never uses resume, a model override, or the target's composer.
type CodexQueueTarget struct {
	ThreadID, Home, Binary, Rollout string
	From                            int64
	instance                        *Instance
}

func codexQueueCommand(ctx context.Context, target CodexQueueTarget, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, target.Binary, args...) // #nosec G204 -- the configured target command is resolved to an executable and run without a shell
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
	executable, rest, ok := nextShellWord(inst.resolveCodexCommand(inst.Command))
	if home, assigned := strings.CutPrefix(executable, "CODEX_HOME="); ok && assigned && strings.TrimSpace(home) != "" {
		executable, rest, ok = nextShellWord(rest)
	}
	if !ok || isShellEnvAssignment(executable) || strings.TrimSpace(rest) != "" {
		return target, fmt.Errorf("native Codex queue launch command is unsupported")
	}
	binary, err := exec.LookPath(executable)
	if err != nil {
		return target, fmt.Errorf("resolve native Codex queue command: %w", err)
	}
	path, err := inst.codexRolloutPath()
	if err != nil {
		return target, err
	}
	target = CodexQueueTarget{ThreadID: thread, Home: ExpandPath(inst.ResolvedCodexHome()), Binary: binary, Rollout: path, instance: inst}
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
