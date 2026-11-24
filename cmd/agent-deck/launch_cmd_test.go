package main

import (
	"crypto/sha256"
	"fmt"
	"github.com/asheshgoplani/agent-deck/internal/session"
	"github.com/asheshgoplani/agent-deck/internal/statedb"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

// TestLaunch_ToolWithFlags_FoldsExtrasIntoWrapper pins the CLI parse boundary
// of the issue-#601 data flow. The reporter's repro:
//
//	agent-deck launch . -c "codex --dangerously-bypass-approvals-and-sandbox --session-id UUID"
//
// must produce a wrapper string shaped exactly "{command} <extras>" with EVERY
// extra flag folded in. The session-layer test
// TestPrepareCommand_Issue601_ReporterRepro then proves that this exact wrapper
// shape results in flags landing INSIDE the bash -c single-quoted payload.
//
// Keeping assertions at both ends of the data-flow trace means neither layer
// can silently drop flags without a test turning red.
func TestLaunch_ToolWithFlags_FoldsExtrasIntoWrapper(t *testing.T) {
	raw := "codex --dangerously-bypass-approvals-and-sandbox --session-id aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	tool, command, wrapper, note, _, err := resolveSessionCommand(raw, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tool != "codex" {
		t.Fatalf("tool = %q, want codex", tool)
	}
	if command == "" {
		t.Fatalf("command should not be empty")
	}

	want := "{command} --dangerously-bypass-approvals-and-sandbox --session-id aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	if wrapper != want {
		t.Fatalf("wrapper did not fold extras correctly.\n  got:  %q\n  want: %q", wrapper, want)
	}
	if note == "" {
		t.Fatalf("expected resolveSessionCommand to emit a note explaining the wrapper fold; got empty")
	}
}

// TestLaunch_ToolWithSessionIdFlag_IsPreserved guards the narrower #601 symptom
// the reporter documented: --session-id silently dropped by bash-c-positional-args,
// breaking deterministic session IDs and JSONL file resolution for claude-compatible
// wrappers.
func TestLaunch_ToolWithSessionIdFlag_IsPreserved(t *testing.T) {
	raw := "my-claude-wrapper --session-id abc-123 --dangerously-skip-permissions"

	_, command, wrapper, _, _, err := resolveSessionCommand(raw, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if command != "my-claude-wrapper" {
		t.Fatalf("command = %q, want %q (base tool only)", command, "my-claude-wrapper")
	}
	want := "{command} --session-id abc-123 --dangerously-skip-permissions"
	if wrapper != want {
		t.Fatalf("wrapper shape dropped --session-id or --dang… flag.\n  got:  %q\n  want: %q", wrapper, want)
	}
}

func TestWorkflowLaunchAdmission(t *testing.T) {
	dir := t.TempDir()
	store := newFleetCLIStorage(t)
	parent := session.NewInstance("parent", dir)
	reviewer := session.NewInstance("reviewer", dir)
	child := session.NewInstance("writer", dir)
	child.SetParentWithPath(parent.ID, dir)
	child.Tool, child.Command = "shell", "custom-provider"
	peers := []*session.Instance{parent, reviewer}
	t.Setenv("AGENT_DECK_SESSION_ID", parent.ID)
	admit := func() (string, error) { return admitWorkflowLaunch(store, peers, child) }
	setHint := func(key, value string) {
		t.Helper()
		require.NoError(t, store.GetDB().SetSessionHint(statedb.HintScopeInstance, parent.ID, key, value, statedb.HintSourceAnnotate, ""))
	}
	_, err := admit()
	require.NoError(t, err) // ordinary managed session
	setHint("workflow-project", dir)
	_, err = admit()
	require.ErrorContains(t, err, "prepared brief")
	child.Tool, child.Command = "codex", "codex"
	child.Wrapper = "{command} --no-daemon --sandbox read-only --ask-for-approval never -c check_for_update_on_startup=false"
	_, err = admit()
	require.NoError(t, err)
	child.Command = "custom-provider"
	_, err = admit()
	require.ErrorContains(t, err, "prepared brief")

	brief := filepath.Join(dir, "brief")
	body := filepath.Join(dir, "issue")
	t.Setenv("WORKFLOW_TEST_BODY", body)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "gh"), []byte("#!/bin/sh\ncat \"$WORKFLOW_TEST_BODY\"\n"), 0700))
	setHint("workflow-brief", brief)
	const issue = "owner/repo#12"
	writePlan := func(header string) string {
		t.Helper()
		block := header + "\nTrace one case"
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(block)))
		require.NoError(t, os.WriteFile(body, []byte("<!-- agent-deck plan -->\n"+block+"\n<!-- /agent-deck plan -->"), 0600))
		require.NoError(t, os.WriteFile(brief, []byte(fmt.Sprintf("receiver_role: bounded-executor\nLaunch binding: {\"Parent\":%q,\"Directory\":%q,\"Issue\":%q,\"Plan\":%q}\n", parent.ID, dir, issue, hash)), 0600))
		return issue + " " + hash
	}
	want := writePlan("routine")
	got, err := admit()
	require.NoError(t, err)
	require.Equal(t, want, got)
	writePlan("required " + reviewer.ID + " " + parent.ID)
	_, err = admit()
	require.ErrorContains(t, err, "independent acceptance")
	want = writePlan("required " + parent.ID + " " + reviewer.ID)
	_, err = admit()
	require.ErrorContains(t, err, "independent acceptance")

	home := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", home)
	reviewer.Tool = "claude"
	reviewer.ClaudeSessionID = "9b9c6c02-3d87-4b84-a4e9-aac8a0b36211"
	projects := filepath.Join(home, "projects", session.ConvertToClaudeDirName(dir))
	require.NoError(t, os.MkdirAll(projects, 0700))
	writeClaudeJSONL(t, projects, reviewer.ClaudeSessionID, "Review", "PLAN ACCEPTED "+want, "2026-11-24T08:00:00Z")
	got, err = admit()
	require.NoError(t, err)
	require.Equal(t, want, got)
	child.BindQueuedWorkflowPlan(got)
	child.MarkQueued("")
	require.NoError(t, store.SaveWithGroups([]*session.Instance{child}, nil))
	stored, err := store.Load()
	require.NoError(t, err)
	var queued *session.Instance
	for _, inst := range stored {
		if inst.ID == child.ID {
			queued = inst
		}
	}
	require.NotNil(t, queued)
	require.Equal(t, child.WorkflowPlan, queued.WorkflowPlan)
	writePlan("required " + parent.ID + " " + reviewer.ID + "-changed")
	_, err = store.StartQueued(queued)
	require.ErrorContains(t, err, "queued workflow plan changed")
}

func TestWorkflowLaunchAdmissionCLI(t *testing.T) {
	home := t.TempDir()
	project := filepath.Join(home, "project")
	parent := sessionMoveAddSession(t, home, project, "lead")
	stdout, stderr, code := runAgentDeck(t, home, "session", "annotate", "--set-hint", "workflow-project="+project, parent)
	require.Zerof(t, code, "%s %s", stdout, stderr)
	stdout, stderr, code = runAgentDeckEnv(t, home, "", []string{"AGENT_DECK_SESSION_ID=" + parent}, "launch", "--no-parent", "--cmd", "sh", "-t", "unbound-writer", project)
	require.NotZero(t, code)
	require.Contains(t, stdout+stderr, "parent's prepared brief")
}
