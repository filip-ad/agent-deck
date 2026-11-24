package session

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// The parent binds its prepared brief before launch. The brief identifies the
// task. Only the owning issue supplies the review requirement and current plan.
type workflowBinding struct {
	Parent    string
	Directory string
	Issue     string
	Plan      string
}

var workflowIssueID = regexp.MustCompile(`^([a-zA-Z0-9_.-]+)/([a-zA-Z0-9_.-]+)#([1-9][0-9]*)$`)

func workflowIssueBody(issue string) (string, error) {
	m := workflowIssueID.FindStringSubmatch(issue)
	if m == nil || m[1] == "." || m[1] == ".." || m[2] == "." || m[2] == ".." {
		return "", fmt.Errorf("invalid workflow issue %q", issue)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	body, err := exec.CommandContext(ctx, "gh", "api", "--hostname", "github.com", "repos/"+m[1]+"/"+m[2]+"/issues/"+m[3], "--jq", `select(.state == "open" and .pull_request == null) | .body`).Output()
	if err != nil {
		return "", fmt.Errorf("reading workflow issue %s: %w", issue, err)
	}
	return string(body), nil
}

func workflowPlan(issue, body string) (pin, author, reviewer string, err error) {
	const start, end = "<!-- agent-deck plan -->", "<!-- /agent-deck plan -->"
	if strings.Count(body, start) != 1 || strings.Count(body, end) != 1 {
		return "", "", "", fmt.Errorf("%s needs one marked workflow plan", issue)
	}
	_, after, _ := strings.Cut(body, start)
	block, _, ok := strings.Cut(after, end)
	header, plan, _ := strings.Cut(strings.TrimSpace(block), "\n")
	fields := strings.Fields(header)
	if !ok || strings.TrimSpace(plan) == "" || len(fields) == 0 {
		return "", "", "", fmt.Errorf("%s has an incomplete workflow plan", issue)
	}
	switch {
	case len(fields) == 1 && fields[0] == "routine":
	case len(fields) == 3 && fields[0] == "required" && fields[1] != fields[2]:
		author, reviewer = fields[1], fields[2]
	default:
		return "", "", "", fmt.Errorf("%s must specify routine or required with distinct author and reviewer IDs", issue)
	}
	return fmt.Sprintf("%s %x", issue, sha256.Sum256([]byte(strings.TrimSpace(block)))), author, reviewer, nil
}

const workflowReadOnly = "read-only"

func (i *Instance) workflowReadOnly() bool {
	return i.Tool == "codex" && i.resolveCodexCommand(i.Command) == "codex" &&
		i.Wrapper == "{command} --no-daemon --sandbox read-only --ask-for-approval never -c check_for_update_on_startup=false" &&
		i.resolveCodexYoloFlag() == ""
}

// CheckWorkflowLaunch admits the command the launcher actually resolved.
// A custom command or wrapper is a writer unless it passes the review check.
func (i *Instance) CheckWorkflowLaunch(parent *Instance, brief string, peers []*Instance) (string, error) {
	if i.workflowReadOnly() {
		return workflowReadOnly, nil
	}
	if !filepath.IsAbs(brief) {
		return "", fmt.Errorf("workflow writer needs the parent's prepared brief")
	}
	data, err := os.ReadFile(brief)
	if err != nil {
		return "", fmt.Errorf("reading workflow brief: %w", err)
	}
	lines := strings.SplitN(string(data), "\n", 3)
	var binding workflowBinding
	if len(lines) < 2 || !strings.HasPrefix(lines[1], "Launch binding: ") ||
		json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "Launch binding: ")), &binding) != nil ||
		binding.Parent != parent.ID || binding.Directory != i.ProjectPath {
		return "", fmt.Errorf("workflow brief must bind the launching parent and child directory")
	}
	linkedHelper := lines[0] == "receiver_role: bounded-executor" && i.ParentSessionID == parent.ID
	topLevelLead := lines[0] == "receiver_role: orchestrator" && i.ParentSessionID == ""
	if !linkedHelper && !topLevelLead {
		return "", fmt.Errorf("workflow brief role does not match the launch parent")
	}
	body, err := workflowIssueBody(binding.Issue)
	if err != nil {
		return "", err
	}
	pin, author, reviewer, err := workflowPlan(binding.Issue, body)
	if err != nil {
		return "", err
	}
	if pin != binding.Issue+" "+binding.Plan {
		return "", fmt.Errorf("workflow brief does not name the current plan for %s", binding.Issue)
	}
	if reviewer == "" {
		return pin, nil
	}
	var planAuthor *Instance
	for _, peer := range peers {
		if peer.ID == author {
			planAuthor = peer
			break
		}
	}
	if planAuthor == nil {
		return "", fmt.Errorf("cannot verify workflow plan author %q", author)
	}
	for _, peer := range peers {
		if peer.ID != reviewer || sameWorkflowSession(peer, planAuthor) || sameWorkflowSession(peer, parent) || sameWorkflowSession(peer, i) {
			continue
		}
		version := peer.ResponseContentVersion(peers)
		if version == "" {
			break
		}
		response, exact, err := peer.GetLastResponseAtVersion(peers, version)
		if err == nil && exact && response != nil {
			line, _, _ := strings.Cut(strings.TrimSpace(response.Content), "\n")
			if line == "PLAN ACCEPTED "+pin {
				return pin, nil
			}
		}
	}
	return "", fmt.Errorf("workflow writer needs independent acceptance of %s", pin)
}

func sameWorkflowSession(a, b *Instance) bool {
	return a.ID == b.ID || (a.CodexSessionID != "" && a.CodexSessionID == b.CodexSessionID) ||
		(a.ClaudeSessionID != "" && a.ClaudeSessionID == b.ClaudeSessionID)
}

// The saved pin includes routine plans. A task can start to require review
// while it waits for capacity.
func (i *Instance) checkQueuedWorkflowPlan() error {
	if i.WorkflowPlan == "" {
		return nil
	}
	if i.WorkflowPlan == workflowReadOnly {
		if !i.workflowReadOnly() {
			return fmt.Errorf("queued workflow reader needs the bound read-only command")
		}
		return nil
	}
	pinned, target, ok := strings.Cut(i.WorkflowPlan, "\n")
	if !ok || target != i.ParentSessionID+"\n"+i.ProjectPath {
		return fmt.Errorf("queued workflow writer changed parent or directory")
	}
	issue, _, _ := strings.Cut(pinned, " ")
	body, err := workflowIssueBody(issue)
	if err != nil {
		return err
	}
	pin, _, _, err := workflowPlan(issue, body)
	if err != nil {
		return err
	}
	if pin != pinned {
		return fmt.Errorf("queued workflow plan changed for %s", issue)
	}
	return nil
}

// Bind the queued writer to the destination admitted by the caller.
func (i *Instance) BindQueuedWorkflowPlan(pin string) {
	i.WorkflowPlan = pin
	if pin != "" && pin != workflowReadOnly {
		i.WorkflowPlan += "\n" + i.ParentSessionID + "\n" + i.ProjectPath
	}
}

func readWorkflowPlan(td json.RawMessage) string {
	var blob struct {
		Plan string `json:"workflow_plan"`
	}
	_ = json.Unmarshal(td, &blob)
	return blob.Plan
}
