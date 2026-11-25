// Package integration — how workflows and batches end, and what a retry goes on with.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/pkg/types"
)

func tallyTask(name string, deps ...string) types.WorkflowTask {
	return types.WorkflowTask{Name: name, TaskType: "tally.wf", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, name)), DependsOn: deps}
}

func waitWorkflow(t *testing.T, cli *client.Client, id string, want types.WorkflowStatus) *types.WorkflowInstance {
	t.Helper()
	var wf *types.WorkflowInstance
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got, _, err := cli.Store().GetWorkflow(context.Background(), id)
		if err == nil {
			wf = got
			if got.Status == want {
				return got
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	for name, st := range wf.Tasks {
		t.Logf("  task %s: %s skipped=%v child=%q job=%q", name, st.Status, st.Skipped, st.ChildWorkflowID, st.JobID)
	}
	t.Fatalf("workflow is %s, want %s", wf.Status, want)
	return nil
}

// A root task that is a workflow of its own is started.
func TestWorkflow_ChildWorkflowAtRoot(t *testing.T) {
	for name, extra := range map[string][]types.WorkflowTask{"only root": nil, "with a plain root": {tallyTask("plain")}} {
		t.Run(name, func(t *testing.T) {
			var c tally
			cli := startTally(t, "tally.wf", &c, nil)
			child := types.WorkflowTask{Name: "child", ChildWorkflowDef: &types.WorkflowDefinition{Name: "inner", Tasks: []types.WorkflowTask{tallyTask("c1"), tallyTask("c2", "c1")}}}
			wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{Name: "outer", Tasks: append([]types.WorkflowTask{child, tallyTask("after", "child")}, extra...)}, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
			ids := []string{"c1", "c2", "after"}
			for _, e := range extra {
				ids = append(ids, e.Name)
			}
			if off := c.not(1, ids); len(off) > 0 {
				t.Errorf("did not run once: %v", off)
			}
		})
	}
}

// A workflow whose last task is skipped is done.
func TestWorkflow_LastTaskSkipped_Completes(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, nil)
	gated := tallyTask("gated", "plain")
	gated.Preconditions = []types.Precondition{{Type: "upstream_output", Task: "plain", Path: "ok", Expected: "yes"}}
	wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{Name: "w", Tasks: []types.WorkflowTask{tallyTask("plain"), gated}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
}

// The init hook starts with a workflow, and with a child workflow.
func TestWorkflow_InitHook(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, nil)
	hook := func(id string) *types.WorkflowHookDef {
		return &types.WorkflowHookDef{TaskType: "tally.wf", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))}
	}
	child := types.WorkflowTask{Name: "child", DependsOn: []string{"a"}, ChildWorkflowDef: &types.WorkflowDefinition{
		Name: "inner", Tasks: []types.WorkflowTask{tallyTask("c1")}, Hooks: &types.WorkflowHooks{OnInit: hook("child-init"), OnExit: hook("child-exit")},
	}}
	wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{
		Name: "w", Tasks: []types.WorkflowTask{tallyTask("a"), child},
		Hooks: &types.WorkflowHooks{OnInit: hook("init"), OnSuccess: hook("success"), OnExit: hook("exit")},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
	time.Sleep(2 * time.Second)
	if off := c.not(1, []string{"init", "success", "exit", "a", "c1", "child-init", "child-exit"}); len(off) > 0 {
		t.Errorf("did not run once: %v", off)
	}
}

// A task next to one that is skipped.
func TestWorkflow_TasksNextToSkippedOneRunOnce(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		if id[0] == 'x' || id[0] == 'y' {
			time.Sleep(400 * time.Millisecond)
		}
		return nil
	})
	for round := range 8 {
		n := func(s string) string { return fmt.Sprintf("%s%d", s, round) }
		gated := tallyTask(n("gated"), n("plain"))
		gated.Preconditions = []types.Precondition{{Type: "upstream_output", Task: n("plain"), Path: "ok", Expected: "yes"}}
		tasks := []types.WorkflowTask{tallyTask(n("plain")), gated, tallyTask(n("x"), n("plain")), tallyTask(n("y"), n("plain"))}
		if _, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{Name: "w", Tasks: tasks}, nil); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1500 * time.Millisecond)
		if off := c.not(1, []string{n("x"), n("y")}); len(off) > 0 {
			t.Errorf("did not run once: %v", off)
		}
	}
}

// The hooks start however a workflow ends.
func TestWorkflow_HooksOfEveryEnd(t *testing.T) {
	hook := func(id string) *types.WorkflowHookDef {
		return &types.WorkflowHookDef{TaskType: "tally.wf", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))}
	}
	hooks := func() *types.WorkflowHooks {
		return &types.WorkflowHooks{OnSuccess: hook("success"), OnFailure: hook("failure"), OnExit: hook("exit")}
	}
	timeout := types.Duration(2 * time.Second)
	lenient := tallyTask("bad", "a")
	lenient.AllowFailure = true
	gated := tallyTask("gated", "a")
	gated.Preconditions = []types.Precondition{{Type: "upstream_output", Task: "a", Path: "ok", Expected: "yes"}}
	child := func(inner ...types.WorkflowTask) types.WorkflowTask {
		return types.WorkflowTask{Name: "child", DependsOn: []string{"a"}, ChildWorkflowDef: &types.WorkflowDefinition{Name: "inner", Tasks: inner}}
	}
	for name, tc := range map[string]struct {
		def    types.WorkflowDefinition
		status types.WorkflowStatus
		hooks  []string
	}{
		"last task":            {types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("a")}}, types.WorkflowStatusCompleted, []string{"success", "exit"}},
		"failed task":          {types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("bad")}}, types.WorkflowStatusFailed, []string{"failure", "exit"}},
		"allowed failure last": {types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("a"), lenient}}, types.WorkflowStatusCompleted, []string{"success", "exit"}},
		"skipped last":         {types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("a"), gated}}, types.WorkflowStatusCompleted, []string{"success", "exit"}},
		"child last":           {types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("a"), child(tallyTask("c"))}}, types.WorkflowStatusCompleted, []string{"success", "exit"}},
		"child failed":         {types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("a"), child(tallyTask("bad"))}}, types.WorkflowStatusFailed, []string{"failure", "exit"}},
		"timed out":            {types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("slow")}, ExecutionTimeout: &timeout}, types.WorkflowStatusFailed, []string{"failure", "exit"}},
	} {
		t.Run(name, func(t *testing.T) {
			var c tally
			cli := startTally(t, "tally.wf", &c, func(id string) error {
				switch id {
				case "bad":
					return &types.NonRetryableError{Err: errBoom}
				case "slow":
					time.Sleep(9 * time.Second)
				}
				return nil
			})
			tc.def.Name, tc.def.Hooks = "w", hooks()
			wf, err := cli.EnqueueWorkflow(context.Background(), tc.def, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitWorkflow(t, cli, wf.ID, tc.status)
			time.Sleep(1500 * time.Millisecond)
			var never []string
			for _, h := range []string{"success", "failure", "exit"} {
				if !slices.Contains(tc.hooks, h) {
					never = append(never, h)
				}
			}
			if off := c.not(1, tc.hooks); len(off) > 0 {
				t.Errorf("hooks that did not run once: %v", off)
			}
			if off := c.not(0, never); len(off) > 0 {
				t.Errorf("hooks that ran: %v", off)
			}
		})
	}
}

// A workflow that failed and is retried ends twice.
func TestWorkflow_HooksAfterRetry(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		if id == "bad" && len(c.not(1, []string{id})) == 0 {
			return &types.NonRetryableError{Err: errBoom}
		}
		return nil
	})
	hook := func(id string) *types.WorkflowHookDef {
		return &types.WorkflowHookDef{TaskType: "tally.wf", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))}
	}
	wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{
		Name: "w", Tasks: []types.WorkflowTask{tallyTask("bad")},
		Hooks: &types.WorkflowHooks{OnInit: hook("init"), OnSuccess: hook("success"), OnFailure: hook("failure"), OnExit: hook("exit")},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkflow(t, cli, wf.ID, types.WorkflowStatusFailed)
	time.Sleep(time.Second)
	if _, err := cli.RetryWorkflow(context.Background(), wf.ID); err != nil {
		t.Fatal(err)
	}
	waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
	time.Sleep(1500 * time.Millisecond)
	if off := c.not(1, []string{"init", "success", "failure"}); len(off) > 0 {
		t.Errorf("did not run once: %v", off)
	}
	if off := c.not(2, []string{"exit"}); len(off) > 0 {
		t.Errorf("did not run twice: %v", off)
	}
}
