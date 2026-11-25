// Package integration — suspended workflows, child workflows that end in other ways than by
// completing or failing, and deadlines of retries.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/monitor"
	"github.com/FDK0901/dureq/pkg/types"
)

func newAPI(t *testing.T, cli *client.Client) *monitor.APIService {
	t.Helper()
	api := monitor.NewAPIService(cli.Store(), dispatcher.New(cli.Store(), nil), nil)
	t.Cleanup(api.Shutdown)
	return api
}

func tallyHook(id string) *types.WorkflowHookDef {
	return &types.WorkflowHookDef{TaskType: "tally.wf", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))}
}

// kindOf returns the last part of an ID. The parts before it tell the cases apart that run
// side by side.
func kindOf(id string) string { return id[strings.LastIndex(id, "-")+1:] }

func childTask(name string, inner types.WorkflowDefinition, deps ...string) types.WorkflowTask {
	inner.Name = "inner"
	return types.WorkflowTask{Name: name, DependsOn: deps, ChildWorkflowDef: &inner}
}

// waitChild waits for the task to have a child workflow with a task under way.
func waitChild(t *testing.T, cli *client.Client, wfID, task string) string {
	t.Helper()
	var id string
	waitFor(t, 10*time.Second, "child workflow", func() bool {
		wf, _, err := cli.Store().GetWorkflow(context.Background(), wfID)
		if err != nil || wf.Tasks[task].ChildWorkflowID == "" {
			return false
		}
		id = wf.Tasks[task].ChildWorkflowID
		child, _, err := cli.Store().GetWorkflow(context.Background(), id)
		if err != nil {
			return false
		}
		for _, st := range child.Tasks {
			if st.Status == types.JobStatusRunning {
				return true
			}
		}
		return false
	})
	return id
}

// A suspended workflow stays so when what is under way ends, and goes on once it is resumed.
func TestWorkflow_StaysSuspended(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		first := len(c.not(1, []string{id})) == 0
		switch kindOf(id) {
		case "next":
			return nil
		case "bad":
			time.Sleep(1500 * time.Millisecond)
			return &types.NonRetryableError{Err: errBoom}
		case "flaky":
			time.Sleep(1500 * time.Millisecond)
			if first {
				return &types.NonRetryableError{Err: errBoom}
			}
		case "slow":
			time.Sleep(1500 * time.Millisecond)
		}
		return nil
	})
	api := newAPI(t, cli)
	ctx := context.Background()
	for name, def := range map[string]types.WorkflowDefinition{
		"completes": {Tasks: []types.WorkflowTask{tallyTask("completes-slow"), tallyTask("completes-next", "completes-slow")}},
		"allowed": {Tasks: []types.WorkflowTask{
			func() types.WorkflowTask { task := tallyTask("allowed-bad"); task.AllowFailure = true; return task }(),
			tallyTask("allowed-next", "allowed-bad")}},
		"child": {Tasks: []types.WorkflowTask{
			childTask("child", types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("child-slow")}}), tallyTask("child-next", "child")}},
		"retried": {RetryPolicy: &types.RetryPolicy{MaxAttempts: 2}, Tasks: []types.WorkflowTask{tallyTask("retried-flaky"), tallyTask("retried-next", "retried-flaky")}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			def.Name = "w"
			wf, err := cli.EnqueueWorkflow(ctx, def, nil)
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(700 * time.Millisecond)
			if err := api.SuspendWorkflow(ctx, wf.ID); err != nil {
				t.Fatalf("suspend: %v", err)
			}
			time.Sleep(3500 * time.Millisecond)
			next := []string{name + "-next"}
			if off := c.not(0, next); len(off) > 0 {
				t.Errorf("ran while suspended: %v", off)
			}
			if off := c.not(1, []string{"retried-flaky"}); name == "retried" && len(off) > 0 {
				t.Errorf("retried while suspended: %v", off)
			}
			got, _, err := cli.Store().GetWorkflow(ctx, wf.ID)
			if err != nil || got.Status != types.WorkflowStatusSuspended {
				t.Fatalf("workflow is %s (%v), want suspended", got.Status, err)
			}
			if err := api.ResumeWorkflow(ctx, wf.ID); err != nil {
				t.Fatalf("resume: %v", err)
			}
			waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
			if off := c.not(1, next); len(off) > 0 {
				t.Errorf("did not run once: %v", off)
			}
		})
	}
}

// A parent learns of its child workflow whichever way that ends.
func TestWorkflow_ParentLearnsOfChild(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		if kindOf(id) == "slow" {
			time.Sleep(12 * time.Second)
		}
		return nil
	})
	api := newAPI(t, cli)
	ctx := context.Background()
	for name, stop := range map[string]func(id string) error{
		"timeout": nil,
		"client":  func(id string) error { return cli.CancelWorkflow(ctx, id) },
		"api":     func(id string) error { return api.CancelWorkflow(ctx, id) },
		"bulk": func(id string) error {
			_, err := api.BulkCancelWorkflows(ctx, monitor.BulkRequest{IDs: []string{id}})
			return err
		},
	} {
		for _, lenient := range []bool{false, true} {
			p := fmt.Sprintf("%s-%v-", name, lenient)
			t.Run(p, func(t *testing.T) {
				t.Parallel()
				inner := types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask(p + "slow")}}
				if stop == nil {
					timeout := types.Duration(2 * time.Second)
					inner.ExecutionTimeout = &timeout
				}
				child := childTask("child", inner)
				child.AllowFailure = lenient
				wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{
					Name: "outer", Tasks: []types.WorkflowTask{child, tallyTask(p+"after", "child")},
					Hooks: &types.WorkflowHooks{OnSuccess: tallyHook(p + "success"), OnFailure: tallyHook(p + "failure")},
				}, nil)
				if err != nil {
					t.Fatal(err)
				}
				childID := waitChild(t, cli, wf.ID, "child")
				if stop != nil {
					if err := stop(childID); err != nil {
						t.Fatal(err)
					}
				}
				want, hook, after := types.WorkflowStatusFailed, "failure", 0
				if lenient {
					want, hook, after = types.WorkflowStatusCompleted, "success", 1
				}
				got := waitWorkflow(t, cli, wf.ID, want)
				if st := got.Tasks["child"]; st.Status != types.JobStatusFailed || st.Error == nil {
					t.Errorf("task of the child: %+v", st)
				}
				time.Sleep(500 * time.Millisecond)
				if off := c.not(1, []string{p + hook}); len(off) > 0 {
					t.Errorf("hook did not run once: %v", off)
				}
				if off := c.not(after, []string{p + "after"}); len(off) > 0 {
					t.Errorf("after ran %v, want %d", off, after)
				}
			})
		}
	}
}

// A child that fails with its failure allowed does not fail its parent.
func TestWorkflow_ChildFailureAllowed(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		if id == "bad" {
			return &types.NonRetryableError{Err: errBoom}
		}
		return nil
	})
	child := childTask("child", types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("bad")}})
	child.AllowFailure = true
	wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{Name: "outer", Tasks: []types.WorkflowTask{child, tallyTask("after", "child")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
	if off := c.not(1, []string{"after"}); len(off) > 0 {
		t.Errorf("did not run once: %v", off)
	}
}

// A child that continues as new is done once the last of its instances is.
func TestWorkflow_ChildContinuesAsNew(t *testing.T) {
	for _, outcome := range []types.WorkflowStatus{types.WorkflowStatusCompleted, types.WorkflowStatusFailed} {
		t.Run(string(outcome), func(t *testing.T) {
			var c tally
			cli := startTally(t, "tally.wf", &c, func(id string) error {
				if id != "again" {
					return nil
				}
				switch {
				case len(c.not(1, []string{id})) == 0, len(c.not(2, []string{id})) == 0:
					return &types.ContinueAsNewError{Input: json.RawMessage(`{"id":"again"}`)}
				case outcome == types.WorkflowStatusFailed:
					return &types.NonRetryableError{Err: errBoom}
				}
				return nil
			})
			child := childTask("child", types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("again")}})
			wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{Name: "outer", Tasks: []types.WorkflowTask{child, tallyTask("after", "child")}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitWorkflow(t, cli, wf.ID, outcome)
			after := 0
			if outcome == types.WorkflowStatusCompleted {
				after = 1
			}
			if off := c.not(3, []string{"again"}); len(off) > 0 {
				t.Errorf("did not run three times: %v", off)
			}
			if off := c.not(after, []string{"after"}); len(off) > 0 {
				t.Errorf("after ran %v, want %d", off, after)
			}
		})
	}
}

// Ending a parent ends its child, and that does not change how the parent ended.
func TestWorkflow_EndOfParentEndsChild(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		if kindOf(id) == "slow" {
			time.Sleep(12 * time.Second)
		}
		return nil
	})
	ctx := context.Background()
	for name, want := range map[string]types.WorkflowStatus{"cancelled": types.WorkflowStatusCancelled, "timeout": types.WorkflowStatusFailed} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := name + "-"
			def := types.WorkflowDefinition{
				Name: "outer", Hooks: &types.WorkflowHooks{OnFailure: tallyHook(p + "failure")},
				Tasks: []types.WorkflowTask{childTask("child", types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask(p + "slow")}}), tallyTask(p+"after", "child")},
			}
			if name == "timeout" {
				timeout := types.Duration(2 * time.Second)
				def.ExecutionTimeout = &timeout
			}
			wf, err := cli.EnqueueWorkflow(ctx, def, nil)
			if err != nil {
				t.Fatal(err)
			}
			childID := waitChild(t, cli, wf.ID, "child")
			if name == "cancelled" {
				if err := cli.CancelWorkflow(ctx, wf.ID); err != nil {
					t.Fatal(err)
				}
			}
			waitWorkflow(t, cli, wf.ID, want)
			waitWorkflow(t, cli, childID, types.WorkflowStatusCancelled)
			time.Sleep(time.Second)
			got := waitWorkflow(t, cli, wf.ID, want)
			if st := got.Tasks["child"].Status; st != types.JobStatusCancelled {
				t.Errorf("task of the child is %s, want cancelled", st)
			}
			if off := c.not(0, []string{p + "after"}); len(off) > 0 {
				t.Errorf("ran: %v", off)
			}
			if off := c.not(1, []string{p + "failure"}); name == "timeout" && len(off) > 0 {
				t.Errorf("hook did not run once: %v", off)
			}
		})
	}
}

// The execution timeout holds for a retry by hand, too.
func TestRetry_HasDeadline(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(string) error { time.Sleep(40 * time.Second); return nil })
	api := newAPI(t, cli)
	ctx := context.Background()
	timeout := types.Duration(2 * time.Second)
	workflow := func(id string) types.WorkflowStatus {
		wf, _, err := cli.Store().GetWorkflow(ctx, id)
		if err != nil {
			return ""
		}
		return wf.Status
	}
	batch := func(id string) types.WorkflowStatus {
		b, err := cli.GetBatch(ctx, id)
		if err != nil {
			return ""
		}
		return b.Status
	}
	for name, tc := range map[string]struct {
		status func(id string) types.WorkflowStatus
		retry  func(id string) error
	}{
		"workflow-client": {workflow, func(id string) error { _, err := cli.RetryWorkflow(ctx, id); return err }},
		"workflow-api":    {workflow, func(id string) error { return api.RetryWorkflow(ctx, id) }},
		"workflow-bulk": {workflow, func(id string) error {
			_, err := api.BulkRetryWorkflows(ctx, monitor.BulkRequest{IDs: []string{id}})
			return err
		}},
		"batch-client": {batch, func(id string) error { _, err := cli.RetryBatch(ctx, id, true); return err }},
		"batch-api":    {batch, func(id string) error { return api.RetryBatch(ctx, id, true) }},
		"batch-bulk": {batch, func(id string) error {
			_, err := api.BulkRetryBatches(ctx, monitor.BulkRequest{IDs: []string{id}})
			return err
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var id string
			if strings.HasPrefix(name, "batch") {
				b, err := cli.EnqueueBatch(ctx, types.BatchDefinition{Name: name, ItemTaskType: "tally.wf", Items: []types.BatchItem{{ID: "i", Payload: json.RawMessage(`{}`)}}, ExecutionTimeout: &timeout})
				if err != nil {
					t.Fatal(err)
				}
				id = b.ID
			} else {
				wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{Name: name, ExecutionTimeout: &timeout, Tasks: []types.WorkflowTask{tallyTask(name)}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				id = wf.ID
			}
			waitFor(t, 14*time.Second, "timeout", func() bool { return tc.status(id) == types.WorkflowStatusFailed })
			since := time.Now()
			if err := tc.retry(id); err != nil {
				t.Fatal(err)
			}
			waitFor(t, 14*time.Second, "retry to time out", func() bool { return tc.status(id) == types.WorkflowStatusFailed })
			if d := time.Since(since); d < timeout.Std() {
				t.Errorf("timed out after %v, before the timeout", d)
			}
		})
	}
}

// Each attempt by a retry policy has the execution timeout to itself.
func TestRetryPolicy_HasDeadlinePerAttempt(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		first := len(c.not(1, []string{id})) == 0
		if !first && kindOf(id) == "hangs" {
			time.Sleep(60 * time.Second)
		}
		time.Sleep(6 * time.Second)
		if first {
			return &types.NonRetryableError{Err: errBoom}
		}
		return nil
	})
	ctx := context.Background()
	timeout := types.Duration(8 * time.Second)
	policy := &types.RetryPolicy{MaxAttempts: 2}
	for _, name := range []string{"workflow-completes", "workflow-hangs", "batch-completes", "batch-hangs"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			since := time.Now()
			var status func() types.WorkflowStatus
			if strings.HasPrefix(name, "batch") {
				b, err := cli.EnqueueBatch(ctx, types.BatchDefinition{
					Name: name, ItemTaskType: "tally.wf", Items: []types.BatchItem{{ID: name, Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, name))}},
					ExecutionTimeout: &timeout, RetryPolicy: policy, FailurePolicy: types.BatchContinueOnError,
				})
				if err != nil {
					t.Fatal(err)
				}
				status = func() types.WorkflowStatus {
					got, err := cli.GetBatch(ctx, b.ID)
					if err != nil {
						return ""
					}
					return got.Status
				}
			} else {
				wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{Name: name, ExecutionTimeout: &timeout, RetryPolicy: policy, Tasks: []types.WorkflowTask{tallyTask(name)}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				status = func() types.WorkflowStatus {
					got, _, err := cli.Store().GetWorkflow(ctx, wf.ID)
					if err != nil {
						return ""
					}
					return got.Status
				}
			}
			want := types.WorkflowStatusCompleted // after 12s, past the deadline of the first attempt
			if kindOf(name) == "hangs" {
				want = types.WorkflowStatusFailed // after 6s + 8s
			}
			waitFor(t, 25*time.Second, "the end", func() bool { return status().IsTerminal() })
			d := time.Since(since)
			if got := status(); got != want {
				t.Errorf("%s after %v, want %s", got, d, want)
			}
			if want == types.WorkflowStatusFailed && d < 6*time.Second+timeout.Std() {
				t.Errorf("timed out after %v, before the second attempt had its time", d)
			}
		})
	}
}
