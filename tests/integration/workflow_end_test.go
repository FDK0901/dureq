// Package integration — how workflows and batches end, and what a retry goes on with.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/monitor"
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
	child := types.WorkflowTask{Name: "child", DependsOn: []string{"a"}, ChildWorkflowDef: &types.WorkflowDefinition{
		Name: "inner", Tasks: []types.WorkflowTask{tallyTask("c1")}, Hooks: &types.WorkflowHooks{OnInit: tallyHook("child-init"), OnExit: tallyHook("child-exit")},
	}}
	wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{
		Name: "w", Tasks: []types.WorkflowTask{tallyTask("a"), child},
		Hooks: &types.WorkflowHooks{OnInit: tallyHook("init"), OnSuccess: tallyHook("success"), OnExit: tallyHook("exit")},
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

// A batch that was stopped with items pending and under way completes when it is retried,
// and no item that succeeded runs again.
func TestBatch_RetryOfStoppedBatch(t *testing.T) {
	var c tally
	var mu sync.Mutex
	release := make(map[string]chan struct{})
	cli := startTally(t, "tally.stop", &c, func(id string) error {
		first := len(c.not(1, []string{id})) == 0
		switch {
		case strings.HasPrefix(id, "failfast") && kindOf(id) == "i002" && first:
			return &types.NonRetryableError{Err: errBoom}
		case kindOf(id) == "i004" && first: // is under way when the batch is stopped
			mu.Lock()
			ch := release[strings.TrimSuffix(id, "i004")]
			mu.Unlock()
			select {
			case <-ch:
			case <-time.After(20 * time.Second):
			}
		}
		return nil
	})
	api := newAPI(t, cli)
	ctx := context.Background()
	for _, name := range []string{"failfast", "cancelled", "timeout"} {
		for how, retry := range map[string]func(id string) error{
			"client": func(id string) error { _, err := cli.RetryBatch(ctx, id, true); return err },
			"api":    func(id string) error { return api.RetryBatch(ctx, id, true) },
			"bulk": func(id string) error {
				_, err := api.BulkRetryBatches(ctx, monitor.BulkRequest{IDs: []string{id}})
				return err
			},
		} {
			p := name + "-" + how + "-"
			ch := make(chan struct{})
			mu.Lock()
			release[p] = ch
			mu.Unlock()
			t.Run(p, func(t *testing.T) {
				t.Parallel()
				items, ids := batchItems(12)
				for i := range items {
					ids[i] = p + items[i].ID
					items[i].Payload = json.RawMessage(fmt.Sprintf(`{"id":%q}`, ids[i]))
				}
				def := types.BatchDefinition{Name: "stop", ItemTaskType: "tally.stop", Items: items, ChunkSize: 5, FailurePolicy: types.BatchContinueOnError}
				switch name {
				case "failfast":
					def.FailurePolicy = types.BatchFailFast
				case "timeout":
					timeout := types.Duration(3 * time.Second)
					def.ExecutionTimeout = &timeout
				}
				batch, err := cli.EnqueueBatch(ctx, def)
				if err != nil {
					t.Fatal(err)
				}
				if name == "cancelled" {
					waitFor(t, 10*time.Second, "i004 to run", func() bool { return len(c.not(1, []string{p + "i004"})) == 0 })
					if err := cli.CancelBatch(ctx, batch.ID); err != nil {
						t.Fatal(err)
					}
				}
				stopped := waitBatch(t, cli, batch.ID)
				close(ch)
				time.Sleep(time.Second)
				if stopped.Status == types.WorkflowStatusCompleted {
					t.Fatalf("batch completed, want it stopped: %+v", stopped)
				}
				if err := retry(batch.ID); err != nil {
					t.Fatalf("retry: %v", err)
				}
				var got *types.BatchInstance
				for deadline := time.Now().Add(12 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
					if got, err = cli.GetBatch(ctx, batch.ID); err == nil && got.Status.IsTerminal() {
						break
					}
				}
				if got.Status != types.WorkflowStatusCompleted || got.CompletedItems != 12 || got.RunningItems != 0 || got.PendingItems != 0 {
					states := map[types.JobStatus]int{}
					for _, s := range got.ItemStates {
						states[s.Status]++
					}
					t.Errorf("batch %s: %d completed, %d failed, %d running, %d pending; items %v", got.Status, got.CompletedItems, got.FailedItems, got.RunningItems, got.PendingItems, states)
				}
				want := map[string]int{}
				if name == "failfast" {
					want[p+"i002"] = 2 // failed, and retried
				}
				if off := c.not(1, ids); !maps.Equal(off, want) {
					t.Errorf("not run once: %v, want %v", off, want)
				}
			})
		}
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

// A task that is under way when the workflow fails, and done by the time it is retried.
func TestWorkflow_RetryWithTaskUnderWay(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		first := len(c.not(1, []string{id})) == 0
		if kindOf(id) == "slow" {
			time.Sleep(1500 * time.Millisecond)
		}
		if kindOf(id) == "bad" && first {
			return &types.NonRetryableError{Err: errBoom}
		}
		return nil
	})
	api := newAPI(t, cli)
	ctx := context.Background()
	for how, retry := range map[string]func(id string) error{
		"client": func(id string) error { _, err := cli.RetryWorkflow(ctx, id); return err },
		"api":    func(id string) error { return api.RetryWorkflow(ctx, id) },
		"bulk": func(id string) error {
			_, err := api.BulkRetryWorkflows(ctx, monitor.BulkRequest{IDs: []string{id}})
			return err
		},
	} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()
			slow, bad, join := how+"-slow", how+"-bad", how+"-join"
			wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{Name: "w", Tasks: []types.WorkflowTask{tallyTask(slow), tallyTask(bad), tallyTask(join, slow, bad)}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitWorkflow(t, cli, wf.ID, types.WorkflowStatusFailed)
			time.Sleep(2500 * time.Millisecond)
			if err := retry(wf.ID); err != nil {
				t.Fatal(err)
			}
			waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
			if off := c.not(1, []string{slow, join}); len(off) > 0 {
				t.Errorf("did not run once: %v", off)
			}
		})
	}
}

// Retrying all items runs all of them again.
func TestBatch_RetryAllItems(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.all", &c, func(id string) error {
		if id == "i007" && len(c.not(1, []string{id})) == 0 {
			return &types.NonRetryableError{Err: errBoom}
		}
		return nil
	})
	items, ids := batchItems(12)
	batch, err := cli.EnqueueBatch(context.Background(), types.BatchDefinition{Name: "all", ItemTaskType: "tally.all", Items: items, ChunkSize: 5, FailurePolicy: types.BatchContinueOnError})
	if err != nil {
		t.Fatal(err)
	}
	waitBatch(t, cli, batch.ID)
	if _, err := cli.RetryBatch(context.Background(), batch.ID, false); err != nil {
		t.Fatal(err)
	}
	waitBatch(t, cli, batch.ID)
	time.Sleep(time.Second)
	if off := c.not(2, ids); len(off) > 0 {
		t.Errorf("did not run twice: %v", off)
	}
}

// The hooks start however a workflow ends.
func TestWorkflow_HooksOfEveryEnd(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		switch kindOf(id) {
		case "bad":
			return &types.NonRetryableError{Err: errBoom}
		case "slow":
			time.Sleep(9 * time.Second)
		}
		return nil
	})
	timeout := types.Duration(2 * time.Second)
	completed, failed := types.WorkflowStatusCompleted, types.WorkflowStatusFailed
	child := func(p string, kind string) types.WorkflowTask {
		return childTask("child", types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask(p + kind)}}, p+"a")
	}
	for name, tc := range map[string]struct {
		def    func(p string) types.WorkflowDefinition
		status types.WorkflowStatus
	}{
		"last": {func(p string) types.WorkflowDefinition {
			return types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask(p + "a")}}
		}, completed},
		"failed": {func(p string) types.WorkflowDefinition {
			return types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask(p + "bad")}}
		}, failed},
		"allowed": {func(p string) types.WorkflowDefinition {
			lenient := tallyTask(p+"bad", p+"a")
			lenient.AllowFailure = true
			return types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask(p + "a"), lenient}}
		}, completed},
		"skipped": {func(p string) types.WorkflowDefinition {
			gated := tallyTask(p+"gated", p+"a")
			gated.Preconditions = []types.Precondition{{Type: "upstream_output", Task: p + "a", Path: "ok", Expected: "yes"}}
			return types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask(p + "a"), gated}}
		}, completed},
		"child": {func(p string) types.WorkflowDefinition {
			return types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask(p + "a"), child(p, "c")}}
		}, completed},
		"childfailed": {func(p string) types.WorkflowDefinition {
			return types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask(p + "a"), child(p, "bad")}}
		}, failed},
		"timeout": {func(p string) types.WorkflowDefinition {
			return types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask(p + "slow")}, ExecutionTimeout: &timeout}
		}, failed},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := name + "-"
			def := tc.def(p)
			def.Name, def.Hooks = "w", &types.WorkflowHooks{OnSuccess: tallyHook(p + "success"), OnFailure: tallyHook(p + "failure"), OnExit: tallyHook(p + "exit")}
			wf, err := cli.EnqueueWorkflow(context.Background(), def, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitWorkflow(t, cli, wf.ID, tc.status)
			time.Sleep(1500 * time.Millisecond)
			hook, other := p+"success", p+"failure"
			if tc.status == failed {
				hook, other = other, hook
			}
			if off := c.not(1, []string{hook, p + "exit"}); len(off) > 0 {
				t.Errorf("hooks that did not run once: %v", off)
			}
			if off := c.not(0, []string{other}); len(off) > 0 {
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
	wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{
		Name: "w", Tasks: []types.WorkflowTask{tallyTask("bad")},
		Hooks: &types.WorkflowHooks{OnInit: tallyHook("init"), OnSuccess: tallyHook("success"), OnFailure: tallyHook("failure"), OnExit: tallyHook("exit")},
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

// A child workflow that is under way when its parent fails, and done by the time that is
// retried.
func TestWorkflow_RetryWithChildUnderWay(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		first := len(c.not(1, []string{id})) == 0
		if id == "inner" {
			time.Sleep(1500 * time.Millisecond)
		}
		if id == "bad" && first {
			return &types.NonRetryableError{Err: errBoom}
		}
		return nil
	})
	child := types.WorkflowTask{Name: "child", ChildWorkflowDef: &types.WorkflowDefinition{Name: "inner", Tasks: []types.WorkflowTask{tallyTask("inner")}}}
	wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{Name: "w", Tasks: []types.WorkflowTask{child, tallyTask("bad"), tallyTask("join", "child", "bad")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkflow(t, cli, wf.ID, types.WorkflowStatusFailed)
	time.Sleep(2500 * time.Millisecond)
	if _, err := cli.RetryWorkflow(context.Background(), wf.ID); err != nil {
		t.Fatal(err)
	}
	waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
	if off := c.not(1, []string{"inner", "join"}); len(off) > 0 {
		t.Errorf("did not run once: %v", off)
	}
}

// A task whose result is never to be reused runs again when the workflow is retried.
func TestWorkflow_RetryRunsTaskWithoutReuse(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		if id == "bad" && len(c.not(1, []string{id})) == 0 {
			return &types.NonRetryableError{Err: errBoom}
		}
		return nil
	})
	again, kept := tallyTask("again", "kept"), tallyTask("kept")
	again.ResultReuse = types.ResultReuseNever
	wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{
		Name: "w", Tasks: []types.WorkflowTask{again, kept, tallyTask("bad", "again", "kept")}, RetryPolicy: &types.RetryPolicy{MaxAttempts: 2},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
	if off := c.not(2, []string{"again", "bad"}); len(off) > 0 {
		t.Errorf("did not run twice: %v", off)
	}
	if off := c.not(1, []string{"kept"}); len(off) > 0 {
		t.Errorf("did not run once: %v", off)
	}
}

// What is retried while it is still under way from before counts once it is done, whichever
// of the two executions is done first.
func TestRetry_WhileStillUnderWay(t *testing.T) {
	var c tally
	var mu sync.Mutex
	gates := make(map[string][2]chan struct{}) // of the first and second execution
	gate := func(id string) [2]chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := gates[id]; !ok {
			gates[id] = [2]chan struct{}{make(chan struct{}), make(chan struct{})}
		}
		return gates[id]
	}
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		n := 1
		if len(c.not(1, []string{id})) == 0 {
			n = 0
		}
		select { // not stopped by the cancellation
		case <-gate(id)[n]:
		case <-time.After(20 * time.Second):
		}
		return nil
	})
	ctx := context.Background()
	for _, what := range []string{"batch", "workflow"} {
		for _, stop := range []string{"cancelled", "timeout"} {
			for _, first := range []string{"old", "new"} {
				id := what + "-" + stop + "-" + first
				t.Run(id, func(t *testing.T) {
					t.Parallel()
					var timeout *types.Duration
					if stop == "timeout" {
						d := types.Duration(15 * time.Second)
						timeout = &d
					}
					var status func() types.WorkflowStatus
					var cancel, retry func() error
					if what == "batch" {
						b, err := cli.EnqueueBatch(ctx, types.BatchDefinition{Name: id, ItemTaskType: "tally.wf", ExecutionTimeout: timeout,
							Items: []types.BatchItem{{ID: "i", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))}}})
						if err != nil {
							t.Fatal(err)
						}
						status = func() types.WorkflowStatus { got, _ := cli.GetBatch(ctx, b.ID); return got.Status }
						cancel = func() error {
							if stop == "timeout" {
								return expireBatch(cli, b.ID)
							}
							return cli.CancelBatch(ctx, b.ID)
						}
						retry = func() error { _, err := cli.RetryBatch(ctx, b.ID, true); return err }
					} else {
						wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{Name: id, ExecutionTimeout: timeout, Tasks: []types.WorkflowTask{tallyTask(id)}}, nil)
						if err != nil {
							t.Fatal(err)
						}
						status = func() types.WorkflowStatus { got, _, _ := cli.Store().GetWorkflow(ctx, wf.ID); return got.Status }
						cancel = func() error {
							if stop == "timeout" {
								return expireWorkflow(cli, wf.ID)
							}
							return cli.CancelWorkflow(ctx, wf.ID)
						}
						retry = func() error { _, err := cli.RetryWorkflow(ctx, wf.ID); return err }
					}
					waitFor(t, 10*time.Second, "first execution", func() bool { return len(c.not(1, []string{id})) == 0 })
					if err := cancel(); err != nil {
						t.Fatal(err)
					}
					waitFor(t, 14*time.Second, "stop", func() bool { return status().IsTerminal() })
					if err := retry(); err != nil {
						t.Fatal(err)
					}
					waitFor(t, 10*time.Second, "second execution", func() bool { return len(c.not(2, []string{id})) == 0 })
					order := []int{0, 1}
					if first == "new" {
						order = []int{1, 0}
					}
					close(gate(id)[order[0]])
					time.Sleep(700 * time.Millisecond)
					close(gate(id)[order[1]])
					waitFor(t, 10*time.Second, "retry to complete", func() bool { return status() == types.WorkflowStatusCompleted })
				})
			}
		}
	}
}

// expireWorkflow moves the deadline of a workflow to the past.
func expireWorkflow(cli *client.Client, id string) error {
	ctx := context.Background()
	wf, _, err := cli.Store().GetWorkflow(ctx, id)
	if err != nil {
		return err
	}
	past := time.Now().Add(-time.Second)
	wf.Deadline = &past
	_, err = cli.Store().SaveWorkflow(ctx, wf)
	return err
}

// expireBatch moves the deadline of a batch to the past.
func expireBatch(cli *client.Client, id string) error {
	ctx := context.Background()
	batch, _, err := cli.Store().GetBatch(ctx, id)
	if err != nil {
		return err
	}
	past := time.Now().Add(-time.Second)
	batch.Deadline = &past
	_, err = cli.Store().SaveBatch(ctx, batch)
	return err
}
