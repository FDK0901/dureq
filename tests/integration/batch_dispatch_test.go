// Package integration — each batch item and workflow task is dispatched once.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/monitor"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/xid"
)

// tally counts how often each item or task was executed.
type tally struct {
	mu    sync.Mutex
	count map[string]int
}

func (c *tally) add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.count == nil {
		c.count = make(map[string]int)
	}
	c.count[key]++
}

// not returns what was not executed n times, of the keys.
func (c *tally) not(n int, keys []string) map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	off := make(map[string]int)
	for _, k := range keys {
		if c.count[k] != n {
			off[k] = c.count[k]
		}
	}
	return off
}

// startTally starts a server whose handler counts executions by the "id" of the payload.
func startTally(t *testing.T, taskType types.TaskType, c *tally, fail func(id string) error) *client.Client {
	t.Helper()
	flushDB(t)
	srv := newServer(t, "tally-"+xid.New().String(), 100)
	srv.RegisterHandler(types.HandlerDefinition{TaskType: taskType, Handler: func(_ context.Context, payload json.RawMessage) error {
		var p struct {
			ID string `json:"id"`
		}
		json.Unmarshal(payload, &p)
		c.add(p.ID)
		if fail != nil {
			return fail(p.ID)
		}
		return nil
	}})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { srv.Stop() })
	cli := newClient(t)
	t.Cleanup(cli.Close)
	time.Sleep(2 * time.Second) // until there is a leader
	return cli
}

func batchItems(n int) (items []types.BatchItem, ids []string) {
	for i := range n {
		id := fmt.Sprintf("i%03d", i)
		items = append(items, types.BatchItem{ID: id, Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))})
		ids = append(ids, id)
	}
	return items, ids
}

func waitBatch(t *testing.T, cli *client.Client, id string) *types.BatchInstance {
	t.Helper()
	var batch *types.BatchInstance
	waitFor(t, 30*time.Second, "batch to finish", func() bool {
		b, err := cli.GetBatch(context.Background(), id)
		batch = b
		return err == nil && b.Status.IsTerminal()
	})
	return batch
}

func TestBatch_EachItemRunsOnce(t *testing.T) {
	for _, tc := range []struct{ items, chunk int }{{5, 5}, {40, 7}, {60, 100}} {
		t.Run(fmt.Sprintf("%d items by %d", tc.items, tc.chunk), func(t *testing.T) {
			var c tally
			cli := startTally(t, "tally.item", &c, nil)
			items, ids := batchItems(tc.items)
			batch, err := cli.EnqueueBatch(context.Background(), types.BatchDefinition{
				Name: "once", ItemTaskType: "tally.item", Items: items, ChunkSize: tc.chunk,
			})
			if err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			done := waitBatch(t, cli, batch.ID)
			time.Sleep(time.Second) // for what is still queued
			if off := c.not(1, ids); len(off) > 0 {
				t.Errorf("%d of %d items did not run once: %v", len(off), tc.items, off)
			}
			if done.Status != types.WorkflowStatusCompleted || done.CompletedItems != tc.items || done.RunningItems != 0 || done.PendingItems != 0 {
				t.Errorf("batch %s: %d completed, %d running, %d pending", done.Status, done.CompletedItems, done.RunningItems, done.PendingItems)
			}
		})
	}
}

func TestWorkflow_EachRootTaskRunsOnce(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.task", &c, nil)
	for round := range 5 {
		var tasks []types.WorkflowTask
		var ids []string
		for i := range 10 {
			id := fmt.Sprintf("r%d-t%d", round, i)
			tasks = append(tasks, types.WorkflowTask{Name: id, TaskType: "tally.task", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))})
			ids = append(ids, id)
		}
		wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{Name: "once", Tasks: tasks}, nil)
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		waitFor(t, 30*time.Second, "workflow to finish", func() bool {
			got, _, err := cli.Store().GetWorkflow(context.Background(), wf.ID)
			return err == nil && got.Status.IsTerminal()
		})
		time.Sleep(time.Second)
		if off := c.not(1, ids); len(off) > 0 {
			t.Errorf("round %d: %d of %d tasks did not run once: %v", round, len(off), len(ids), off)
		}
	}
}

// The onetime task runs once, and before the items.
func TestBatch_OnetimeRunsOnceBeforeItems(t *testing.T) {
	var c tally
	var mu sync.Mutex
	var early []string
	cli := startTally(t, "tally.mixed", &c, func(id string) error {
		if id != "onetime" && len(c.not(1, []string{"onetime"})) > 0 {
			mu.Lock()
			early = append(early, id)
			mu.Unlock()
		}
		return nil
	})
	items, ids := batchItems(20)
	onetime := types.TaskType("tally.mixed")
	batch, err := cli.EnqueueBatch(context.Background(), types.BatchDefinition{
		Name: "onetime", ItemTaskType: "tally.mixed", Items: items, ChunkSize: 6,
		OnetimeTaskType: &onetime, OnetimePayload: json.RawMessage(`{"id":"onetime"}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	done := waitBatch(t, cli, batch.ID)
	time.Sleep(time.Second)
	if off := c.not(1, append(ids, "onetime")); len(off) > 0 {
		t.Errorf("did not run once: %v", off)
	}
	if done.Status != types.WorkflowStatusCompleted || len(early) > 0 {
		t.Errorf("batch %s, items before the onetime task: %v", done.Status, early)
	}
}

// Retrying a batch runs the failed items again, and only these.
func TestBatch_RetryRunsFailedItemsOnce(t *testing.T) {
	type retry func(cli *client.Client, api *monitor.APIService, id string) error
	for name, tc := range map[string]struct {
		policy *types.RetryPolicy
		retry  retry
	}{
		"client": {retry: func(cli *client.Client, _ *monitor.APIService, id string) error {
			_, err := cli.RetryBatch(context.Background(), id, true)
			return err
		}},
		"monitoring API": {retry: func(_ *client.Client, api *monitor.APIService, id string) error {
			return api.RetryBatch(context.Background(), id, true)
		}},
		"retry policy": {policy: &types.RetryPolicy{MaxAttempts: 2}},
	} {
		t.Run(name, func(t *testing.T) {
			var c tally
			failing := map[string]bool{"i003": true, "i011": true, "i019": true}
			cli := startTally(t, "tally.retry", &c, func(id string) error {
				if failing[id] && len(c.not(1, []string{id})) == 0 { // its first run
					return &types.NonRetryableError{Err: errBoom}
				}
				return nil
			})
			items, ids := batchItems(20)
			batch, err := cli.EnqueueBatch(context.Background(), types.BatchDefinition{
				Name: "retry", ItemTaskType: "tally.retry", Items: items, ChunkSize: 6,
				FailurePolicy: types.BatchContinueOnError, RetryPolicy: tc.policy,
			})
			if err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			if tc.retry != nil {
				first := waitBatch(t, cli, batch.ID)
				if first.Status != types.WorkflowStatusFailed || first.CompletedItems != 17 || first.FailedItems != 3 {
					t.Fatalf("batch %s: %d completed, %d failed; want 17 and 3", first.Status, first.CompletedItems, first.FailedItems)
				}
				api := monitor.NewAPIService(cli.Store(), dispatcher.New(cli.Store(), nil), nil)
				defer api.Shutdown()
				if err := tc.retry(cli, api, batch.ID); err != nil {
					t.Fatalf("retry: %v", err)
				}
			}
			waitFor(t, 30*time.Second, "retry to complete", func() bool {
				b, err := cli.GetBatch(context.Background(), batch.ID)
				return err == nil && b.Status == types.WorkflowStatusCompleted && b.CompletedItems == 20 && b.RunningItems == 0
			})
			time.Sleep(time.Second)
			var once, twice []string
			for _, id := range ids {
				if failing[id] {
					twice = append(twice, id)
				} else {
					once = append(once, id)
				}
			}
			if off := c.not(1, once); len(off) > 0 {
				t.Errorf("items that succeeded did not run once: %v", off)
			}
			if off := c.not(2, twice); len(off) > 0 {
				t.Errorf("items that failed did not run twice: %v", off)
			}
		})
	}
}

// Tasks of a workflow run once, also those behind several roots, and when it is retried.
func TestWorkflow_RetryRunsFailedTaskOnce(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wfretry", &c, func(id string) error {
		if id == "c" && len(c.not(1, []string{"c"})) == 0 { // its first run
			return &types.NonRetryableError{Err: errBoom}
		}
		return nil
	})
	task := func(name string, deps ...string) types.WorkflowTask {
		return types.WorkflowTask{Name: name, TaskType: "tally.wfretry", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, name)), DependsOn: deps}
	}
	ctx := context.Background()
	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{Name: "retry", Tasks: []types.WorkflowTask{task("a"), task("b"), task("c"), task("join", "a", "b", "c")}}, nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitStatus := func(want types.WorkflowStatus) {
		t.Helper()
		waitFor(t, 30*time.Second, fmt.Sprintf("workflow to be %s", want), func() bool {
			got, _, err := cli.Store().GetWorkflow(ctx, wf.ID)
			return err == nil && got.Status == want
		})
	}
	waitStatus(types.WorkflowStatusFailed)
	time.Sleep(time.Second) // for the events of the other roots
	if _, err := cli.RetryWorkflow(ctx, wf.ID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	waitFor(t, 30*time.Second, "retry of the failed task", func() bool { return len(c.not(2, []string{"c"})) == 0 })
	time.Sleep(time.Second)
	if off := c.not(1, []string{"a", "b"}); len(off) > 0 {
		t.Errorf("did not run once: %v", off)
	}
}
