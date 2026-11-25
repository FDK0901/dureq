package client

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// breakDispatch makes it impossible to dispatch anything.
func breakDispatch(t *testing.T, cli *Client) {
	t.Helper()
	for _, tier := range cli.Store().Config().Tiers {
		key := store.WorkStreamKey(cli.Store().Prefix(), tier.Name)
		if err := cli.rdb.Do(context.Background(), cli.rdb.B().Set().Key(key).Value("x").Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
}

func wantNoJobs(t *testing.T, cli *Client) {
	t.Helper()
	if jobs, err := cli.Store().ListJobs(context.Background()); err != nil || len(jobs) != 0 {
		t.Fatalf("%d jobs (%v), want none", len(jobs), err)
	}
}

func TestEnqueueBatch_NothingStarted_IsAnError(t *testing.T) {
	onetime := types.TaskType("prepare")
	for name, def := range map[string]types.BatchDefinition{
		"items":   {ItemTaskType: "t", Items: []types.BatchItem{{ID: "a"}, {ID: "b"}}},
		"onetime": {ItemTaskType: "t", OnetimeTaskType: &onetime, Items: []types.BatchItem{{ID: "a"}}},
	} {
		t.Run(name, func(t *testing.T) {
			cli := newTestClient(t)
			ctx := context.Background()
			breakDispatch(t, cli)
			if batch, err := cli.EnqueueBatch(ctx, def); err == nil {
				t.Fatalf("no error, batch %+v", batch)
			}
			batches, _, err := cli.Store().ListBatchInstances(ctx, store.BatchFilter{})
			if err != nil || len(batches) != 1 || batches[0].Status != types.WorkflowStatusFailed || batches[0].RunningItems != 0 {
				t.Fatalf("batches %+v (%v), want one that failed", batches, err)
			}
			wantNoJobs(t, cli)
		})
	}
}

func TestEnqueueWorkflow_NothingStarted_IsAnError(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	breakDispatch(t, cli)
	_, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{Name: "w", Tasks: []types.WorkflowTask{
		{Name: "a", TaskType: "t", Payload: json.RawMessage(`{}`)},
		{Name: "b", TaskType: "t", Payload: json.RawMessage(`{}`)},
		{Name: "after", TaskType: "t", Payload: json.RawMessage(`{}`), DependsOn: []string{"a", "b"}},
	}}, nil)
	if err == nil {
		t.Fatal("no error")
	}
	wfs, _, err := cli.Store().ListWorkflowInstances(ctx, store.WorkflowFilter{})
	if err != nil || len(wfs) != 1 || wfs[0].Status != types.WorkflowStatusFailed {
		t.Fatalf("workflows %+v (%v), want one that failed", wfs, err)
	}
	for name, want := range map[string]types.JobStatus{"a": types.JobStatusFailed, "b": types.JobStatusFailed, "after": types.JobStatusPending} {
		if got := wfs[0].Tasks[name].Status; got != want {
			t.Errorf("task %s is %s, want %s", name, got, want)
		}
	}
	wantNoJobs(t, cli)
}

// The instance that is stored, and the one that is returned, know what is running.
func TestEnqueue_ReservesWhatItStarts(t *testing.T) {
	cli := newTestClient(t)
	ctx := context.Background()
	batch, err := cli.EnqueueBatch(ctx, types.BatchDefinition{ItemTaskType: "t", ChunkSize: 2, Items: []types.BatchItem{{ID: "a"}, {ID: "b"}, {ID: "c"}}})
	if err != nil {
		t.Fatal(err)
	}
	stored, rev, err := cli.Store().GetBatch(ctx, batch.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []*types.BatchInstance{batch, stored} {
		if b.RunningItems != 2 || b.PendingItems != 1 || b.ItemStates["c"].Status != types.JobStatusPending {
			t.Errorf("batch %+v", b)
		}
		for _, id := range []string{"a", "b"} {
			if job, _, err := cli.Store().GetJob(ctx, b.ItemStates[id].JobID); err != nil || *job.BatchItem != id {
				t.Errorf("job of item %s: %+v (%v)", id, job, err)
			}
		}
	}
	if rev != 1 {
		t.Errorf("batch was stored %d times, want once", rev)
	}

	wf, err := cli.EnqueueWorkflow(ctx, types.WorkflowDefinition{Name: "w", Tasks: []types.WorkflowTask{{Name: "a", TaskType: "t"}, {Name: "b", TaskType: "t", DependsOn: []string{"a"}}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	storedWf, rev, err := cli.Store().GetWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []*types.WorkflowInstance{wf, storedWf} {
		if w.Tasks["a"].Status != types.JobStatusRunning || w.Tasks["b"].Status != types.JobStatusPending {
			t.Errorf("tasks %+v", w.Tasks)
		}
		if job, _, err := cli.Store().GetJob(ctx, w.Tasks["a"].JobID); err != nil || *job.WorkflowTask != "a" {
			t.Errorf("job of task a: %+v (%v)", job, err)
		}
	}
	if rev != 1 {
		t.Errorf("workflow was stored %d times, want once", rev)
	}
}
