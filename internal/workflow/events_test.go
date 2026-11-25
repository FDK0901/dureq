package workflow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// runningWorkflow stores a workflow a -> b, whose task a is done but not yet counted.
func runningWorkflow(t *testing.T, st *store.RedisStore) (*types.WorkflowInstance, *types.Job) {
	t.Helper()
	wf := newWorkflow(t, st, types.WorkflowDefinition{Name: "w", Tasks: []types.WorkflowTask{
		{Name: "a", TaskType: "t"}, {Name: "b", TaskType: "t", DependsOn: []string{"a"}},
	}})
	job := newJob(xid.New().String(), "t", nil, "op", time.Now())
	job.WorkflowID, job.WorkflowTask = &wf.ID, strPtr("a")
	finishJob(t, st, job, types.JobStatusCompleted)
	wf.Tasks["a"] = types.WorkflowTaskState{Name: "a", JobID: job.ID, Status: types.JobStatusRunning}
	if _, err := st.SaveWorkflow(context.Background(), wf); err != nil {
		t.Fatal(err)
	}
	return wf, job
}

func publishEvents(t *testing.T, st *store.RedisStore, n int, event types.JobEvent) {
	t.Helper()
	for range n {
		if err := st.PublishEvent(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
}

// unprocessed returns how many events the orchestrator has yet to process.
func unprocessed(t *testing.T, st *store.RedisStore) int64 {
	t.Helper()
	n, err := st.Client().Do(context.Background(), st.Client().B().Xlen().Key(store.OrchestratorStreamKey(st.Prefix())).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// startOrchestrator runs one until the test ends.
func startOrchestrator(t *testing.T, st *store.RedisStore) {
	t.Helper()
	o := newTestOrchestrator(st)
	o.Start(context.Background())
	t.Cleanup(o.Stop)
}

func waitTask(t *testing.T, st *store.RedisStore, wfID, task string, want types.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := getWorkflow(t, st, wfID).Tasks[task].Status
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s is %q, want %q", task, got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The event stream is limited in length. What the orchestrator has yet to see is not.
func TestEvents_AreKeptUntilProcessed(t *testing.T) {
	st := newTestStoreWith(t, 100)
	wf, job := runningWorkflow(t, st)
	publishEvents(t, st, 1, completed(job))
	publishEvents(t, st, 1000, types.JobEvent{Type: types.EventJobStarted, JobID: "plain"})
	publishEvents(t, st, 300, types.JobEvent{Type: types.EventJobCompleted, JobID: "plain", Standalone: true})
	publishEvents(t, st, 300, types.JobEvent{Type: types.EventJobCompleted, JobID: "unknown"})

	kept, err := st.Client().Do(context.Background(), st.Client().B().Xlen().Key(store.EventsStreamKey(st.Prefix())).Build()).AsInt64()
	if err != nil || kept > 300 {
		t.Fatalf("the event stream has %d events (%v), want about 100", kept, err)
	}
	if n := unprocessed(t, st); n != 301 {
		t.Errorf("%d events are for the orchestrator, want 301", n)
	}

	startOrchestrator(t, st)
	waitTask(t, st, wf.ID, "b", types.JobStatusRunning)
	deadline := time.Now().Add(5 * time.Second)
	for unprocessed(t, st) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d events are left", unprocessed(t, st))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Nodes before v0.2.6 write to the event stream only. What their orchestrator had taken but
// not finished counts, too.
func TestEvents_OfOlderNodes(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	rdb := st.Client()
	key := store.EventsStreamKey(st.Prefix())
	add := func(event types.JobEvent) {
		t.Helper()
		data, _ := json.Marshal(event)
		cmd := rdb.B().Xadd().Key(key).Id("*").FieldValue().FieldValue("type", string(event.Type)).FieldValue("job_id", event.JobID).FieldValue("data", string(data)).Build()
		if err := rdb.Do(ctx, cmd).Error(); err != nil {
			t.Fatal(err)
		}
	}
	taken, takenJob := runningWorkflow(t, st)
	add(completed(takenJob))
	if err := rdb.Do(ctx, rdb.B().Xreadgroup().Group(store.OrchestratorConsumerGroup, consumerName).Streams().Key(key).Id(">").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	unread, unreadJob := runningWorkflow(t, st)
	add(completed(unreadJob))

	startOrchestrator(t, st)
	waitTask(t, st, taken.ID, "b", types.JobStatusRunning)
	waitTask(t, st, unread.ID, "b", types.JobStatusRunning)
	later, laterJob := runningWorkflow(t, st)
	add(completed(laterJob))
	waitTask(t, st, later.ID, "b", types.JobStatusRunning)
	if got := dispatched(t, st); len(got) != 3 {
		t.Errorf("dispatched %v, want b of the three workflows", got)
	}
}

// Events that newer nodes have given the orchestrator are not given to it again.
func TestEvents_AreNotForwardedTwice(t *testing.T) {
	st := newTestStore(t)
	_, job := runningWorkflow(t, st)
	publishEvents(t, st, 1, completed(job))
	for range 3 {
		if _, err := st.ForwardEvents(context.Background(), consumerName, 100); err != nil {
			t.Fatal(err)
		}
	}
	if n := unprocessed(t, st); n != 1 {
		t.Errorf("%d events for the orchestrator, want 1", n)
	}
}

func readEvents(t *testing.T, st *store.RedisStore) []rueidis.XRangeEntry {
	t.Helper()
	key := store.OrchestratorStreamKey(st.Prefix())
	streams, err := st.Client().Do(context.Background(), st.Client().B().Xreadgroup().Group(store.OrchestratorConsumerGroup, consumerName).Streams().Key(key).Id(">").Build()).AsXRead()
	if err != nil {
		t.Fatal(err)
	}
	return streams[key]
}

// An event that cannot be processed is kept and tried again, and does not keep others waiting.
func TestEvents_FailedIsTriedAgain(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	rdb := st.Client()
	o := newTestOrchestrator(st)
	broken, brokenJob := runningWorkflow(t, st)
	publishEvents(t, st, 1, completed(brokenJob))
	fine, fineJob := runningWorkflow(t, st)
	publishEvents(t, st, 1, completed(fineJob))

	key := store.WorkflowKey(st.Prefix(), broken.ID)
	data, err := rdb.Do(ctx, rdb.B().Hget().Key(key).Field("data").Build()).ToString()
	if err != nil {
		t.Fatal(err)
	}
	set := func(value string) {
		t.Helper()
		if err := rdb.Do(ctx, rdb.B().Hset().Key(key).FieldValue().FieldValue("data", value).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	set("{")
	if o.processEvents(ctx, readEvents(t, st)) {
		t.Error("the failure is not reported")
	}
	if got := getWorkflow(t, st, fine.ID).Tasks["b"].Status; got != types.JobStatusRunning {
		t.Errorf("task b of the other workflow is %q", got)
	}
	streamKey := store.OrchestratorStreamKey(st.Prefix())
	if !o.drainPending(ctx, streamKey) || unprocessed(t, st) != 1 {
		t.Errorf("%d events are left, want the failed one", unprocessed(t, st))
	}

	set(data)
	if o.drainPending(ctx, streamKey) || unprocessed(t, st) != 0 {
		t.Errorf("%d events are left, want none", unprocessed(t, st))
	}
	if got := getWorkflow(t, st, broken.ID).Tasks["b"].Status; got != types.JobStatusRunning {
		t.Errorf("task b is %q", got)
	}
}

// There is nothing to do for what has been deleted.
func TestEvents_OfDeleted(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	wf, job := runningWorkflow(t, st)
	if err := st.Client().Do(ctx, st.Client().B().Del().Key(store.WorkflowKey(st.Prefix(), wf.ID)).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	publishEvents(t, st, 1, completed(job))
	publishEvents(t, st, 1, types.JobEvent{Type: types.EventJobFailed, JobID: "none"})
	publishEvents(t, st, 1, types.JobEvent{Type: types.EventWorkflowCompleted, JobID: "none"})
	publishEvents(t, st, 1, types.JobEvent{Type: types.EventWorkflowRetrying, JobID: "none"})
	publishEvents(t, st, 1, types.JobEvent{Type: types.EventBatchRetrying, JobID: "none"})
	if !newTestOrchestrator(st).processEvents(ctx, readEvents(t, st)) {
		t.Error("an event failed")
	}
	if n := unprocessed(t, st); n != 0 {
		t.Errorf("%d events are left", n)
	}
}
