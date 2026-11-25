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

// Nodes before v0.3.1 write to the event stream only. What their orchestrator had taken but
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

// The events of a workflow and its child workflows have one key, those of others have others.
func TestEventKeys(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	inner := types.WorkflowDefinition{Name: "w", Tasks: []types.WorkflowTask{{Name: "a", TaskType: "t"}}}
	child := func(parent *types.WorkflowInstance) *types.WorkflowInstance {
		t.Helper()
		wf := newWorkflow(t, st, inner)
		wf.ParentWorkflowID = &parent.ID
		if _, err := st.SaveWorkflow(ctx, wf); err != nil {
			t.Fatal(err)
		}
		return wf
	}
	top, other := newWorkflow(t, st, inner), newWorkflow(t, st, inner)
	mid := child(top)
	low := child(mid)
	lost := child(&types.WorkflowInstance{ID: "gone"})
	broken := newWorkflow(t, st, inner)
	underBroken := child(broken)
	if err := st.Client().Do(ctx, st.Client().B().Hset().Key(store.WorkflowKey(st.Prefix(), broken.ID)).FieldValue().FieldValue("data", "{").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	jobOf := func(wfID, batchID string) string {
		t.Helper()
		job := newJob(xid.New().String(), "t", nil, "op", time.Now())
		if wfID != "" {
			job.WorkflowID = &wfID
		}
		if batchID != "" {
			job.BatchID = &batchID
		}
		finishJob(t, st, job, types.JobStatusCompleted)
		return job.ID
	}

	for range 2 { // the second time, they are known
		o := newTestOrchestrator(st)
		for range 2 {
			var entries []rueidis.XRangeEntry
			var want []string
			add := func(eventType types.EventType, id, key string) {
				entries = append(entries, rueidis.XRangeEntry{FieldValues: map[string]string{"type": string(eventType), "job_id": id}})
				want = append(want, key)
			}
			add(types.EventJobCompleted, jobOf(low.ID, ""), top.ID)
			add(types.EventJobCompleted, jobOf(top.ID, ""), top.ID)
			add(types.EventWorkflowStarted, mid.ID, top.ID)
			add(types.EventWorkflowCompleted, low.ID, top.ID)
			add(types.EventWorkflowCompleted, top.ID, "")
			add(types.EventWorkflowRetrying, top.ID, top.ID)
			add(types.EventJobFailed, jobOf(other.ID, ""), other.ID)
			add(types.EventJobCompleted, jobOf("", "batch"), "batch")
			add(types.EventBatchRetrying, "batch", "batch")
			add(types.EventJobCompleted, jobOf("", ""), "")
			add(types.EventJobCompleted, "none", "")
			add(types.EventWorkflowFailed, lost.ID, "gone")
			add(types.EventJobCompleted, jobOf(underBroken.ID, ""), unknownKey)
			add(types.EventWorkflowResumed, broken.ID, unknownKey)
			for i, got := range o.eventKeys(ctx, entries) {
				if got != want[i] {
					t.Errorf("%s: key %q, want %q", entries[i].FieldValues["type"], got, want[i])
				}
			}
		}
	}
}

// A workflow whose ancestors are known already, as far as they are, has the same key as they.
func TestEventKeys_ChildOfKnown(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	chain := []*types.WorkflowInstance{newWorkflow(t, st, types.WorkflowDefinition{Name: "w"})}
	for range 4 {
		wf := newWorkflow(t, st, types.WorkflowDefinition{Name: "w"})
		wf.ParentWorkflowID = &chain[len(chain)-1].ID
		if _, err := st.SaveWorkflow(ctx, wf); err != nil {
			t.Fatal(err)
		}
		chain = append(chain, wf)
	}
	for known := range chain {
		for next := known + 1; next < len(chain); next++ {
			o := newTestOrchestrator(st)
			o.findRoots(ctx, []string{chain[known].ID})
			o.findRoots(ctx, []string{chain[next].ID})
			if got := o.roots[chain[next].ID]; got != chain[0].ID {
				t.Errorf("workflow %d after %d: root %q, want %q", next, known, got, chain[0].ID)
			}
		}
	}
}

// Workflows that are related to each other as a cycle, which cannot be, do not stop the loop.
func TestEventKeys_Cycle(t *testing.T) {
	st := newTestStore(t)
	a := newWorkflow(t, st, types.WorkflowDefinition{Name: "w"})
	b := newWorkflow(t, st, types.WorkflowDefinition{Name: "w"})
	a.ParentWorkflowID, b.ParentWorkflowID = &b.ID, &a.ID
	for _, wf := range []*types.WorkflowInstance{a, b} {
		if _, err := st.SaveWorkflow(context.Background(), wf); err != nil {
			t.Fatal(err)
		}
	}
	newTestOrchestrator(st).findRoots(context.Background(), []string{a.ID, b.ID})
}

// Events of one family of workflows do not overlap, and those of many workflows all count.
func TestEvents_OfManyWorkflows(t *testing.T) {
	st := newTestStore(t)
	const n = 200
	var wfs []*types.WorkflowInstance
	for range n {
		wf, job := runningWorkflow(t, st)
		publishEvents(t, st, 2, completed(job)) // twice, as it may come
		wfs = append(wfs, wf)
	}
	startOrchestrator(t, st)
	for _, wf := range wfs {
		waitTask(t, st, wf.ID, "b", types.JobStatusRunning)
	}
	count := dispatched(t, st)
	for _, wf := range wfs {
		if jobID := getWorkflow(t, st, wf.ID).Tasks["b"].JobID; count[jobID] != 1 {
			t.Errorf("task b was dispatched %d times", count[jobID])
		}
	}
	if len(count) != n {
		t.Errorf("%d jobs were dispatched, want %d", len(count), n)
	}
}
