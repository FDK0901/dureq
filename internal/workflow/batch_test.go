package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// newTestStore connects to the standalone Redis at REDIS_URL under a unique key prefix.
// Skips if REDIS_URL is unset or Redis is unreachable.
func newTestStore(t *testing.T) *store.RedisStore {
	t.Helper()
	addr := strings.TrimPrefix(os.Getenv("REDIS_URL"), "redis://")
	if addr == "" {
		t.Skip("REDIS_URL not set")
	}
	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		password = "your-password"
	}
	rdb, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, Password: password})
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(rdb.Close)
	st, err := store.NewRedisStore(rdb, store.RedisStoreConfig{
		KeyPrefix: "test_" + xid.New().String(),
		Tiers:     []store.TierConfig{{Name: "normal", Weight: 1, FetchBatch: 10}},
	}, nil)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	if err := st.EnsureStreams(context.Background()); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}
	return st
}

func newTestOrchestrator(st *store.RedisStore) *Orchestrator {
	return NewOrchestrator(OrchestratorConfig{Store: st, Dispatcher: dispatcher.New(st, nil)})
}

// reservedBatch stores a batch with its first chunk reserved, and returns the jobs of it.
func reservedBatch(t *testing.T, st *store.RedisStore, items, chunk int) (*types.BatchInstance, []*types.Job) {
	t.Helper()
	now := time.Now()
	batch := &types.BatchInstance{
		ID: xid.New().String(), Status: types.WorkflowStatusRunning, TotalItems: items, PendingItems: items, Attempt: 1,
		ItemStates: make(map[string]types.BatchItemState), CreatedAt: now, UpdatedAt: now,
		Definition: types.BatchDefinition{ItemTaskType: "t", ChunkSize: chunk, FailurePolicy: types.BatchContinueOnError},
	}
	for i := range items {
		id := fmt.Sprintf("i%02d", i)
		batch.Definition.Items = append(batch.Definition.Items, types.BatchItem{ID: id, Payload: json.RawMessage(`{}`)})
		batch.ItemStates[id] = types.BatchItemState{ItemID: id, Status: types.JobStatusPending}
	}
	jobs := ReserveBatch(batch, now)
	if _, err := st.SaveBatch(context.Background(), batch); err != nil {
		t.Fatalf("save batch: %v", err)
	}
	return batch, jobs
}

// finishJob stores the job as it is once a worker is done with it.
func finishJob(t *testing.T, st *store.RedisStore, job *types.Job, status types.JobStatus) {
	t.Helper()
	job.Status = status
	if _, err := st.SaveJob(context.Background(), job); err != nil {
		t.Fatalf("save job: %v", err)
	}
}

func getBatch(t *testing.T, st *store.RedisStore, id string) *types.BatchInstance {
	t.Helper()
	batch, _, err := st.GetBatch(context.Background(), id)
	if err != nil {
		t.Fatalf("get batch: %v", err)
	}
	return batch
}

// dispatched returns how often each job is on the work stream.
func dispatched(t *testing.T, st *store.RedisStore) map[string]int {
	t.Helper()
	rdb := st.Client()
	entries, err := rdb.Do(context.Background(), rdb.B().Xrange().Key(store.WorkStreamKey(st.Prefix(), "normal")).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatalf("XRANGE: %v", err)
	}
	count := make(map[string]int)
	for _, e := range entries {
		count[e.FieldValues["job_id"]]++
	}
	return count
}

func completed(job *types.Job) types.JobEvent {
	return types.JobEvent{Type: types.EventJobCompleted, JobID: job.ID, Timestamp: time.Now()}
}

func TestReserveBatchChunk(t *testing.T) {
	batch := &types.BatchInstance{
		ID: "b", PendingItems: 5, TotalItems: 5,
		Definition: types.BatchDefinition{ItemTaskType: "t", ChunkSize: 2},
		ItemStates: map[string]types.BatchItemState{},
	}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		batch.Definition.Items = append(batch.Definition.Items, types.BatchItem{ID: id})
		batch.ItemStates[id] = types.BatchItemState{ItemID: id, Status: types.JobStatusPending}
	}
	batch.ItemStates["b"] = types.BatchItemState{ItemID: "b", Status: types.JobStatusCompleted}

	items := func(jobs []*types.Job) string {
		var ids []string
		for _, job := range jobs {
			ids = append(ids, *job.BatchItem)
			if st := batch.ItemStates[*job.BatchItem]; st.Status != types.JobStatusRunning || st.JobID != job.ID || *job.BatchID != "b" {
				t.Errorf("item %s: %+v does not go with job %s", *job.BatchItem, st, job.ID)
			}
		}
		return strings.Join(ids, " ")
	}
	if got := items(ReserveBatchChunk(batch, time.Now())); got != "a c" {
		t.Fatalf("first chunk %q, want a c", got)
	}
	if got := ReserveBatchChunk(batch, time.Now()); len(got) != 0 {
		t.Fatalf("reserved %d more items while a chunk is running", len(got))
	}
	batch.RunningItems--
	if got := items(ReserveBatchChunk(batch, time.Now())); got != "d" {
		t.Fatalf("next %q, want d", got)
	}
	if batch.RunningItems != 2 || batch.PendingItems != 2 {
		t.Fatalf("%d running, %d pending", batch.RunningItems, batch.PendingItems)
	}
}

// Nothing is lost when the batch is changed by others at the same time, like by a leader
// that has not noticed yet that it is one no more.
func TestBatch_ConcurrentChangesAreNotLost(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	const items = 40
	batch, jobs := reservedBatch(t, st, items, items)
	for i, job := range jobs {
		status := types.JobStatusCompleted
		if i%4 == 0 {
			status = types.JobStatusDead
		}
		finishJob(t, st, job, status)
	}

	var others, leaders sync.WaitGroup
	stop := make(chan struct{})
	others.Add(1)
	go func() {
		defer others.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(3 * time.Millisecond):
				if b, rev, err := st.GetBatch(ctx, batch.ID); err == nil && !b.Status.IsTerminal() {
					st.UpdateBatch(ctx, b, rev)
				}
			}
		}
	}()
	for n := range 2 {
		leaders.Add(1)
		go func() {
			defer leaders.Done()
			o := newTestOrchestrator(st)
			for i := n; i < items; i += 2 {
				event := completed(jobs[i])
				handle := o.handleJobCompleted
				if jobs[i].Status == types.JobStatusDead {
					event.Type, handle = types.EventJobFailed, o.handleJobFailed
				}
				if err := handle(ctx, event); err != nil {
					t.Errorf("item %d: %v", i, err)
				}
			}
		}()
	}
	leaders.Wait()
	close(stop)
	others.Wait()

	got := getBatch(t, st, batch.ID)
	if got.Status != types.WorkflowStatusFailed || got.CompletedItems != 30 || got.FailedItems != 10 || got.RunningItems != 0 {
		t.Fatalf("batch %s: %d completed, %d failed, %d running; want 30, 10 and 0", got.Status, got.CompletedItems, got.FailedItems, got.RunningItems)
	}
	if results, err := st.ListBatchItemResults(ctx, batch.ID); err != nil || len(results) != items {
		t.Fatalf("%d results (%v), want %d", len(results), err, items)
	}
	if n := len(dispatched(t, st)); n != 0 {
		t.Fatalf("%d jobs dispatched again", n)
	}
}

// A chunk goes out once, also if both of two leaders see the event that makes room for it.
func TestBatch_NextChunkIsStartedOnce(t *testing.T) {
	st := newTestStore(t)
	batch, jobs := reservedBatch(t, st, 30, 10)
	for _, job := range jobs {
		finishJob(t, st, job, types.JobStatusCompleted)
	}
	var leaders sync.WaitGroup
	for range 2 {
		leaders.Add(1)
		go func() {
			defer leaders.Done()
			o := newTestOrchestrator(st)
			for _, job := range jobs {
				if err := o.handleJobCompleted(context.Background(), completed(job)); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	leaders.Wait()

	got := getBatch(t, st, batch.ID)
	if got.CompletedItems != 10 || got.RunningItems != 10 || got.PendingItems != 10 {
		t.Fatalf("%d completed, %d running, %d pending; want 10 each", got.CompletedItems, got.RunningItems, got.PendingItems)
	}
	count := dispatched(t, st)
	for _, item := range batch.Definition.Items[10:20] {
		if n := count[got.ItemStates[item.ID].JobID]; n != 1 {
			t.Errorf("item %s dispatched %d times", item.ID, n)
		}
	}
	if len(count) != 10 {
		t.Errorf("%d jobs dispatched, want 10", len(count))
	}
}

// What was reserved but not started, because whoever did stopped in between, is started at
// the first event of the batch, under the job IDs that are reserved and only once.
func TestBatch_ReservedButNotStarted_IsStarted(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	batch, jobs := reservedBatch(t, st, 8, 5)
	finishJob(t, st, jobs[0], types.JobStatusCompleted)
	if _, err := st.CreateJob(ctx, jobs[1]); err != nil { // by the client, which dispatches it
		t.Fatal(err)
	}

	for range 2 { // again by a new leader
		o := newTestOrchestrator(st)
		for range 2 { // and again
			if err := o.handleJobCompleted(ctx, completed(jobs[0])); err != nil {
				t.Fatal(err)
			}
		}
	}

	count := dispatched(t, st)
	for i, want := range []int{0, 0, 1, 1, 1} {
		if n := count[jobs[i].ID]; n != want {
			t.Errorf("job %d dispatched %d times, want %d", i, n, want)
		}
	}
	got := getBatch(t, st, batch.ID)
	if next := got.ItemStates["i05"]; next.Status != types.JobStatusRunning || count[next.JobID] != 1 {
		t.Errorf("next item %+v, dispatched %d times", next, count[next.JobID])
	}
	if len(count) != 4 || got.CompletedItems != 1 || got.RunningItems != 5 {
		t.Errorf("%d jobs dispatched, %d items completed, %d running", len(count), got.CompletedItems, got.RunningItems)
	}
}

func TestBatch_OnetimeReservedButNotStarted_IsStartedByRetryEvent(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	onetime := types.TaskType("prepare")
	now := time.Now()
	batch := &types.BatchInstance{
		ID: xid.New().String(), Status: types.WorkflowStatusRunning, TotalItems: 1, PendingItems: 1, CreatedAt: now,
		Definition: types.BatchDefinition{OnetimeTaskType: &onetime, ItemTaskType: "t", Items: []types.BatchItem{{ID: "i"}}},
		ItemStates: map[string]types.BatchItemState{"i": {ItemID: "i", Status: types.JobStatusPending}},
	}
	jobs := ReserveBatch(batch, now)
	if len(jobs) != 1 || *jobs[0].BatchRole != "onetime" || jobs[0].TaskType != onetime {
		t.Fatalf("reserved %+v, want the onetime task", jobs)
	}
	if _, err := st.SaveBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		event := types.JobEvent{Type: types.EventBatchRetrying, JobID: batch.ID}
		if err := newTestOrchestrator(st).handleBatchRetry(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	if count := dispatched(t, st); len(count) != 1 || count[jobs[0].ID] != 1 {
		t.Fatalf("dispatched %v, want the onetime job once", count)
	}

	finishJob(t, st, jobs[0], types.JobStatusCompleted)
	if err := newTestOrchestrator(st).handleJobCompleted(ctx, completed(jobs[0])); err != nil {
		t.Fatal(err)
	}
	got := getBatch(t, st, batch.ID)
	if got.OnetimeState.Status != types.JobStatusCompleted || got.ItemStates["i"].Status != types.JobStatusRunning {
		t.Fatalf("after the onetime task: %+v, item %+v", got.OnetimeState, got.ItemStates["i"])
	}
}

// An event whose job could not be started is not done with.
func TestBatch_JobNotStarted_IsStartedByNextEvent(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	o := newTestOrchestrator(st)
	batch, jobs := reservedBatch(t, st, 4, 2)
	for _, job := range jobs {
		finishJob(t, st, job, types.JobStatusCompleted)
	}

	rdb, stream := st.Client(), store.WorkStreamKey(st.Prefix(), "normal")
	rdb.Do(ctx, rdb.B().Rename().Key(stream).Newkey(stream+":away").Build())
	rdb.Do(ctx, rdb.B().Set().Key(stream).Value("x").Build())
	if err := o.handleJobCompleted(ctx, completed(jobs[0])); err == nil {
		t.Fatal("no error although nothing can be dispatched")
	}
	rdb.Do(ctx, rdb.B().Rename().Key(stream+":away").Newkey(stream).Build())

	if err := o.handleJobCompleted(ctx, completed(jobs[1])); err != nil {
		t.Fatal(err)
	}
	got, count := getBatch(t, st, batch.ID), dispatched(t, st)
	for _, id := range []string{"i02", "i03"} {
		if n := count[got.ItemStates[id].JobID]; n != 1 {
			t.Errorf("item %s dispatched %d times", id, n)
		}
	}
}

func TestWorkflow_ReservedButNotStarted_IsStarted(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	wf := &types.WorkflowInstance{ID: xid.New().String(), Status: types.WorkflowStatusRunning, Tasks: map[string]types.WorkflowTaskState{}, CreatedAt: now}
	for _, name := range []string{"a", "b", "c", "after"} {
		task := types.WorkflowTask{Name: name, TaskType: "t", Payload: json.RawMessage(`{}`)}
		if name == "after" {
			task.DependsOn = []string{"a", "b", "c"}
		}
		wf.Definition.Tasks = append(wf.Definition.Tasks, task)
		wf.Tasks[name] = types.WorkflowTaskState{Name: name, Status: types.JobStatusPending}
	}
	InitPendingDeps(wf)
	jobs := ReserveRootTasks(wf, now)
	if _, err := st.SaveWorkflow(ctx, wf); err != nil {
		t.Fatal(err)
	}
	finishJob(t, st, jobs[0], types.JobStatusCompleted)
	if _, err := st.CreateJob(ctx, jobs[1]); err != nil { // by the client, which dispatches it
		t.Fatal(err)
	}
	for range 2 {
		if err := newTestOrchestrator(st).handleJobCompleted(ctx, completed(jobs[0])); err != nil {
			t.Fatal(err)
		}
	}
	if count := dispatched(t, st); len(count) != 1 || count[jobs[2].ID] != 1 {
		t.Fatalf("dispatched %v, want c (%s) once", count, jobs[2].ID)
	}
}

func TestBatch_UnknownItem_IsIgnored(t *testing.T) {
	st := newTestStore(t)
	batch, jobs := reservedBatch(t, st, 2, 2)
	for _, job := range jobs {
		finishJob(t, st, job, types.JobStatusPending)
	}
	none := "none"
	stray := newJob(xid.New().String(), "t", nil, time.Now())
	stray.BatchID, stray.BatchItem, stray.BatchRole = &batch.ID, &none, strPtr("item")
	finishJob(t, st, stray, types.JobStatusCompleted)
	if err := newTestOrchestrator(st).handleJobCompleted(context.Background(), completed(stray)); err != nil && !errors.Is(err, types.ErrJobNotFound) {
		t.Fatal(err)
	}
	if got := getBatch(t, st, batch.ID); got.CompletedItems != 0 || got.RunningItems != 2 {
		t.Fatalf("%d completed, %d running", got.CompletedItems, got.RunningItems)
	}
}

// newWorkflow stores a workflow with nothing started.
func newWorkflow(t *testing.T, st *store.RedisStore, def types.WorkflowDefinition) *types.WorkflowInstance {
	t.Helper()
	wf := &types.WorkflowInstance{ID: xid.New().String(), WorkflowName: def.Name, Definition: def, Status: types.WorkflowStatusRunning, Attempt: 1, Tasks: map[string]types.WorkflowTaskState{}, CreatedAt: time.Now()}
	for _, task := range def.Tasks {
		wf.Tasks[task.Name] = types.WorkflowTaskState{Name: task.Name, Status: types.JobStatusPending}
	}
	InitPendingDeps(wf)
	if _, err := st.SaveWorkflow(context.Background(), wf); err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	return wf
}

func getWorkflow(t *testing.T, st *store.RedisStore, id string) *types.WorkflowInstance {
	t.Helper()
	wf, _, err := st.GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	return wf
}

// A child workflow at the root is started once, however often the event comes. Root tasks
// that are jobs are left to the client.
func TestWorkflowStarted_StartsChildWorkflowsOnce(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	inner := &types.WorkflowDefinition{Name: "inner", Tasks: []types.WorkflowTask{{Name: "x", TaskType: "t"}, {Name: "y", TaskType: "t", DependsOn: []string{"x"}}}}
	wf := newWorkflow(t, st, types.WorkflowDefinition{Name: "outer", Tasks: []types.WorkflowTask{
		{Name: "child", ChildWorkflowDef: inner}, {Name: "plain", TaskType: "t"}, {Name: "after", ChildWorkflowDef: inner, DependsOn: []string{"child"}},
	}})
	for range 2 {
		event := types.JobEvent{Type: types.EventWorkflowStarted, JobID: wf.ID}
		if err := newTestOrchestrator(st).handleWorkflowStarted(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	got := getWorkflow(t, st, wf.ID)
	if got.Tasks["plain"].Status != types.JobStatusPending || got.Tasks["after"].Status != types.JobStatusPending {
		t.Errorf("tasks %+v", got.Tasks)
	}
	all, err := st.ListWorkflows(ctx)
	if err != nil || len(all) != 2 {
		t.Fatalf("%d workflows (%v), want the two", len(all), err)
	}
	child := getWorkflow(t, st, got.Tasks["child"].ChildWorkflowID)
	if count := dispatched(t, st); len(count) != 1 || count[child.Tasks["x"].JobID] != 1 {
		t.Errorf("dispatched %v, want x of the child once", count)
	}
	// It is gone by the time the event comes.
	if err := newTestOrchestrator(st).handleWorkflowStarted(ctx, types.JobEvent{JobID: "none"}); err != nil {
		t.Errorf("event of a workflow that is gone: %v", err)
	}
}

// Hooks that were reserved at the end of a workflow but not started are started.
func TestWorkflow_HooksReservedButNotStarted_AreStarted(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	hook := &types.WorkflowHookDef{TaskType: "hook", Payload: json.RawMessage(`{}`)}
	wf := newWorkflow(t, st, types.WorkflowDefinition{Name: "w", Tasks: []types.WorkflowTask{{Name: "a", TaskType: "t"}}, Hooks: &types.WorkflowHooks{OnSuccess: hook, OnExit: hook}})
	task := ReserveRootTasks(wf, time.Now())[0]
	wf.Status = types.WorkflowStatusCompleted
	wf.Tasks["a"] = types.WorkflowTaskState{Name: "a", JobID: task.ID, Status: types.JobStatusCompleted}
	hooks := reserveHooks(wf, time.Now(), hookInit, hookSuccess, hookExit)
	if len(hooks) != 2 {
		t.Fatalf("reserved %d hooks, want the 2 there are", len(hooks))
	}
	if _, err := st.SaveWorkflow(ctx, wf); err != nil {
		t.Fatal(err)
	}
	finishJob(t, st, task, types.JobStatusCompleted)
	for range 2 {
		if err := newTestOrchestrator(st).handleJobCompleted(ctx, completed(task)); err != nil {
			t.Fatal(err)
		}
	}
	if count := dispatched(t, st); len(count) != 2 || count[hooks[0].ID] != 1 || count[hooks[1].ID] != 1 {
		t.Fatalf("dispatched %v, want each hook once", count)
	}
}
