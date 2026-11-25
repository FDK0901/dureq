package workflow

import (
	"fmt"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/xid"
)

// The functions in this file reserve work: they mark it as running in a workflow or batch
// instance, under the ID of a job that they return and that does not exist yet. The caller
// stores the instance and only then creates and dispatches the jobs. A job that is started
// first may finish while the stored instance does not know of it, so that the work is taken
// for pending and started a second time.

// NeedsBatchOnetime reports whether the batch has a onetime task that has not completed yet.
func NeedsBatchOnetime(batch *types.BatchInstance) bool {
	return batch.Definition.OnetimeTaskType != nil && *batch.Definition.OnetimeTaskType != "" &&
		(batch.OnetimeState == nil || batch.OnetimeState.Status != types.JobStatusCompleted)
}

// ReserveBatch reserves what a batch starts with: its onetime task or else a chunk of items.
func ReserveBatch(batch *types.BatchInstance, now time.Time) []*types.Job {
	if !NeedsBatchOnetime(batch) {
		return ReserveBatchChunk(batch, now)
	}
	batch.OnetimeState = &types.BatchOnetimeState{
		JobID:     xid.New().String(),
		Status:    types.JobStatusRunning,
		StartedAt: &now,
	}
	return []*types.Job{batchOnetimeJob(batch, now)}
}

// ReserveBatchChunk reserves the next items of a batch, as many as may run.
func ReserveBatchChunk(batch *types.BatchInstance, now time.Time) []*types.Job {
	chunkSize := batch.Definition.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 100
	}

	var jobs []*types.Job
	items := batch.Definition.Items
	for batch.NextChunkIndex < len(items) && len(jobs) < chunkSize && batch.RunningItems < chunkSize {
		item := &items[batch.NextChunkIndex]
		batch.NextChunkIndex++

		// Skip already processed items (e.g. on retry).
		if state, ok := batch.ItemStates[item.ID]; ok && state.Status != types.JobStatusPending {
			continue
		}

		batch.ItemStates[item.ID] = types.BatchItemState{
			ItemID:    item.ID,
			JobID:     xid.New().String(),
			Status:    types.JobStatusRunning,
			StartedAt: &now,
			Iteration: batch.ItemStates[item.ID].Iteration,
		}
		batch.RunningItems++
		batch.PendingItems--
		jobs = append(jobs, batchItemJob(batch, item, now))
	}
	return jobs
}

// ReserveRootTasks reserves the root tasks of a workflow that are jobs. Those that are
// workflows of their own are left to the orchestrator, which starts them once it learns that
// the workflow has started.
func ReserveRootTasks(wf *types.WorkflowInstance, now time.Time) []*types.Job {
	var jobs []*types.Job
	for _, name := range RootTasks(&wf.Definition) {
		def := findTaskDef(&wf.Definition, name)
		if def.ChildWorkflowDef != nil {
			continue
		}
		state := wf.Tasks[name]
		state.JobID = xid.New().String()
		state.Status = types.JobStatusRunning
		state.StartedAt = &now
		wf.Tasks[name] = state
		jobs = append(jobs, taskJob(wf, def, state.JobID, now))
	}
	return jobs
}

// The hooks of a workflow.
const (
	hookInit    = "on_init"
	hookSuccess = "on_success"
	hookFailure = "on_failure"
	hookExit    = "on_exit"
)

// ReserveInitHook reserves the hook that starts before the root tasks of a workflow, if it
// has one.
func ReserveInitHook(wf *types.WorkflowInstance, now time.Time) []*types.Job {
	return reserveHooks(wf, now, hookInit)
}

// reserveHooks reserves those of the hooks that the workflow has.
func reserveHooks(wf *types.WorkflowInstance, now time.Time, names ...string) []*types.Job {
	var jobs []*types.Job
	for _, name := range names {
		if hookDef(wf, name) == nil {
			continue
		}
		if wf.HookStates == nil {
			wf.HookStates = make(map[string]types.WorkflowTaskState)
		}
		wf.HookStates[name] = types.WorkflowTaskState{
			Name:      name,
			JobID:     xid.New().String(),
			Status:    types.JobStatusRunning,
			StartedAt: &now,
		}
		jobs = append(jobs, hookJob(wf, name, now))
	}
	return jobs
}

// hookDef returns the definition of a hook, or nil if the workflow does not have it.
func hookDef(wf *types.WorkflowInstance, name string) *types.WorkflowHookDef {
	hooks := wf.Definition.Hooks
	if hooks == nil {
		return nil
	}
	switch name {
	case hookInit:
		return hooks.OnInit
	case hookSuccess:
		return hooks.OnSuccess
	case hookFailure:
		return hooks.OnFailure
	case hookExit:
		return hooks.OnExit
	}
	return nil
}

// ResetWorkflow puts a workflow that has ended back to running. Tasks that failed or were
// cancelled are pending again. The orchestrator goes on with it when it learns of that, and
// finds out about tasks that were still under way when the workflow ended.
func ResetWorkflow(wf *types.WorkflowInstance, now time.Time) {
	wf.Attempt++
	wf.Status = types.WorkflowStatusRunning
	wf.UpdatedAt = now
	wf.CompletedAt = nil
	wf.Deadline = deadline(wf.Definition.ExecutionTimeout, now)
	for name, state := range wf.Tasks {
		switch state.Status {
		case types.JobStatusFailed, types.JobStatusDead, types.JobStatusCancelled:
			resetTask(&state)
			wf.Tasks[name] = state
		}
	}
}

// deadline returns the deadline of an attempt that starts now. Each has the timeout to itself.
func deadline(timeout *types.Duration, now time.Time) *time.Time {
	if timeout == nil {
		return nil
	}
	at := now.Add(timeout.Std())
	return &at
}

func resetTask(state *types.WorkflowTaskState) {
	state.Status = types.JobStatusPending
	state.JobID = ""
	state.ChildWorkflowID = ""
	state.Error = nil
	state.StartedAt = nil
	state.FinishedAt = nil
}

// ResetBatch puts a batch that has ended back to running. Items that failed or were
// cancelled are pending again, and those that completed as well unless failedOnly is set.
// The orchestrator goes on with it when it learns of that, and finds out about items that
// were still under way when the batch ended.
func ResetBatch(batch *types.BatchInstance, failedOnly bool, now time.Time) {
	batch.Attempt++
	batch.Status = types.WorkflowStatusRunning
	batch.UpdatedAt = now
	batch.CompletedAt = nil
	batch.Deadline = deadline(batch.Definition.ExecutionTimeout, now)
	batch.NextChunkIndex = 0

	for id, state := range batch.ItemStates {
		switch state.Status {
		case types.JobStatusFailed, types.JobStatusDead, types.JobStatusCancelled:
			resetItem(&state)
		case types.JobStatusCompleted:
			if !failedOnly {
				resetItem(&state)
				state.Iteration++
			}
		}
		batch.ItemStates[id] = state
	}
	countItems(batch)

	if NeedsBatchOnetime(batch) {
		batch.OnetimeState = nil
	}
}

func resetItem(state *types.BatchItemState) {
	state.Status = types.JobStatusPending
	state.JobID = ""
	state.Error = nil
	state.StartedAt = nil
	state.FinishedAt = nil
}

// countItems counts the items of a batch by their states.
func countItems(batch *types.BatchInstance) {
	batch.PendingItems, batch.RunningItems, batch.CompletedItems, batch.FailedItems = 0, 0, 0, 0
	for _, state := range batch.ItemStates {
		switch state.Status {
		case types.JobStatusPending:
			batch.PendingItems++
		case types.JobStatusRunning, types.JobStatusRetrying:
			batch.RunningItems++
		case types.JobStatusCompleted:
			batch.CompletedItems++
		case types.JobStatusFailed, types.JobStatusDead:
			batch.FailedItems++
		}
	}
}

// batchOnetimeJob returns the job of the onetime task that is reserved.
func batchOnetimeJob(batch *types.BatchInstance, now time.Time) *types.Job {
	job := newJob(batch.OnetimeState.JobID, *batch.Definition.OnetimeTaskType, batch.Definition.OnetimePayload, batch.ID+":onetime", now)
	job.Priority = batch.Definition.DefaultPriority
	job.BatchID = &batch.ID
	job.BatchRole = strPtr("onetime")
	return job
}

// batchItemJob returns the job of an item that is reserved.
func batchItemJob(batch *types.BatchInstance, item *types.BatchItem, now time.Time) *types.Job {
	state := batch.ItemStates[item.ID]
	operation := batch.ID + ":item:" + item.ID
	if state.Iteration > 0 {
		operation += fmt.Sprintf(":%d", state.Iteration)
	}
	job := newJob(state.JobID, batch.Definition.ItemTaskType, item.Payload, operation, now)
	job.Priority = batch.Definition.DefaultPriority
	job.RetryPolicy = batch.Definition.ItemRetryPolicy
	job.BatchID = &batch.ID
	job.BatchItem = &item.ID
	job.BatchRole = strPtr("item")
	return job
}

// taskJob returns the job of a task of a workflow.
func taskJob(wf *types.WorkflowInstance, def *types.WorkflowTask, jobID string, now time.Time) *types.Job {
	operation := fmt.Sprintf("%s:%s:%d", wf.ID, def.Name, wf.Tasks[def.Name].Iteration)
	job := newJob(jobID, def.TaskType, def.Payload, operation, now)
	job.Priority = wf.Definition.DefaultPriority
	job.WorkflowID = &wf.ID
	job.WorkflowTask = &def.Name
	return job
}

// hookJob returns the job of a hook that is reserved.
func hookJob(wf *types.WorkflowInstance, name string, now time.Time) *types.Job {
	def, taskName, operation := hookDef(wf, name), hookTaskPrefix+name, wf.ID+":hook:"+name
	if wf.Attempt > 1 { // a workflow that is retried ends once more
		operation += fmt.Sprintf(":%d", wf.Attempt)
	}
	job := newJob(wf.HookStates[name].JobID, def.TaskType, def.Payload, operation, now)
	job.Priority = wf.Definition.DefaultPriority
	job.WorkflowID = &wf.ID
	job.WorkflowTask = &taskName
	return job
}

// newJob returns a job that is to run at once. Executions of the same operation count once.
func newJob(id string, taskType types.TaskType, payload []byte, operation string, now time.Time) *types.Job {
	return &types.Job{
		ID:        id,
		TaskType:  taskType,
		Payload:   payload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		Status:    types.JobStatusPending,
		Headers:   map[string]string{"x-dureq-operation-key": operation},
		CreatedAt: now,
		UpdatedAt: now,
	}
}
