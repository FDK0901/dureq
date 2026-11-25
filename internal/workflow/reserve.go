package workflow

import (
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
		}
		batch.RunningItems++
		batch.PendingItems--
		jobs = append(jobs, batchItemJob(batch, item, now))
	}
	return jobs
}

// ReserveTasks reserves tasks of a workflow.
func ReserveTasks(wf *types.WorkflowInstance, names []string, now time.Time) []*types.Job {
	var jobs []*types.Job
	for _, name := range names {
		def := findTaskDef(&wf.Definition, name)
		state, ok := wf.Tasks[name]
		if def == nil || !ok {
			continue
		}
		state.JobID = xid.New().String()
		state.Status = types.JobStatusRunning
		state.StartedAt = &now
		wf.Tasks[name] = state
		jobs = append(jobs, taskJob(wf, def, state.JobID, now))
	}
	return jobs
}

// ReserveRootTasks reserves the root tasks of a workflow that are jobs. Those that are
// workflows of their own are left to the orchestrator, which starts them once it learns that
// the workflow has started.
func ReserveRootTasks(wf *types.WorkflowInstance, now time.Time) []*types.Job {
	var names []string
	for _, name := range RootTasks(&wf.Definition) {
		if findTaskDef(&wf.Definition, name).ChildWorkflowDef == nil {
			names = append(names, name)
		}
	}
	return ReserveTasks(wf, names, now)
}

// batchOnetimeJob returns the job of the onetime task that is reserved.
func batchOnetimeJob(batch *types.BatchInstance, now time.Time) *types.Job {
	job := newJob(batch.OnetimeState.JobID, *batch.Definition.OnetimeTaskType, batch.Definition.OnetimePayload, now)
	job.Priority = batch.Definition.DefaultPriority
	job.BatchID = &batch.ID
	job.BatchRole = strPtr("onetime")
	return job
}

// batchItemJob returns the job of an item that is reserved.
func batchItemJob(batch *types.BatchInstance, item *types.BatchItem, now time.Time) *types.Job {
	job := newJob(batch.ItemStates[item.ID].JobID, batch.Definition.ItemTaskType, item.Payload, now)
	job.Priority = batch.Definition.DefaultPriority
	job.RetryPolicy = batch.Definition.ItemRetryPolicy
	job.BatchID = &batch.ID
	job.BatchItem = &item.ID
	job.BatchRole = strPtr("item")
	return job
}

// taskJob returns the job of a task of a workflow.
func taskJob(wf *types.WorkflowInstance, def *types.WorkflowTask, jobID string, now time.Time) *types.Job {
	job := newJob(jobID, def.TaskType, def.Payload, now)
	job.Priority = wf.Definition.DefaultPriority
	job.WorkflowID = &wf.ID
	job.WorkflowTask = &def.Name
	return job
}

func newJob(id string, taskType types.TaskType, payload []byte, now time.Time) *types.Job {
	return &types.Job{
		ID:        id,
		TaskType:  taskType,
		Payload:   payload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		Status:    types.JobStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
}
