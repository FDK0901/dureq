package actors

import (
	"context"
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq"
	"github.com/FDK0901/dureq/internal/messages"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/internal/workflow"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	chainedslog "github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/anthdm/hollywood/actor"
	"github.com/anthdm/hollywood/cluster"
	"github.com/bytedance/sonic"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

const (
	timeoutCheckInterval = 5 * time.Second
	casMaxRetries        = 5
)

// OrchestratorActor is a cluster singleton that handles workflow DAG execution
// and batch processing. It receives domain events (job completed/failed) and
// advances the state machines for workflows and batches accordingly.
type OrchestratorActor struct {
	store         *store.RedisStore
	cluster       *cluster.Cluster
	dispatcherPID *actor.PID
	repeater      actor.SendRepeater
	logger        gochainedlog.Logger
}

// NewOrchestratorActor returns a Hollywood Producer that creates an OrchestratorActor.
// The cluster reference is used to discover the DispatcherActor PID at runtime.
func NewOrchestratorActor(s *store.RedisStore, c *cluster.Cluster, logger gochainedlog.Logger) actor.Producer {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return func() actor.Receiver {
		return &OrchestratorActor{
			store:   s,
			cluster: c,
			logger:  logger,
		}
	}
}

// Receive implements actor.Receiver.
func (o *OrchestratorActor) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case actor.Started:
		o.onStarted(ctx)
	case actor.Stopped:
		o.onStopped()
	case *pb.SingletonCheckMsg:
		ctx.Respond(&pb.SingletonCheckMsg{})
	case *pb.DomainEventMsg:
		o.onDomainEvent(ctx, msg)
	case messages.TimeoutCheckMsg:
		o.onTimeoutCheck(ctx)
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) onStarted(ctx *actor.Context) {
	o.repeater = ctx.SendRepeat(ctx.PID(), messages.TimeoutCheckMsg{}, timeoutCheckInterval)

	// Drain any unprocessed events from the Redis events stream.
	o.drainPendingEvents(ctx)

	o.logger.Info().Duration("timeout_check_interval", timeoutCheckInterval).Msg("orchestrator actor started")
}

func (o *OrchestratorActor) onStopped() {
	o.repeater.Stop()
	o.logger.Info().Msg("orchestrator actor stopped")
}

// ---------------------------------------------------------------------------
// Domain event routing
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) onDomainEvent(ctx *actor.Context, msg *pb.DomainEventMsg) {
	event := messages.DomainEventToJobEvent(msg)

	switch event.Type {
	case types.EventJobCompleted:
		o.handleJobCompleted(ctx, event)
	case types.EventJobFailed, types.EventJobDead:
		o.handleJobFailed(ctx, event)
	case types.EventWorkflowRetrying:
		o.handleWorkflowRetry(ctx, event)
	case types.EventBatchRetrying:
		o.handleBatchRetry(ctx, event)
	}
}

// ---------------------------------------------------------------------------
// Workflow: job completed
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) handleJobCompleted(ctx *actor.Context, event types.JobEvent) {
	if event.JobID == "" {
		return
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, _, err := o.store.GetJob(bgCtx, event.JobID)
	if err != nil {
		return
	}

	// Route to batch handler if this is a batch job.
	if job.BatchID != nil {
		o.handleBatchJobCompleted(ctx, event, job)
		return
	}

	if job.WorkflowID == nil {
		return
	}

	wfID := *job.WorkflowID
	taskName := ""
	if job.WorkflowTask != nil {
		taskName = *job.WorkflowTask
	}

	o.logger.Info().String("workflow_id", wfID).String("task", taskName).Msg("orchestrator: workflow task completed")

	now := time.Now()

	// Publish task completion event.
	o.publishEvent(ctx, types.JobEvent{
		Type:      types.EventWorkflowTaskCompleted,
		JobID:     event.JobID,
		Timestamp: now,
	})

	// CAS retry loop: re-read workflow on conflict to pick up concurrent changes.
	for attempt := 0; attempt < casMaxRetries; attempt++ {
		wf, rev, err := o.store.GetWorkflow(bgCtx, wfID)
		if err != nil {
			o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to get workflow")
			return
		}
		if wf.Status.IsTerminal() {
			return
		}

		// Mark task as completed.
		if state, ok := wf.Tasks[taskName]; ok {
			state.Status = types.JobStatusCompleted
			state.FinishedAt = &now
			wf.Tasks[taskName] = state
		}

		// Check if all tasks are done.
		if workflow.AllTasksCompleted(wf) {
			wf.Status = types.WorkflowStatusCompleted
			wf.CompletedAt = &now
			wf.UpdatedAt = now
			if _, err := o.store.UpdateWorkflow(bgCtx, wf, rev); err != nil {
				if err == store.ErrCASConflict {
					o.logger.Debug().String("workflow_id", wfID).Int("attempt", attempt).Msg("orchestrator: CAS conflict on workflow completion, retrying")
					continue
				}
				o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save completed workflow")
				return
			}
			o.publishEvent(ctx, types.JobEvent{
				Type:      types.EventWorkflowCompleted,
				JobID:     wfID,
				Timestamp: now,
			})
			o.logger.Info().String("workflow_id", wfID).Msg("orchestrator: workflow completed")
			return
		}

		// Save the task completion first (without dispatching).
		wf.Status = types.WorkflowStatusRunning
		wf.UpdatedAt = now
		newRev, err := o.store.UpdateWorkflow(bgCtx, wf, rev)
		if err != nil {
			if err == store.ErrCASConflict {
				o.logger.Debug().String("workflow_id", wfID).Int("attempt", attempt).Msg("orchestrator: CAS conflict on task completion, retrying")
				continue
			}
			o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save task completion")
			return
		}

		// Task completion saved. Now dispatch ready tasks.
		ready := workflow.ReadyTasks(wf)
		if len(ready) > 0 {
			for _, name := range ready {
				o.dispatchWorkflowTask(ctx, bgCtx, wf, name, &now)
			}
			if _, err := o.store.UpdateWorkflow(bgCtx, wf, newRev); err != nil {
				o.logger.Warn().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save dispatched task states (non-critical)")
			}
		}
		return
	}

	o.logger.Error().String("workflow_id", wfID).String("task", taskName).Msg("orchestrator: exhausted CAS retries for workflow update")
}

// ---------------------------------------------------------------------------
// Workflow: job failed
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) handleJobFailed(ctx *actor.Context, event types.JobEvent) {
	if event.JobID == "" {
		return
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, _, err := o.store.GetJob(bgCtx, event.JobID)
	if err != nil {
		return
	}

	// Route to batch handler if this is a batch job.
	if job.BatchID != nil {
		o.handleBatchJobFailed(ctx, event, job)
		return
	}

	if job.WorkflowID == nil {
		return
	}

	wfID := *job.WorkflowID
	taskName := ""
	if job.WorkflowTask != nil {
		taskName = *job.WorkflowTask
	}

	o.logger.Warn().String("workflow_id", wfID).String("task", taskName).Msg("orchestrator: workflow task failed")

	now := time.Now()
	errStr := "task execution failed"
	if event.Error != nil {
		errStr = *event.Error
	}

	// CAS retry loop.
	for attempt := 0; attempt < casMaxRetries; attempt++ {
		wf, rev, err := o.store.GetWorkflow(bgCtx, wfID)
		if err != nil || wf.Status.IsTerminal() {
			return
		}

		// Mark the failed task.
		if state, ok := wf.Tasks[taskName]; ok {
			state.Status = types.JobStatusFailed
			state.FinishedAt = &now
			state.Error = &errStr
			wf.Tasks[taskName] = state
		}

		// Check if workflow-level retry is available.
		if wf.Definition.RetryPolicy != nil && wf.Attempt < wf.Definition.RetryPolicy.MaxAttempts {
			o.retryWorkflow(ctx, bgCtx, wf, rev, &now)
			return
		}

		// Mark workflow as failed.
		wf.Status = types.WorkflowStatusFailed
		wf.UpdatedAt = now
		if _, err := o.store.UpdateWorkflow(bgCtx, wf, rev); err != nil {
			if err == store.ErrCASConflict {
				o.logger.Debug().String("workflow_id", wfID).Int("attempt", attempt).Msg("orchestrator: CAS conflict on workflow failure, retrying")
				continue
			}
			o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save failed workflow")
			return
		}

		o.publishEvent(ctx, types.JobEvent{
			Type:      types.EventWorkflowFailed,
			JobID:     wfID,
			Timestamp: now,
		})
		o.publishEvent(ctx, types.JobEvent{
			Type:      types.EventWorkflowTaskFailed,
			JobID:     event.JobID,
			Timestamp: now,
		})

		o.logger.Info().String("workflow_id", wfID).String("failed_task", taskName).Msg("orchestrator: workflow failed")
		return
	}

	o.logger.Error().String("workflow_id", wfID).String("task", taskName).Msg("orchestrator: exhausted CAS retries for workflow failure update")
}

// ---------------------------------------------------------------------------
// Workflow: dispatch a single task
// ---------------------------------------------------------------------------

// dispatchWorkflowTask creates a job for a workflow task and dispatches it.
func (o *OrchestratorActor) dispatchWorkflowTask(actorCtx *actor.Context, bgCtx context.Context, wf *types.WorkflowInstance, taskName string, now *time.Time) {
	// Find the task definition.
	var taskDef *types.WorkflowTask
	for i := range wf.Definition.Tasks {
		if wf.Definition.Tasks[i].Name == taskName {
			taskDef = &wf.Definition.Tasks[i]
			break
		}
	}
	if taskDef == nil {
		return
	}

	// Create a job for this workflow task.
	job := &types.Job{
		ID:           xid.New().String(),
		TaskType:     taskDef.TaskType,
		Payload:      taskDef.Payload,
		Schedule:     types.Schedule{Type: types.ScheduleImmediate},
		Status:       types.JobStatusPending,
		Priority:     wf.Definition.DefaultPriority,
		WorkflowID:   &wf.ID,
		WorkflowTask: &taskName,
		CreatedAt:    *now,
		UpdatedAt:    *now,
	}

	if _, err := o.store.CreateJob(bgCtx, job); err != nil {
		o.logger.Error().String("workflow_id", wf.ID).String("task", taskName).Err(err).Msg("orchestrator: failed to create job for workflow task")
		return
	}

	// Dispatch via the DispatcherActor.
	if pid := o.resolveDispatcherPID(); pid != nil {
		dispatchMsg := messages.JobToDispatchMsg(job, 0)
		actorCtx.Send(pid, dispatchMsg)
	}

	// Update task state.
	if state, ok := wf.Tasks[taskName]; ok {
		state.JobID = job.ID
		state.Status = types.JobStatusRunning
		state.StartedAt = now
		wf.Tasks[taskName] = state
	}

	o.publishEvent(actorCtx, types.JobEvent{
		Type:      types.EventWorkflowTaskDispatched,
		JobID:     job.ID,
		TaskType:  job.TaskType,
		Timestamp: *now,
	})

	o.logger.Info().String("workflow_id", wf.ID).String("task", taskName).String("job_id", job.ID).Msg("orchestrator: workflow task dispatched")
}

// ---------------------------------------------------------------------------
// Workflow: retry (reset all tasks, re-dispatch roots)
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) retryWorkflow(actorCtx *actor.Context, bgCtx context.Context, wf *types.WorkflowInstance, rev uint64, now *time.Time) {
	wf.Attempt++
	wf.Status = types.WorkflowStatusRunning
	wf.UpdatedAt = *now
	wf.CompletedAt = nil

	// Recalculate deadline if ExecutionTimeout is set.
	if wf.Definition.ExecutionTimeout != nil {
		deadline := now.Add(wf.Definition.ExecutionTimeout.Std())
		wf.Deadline = &deadline
	}

	// Reset all tasks to pending.
	for name, state := range wf.Tasks {
		state.Status = types.JobStatusPending
		state.JobID = ""
		state.Error = nil
		state.StartedAt = nil
		state.FinishedAt = nil
		wf.Tasks[name] = state
	}

	newRev, err := o.store.UpdateWorkflow(bgCtx, wf, rev)
	if err != nil {
		o.logger.Error().String("workflow_id", wf.ID).Err(err).Msg("orchestrator: failed to save workflow retry reset")
		return
	}

	// Re-dispatch root tasks.
	roots := workflow.RootTasks(&wf.Definition)
	for _, name := range roots {
		o.dispatchWorkflowTask(actorCtx, bgCtx, wf, name, now)
	}

	// Save again after dispatch updates task states.
	o.store.UpdateWorkflow(bgCtx, wf, newRev)

	o.publishEvent(actorCtx, types.JobEvent{
		Type:      types.EventWorkflowRetrying,
		JobID:     wf.ID,
		Attempt:   wf.Attempt,
		Timestamp: *now,
	})

	o.logger.Info().String("workflow_id", wf.ID).Int("attempt", wf.Attempt).Msg("orchestrator: workflow retrying")
}

// ---------------------------------------------------------------------------
// Workflow retry dispatch (API-triggered)
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) handleWorkflowRetry(actorCtx *actor.Context, event types.JobEvent) {
	wfID := event.JobID
	if wfID == "" {
		return
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wf, rev, err := o.store.GetWorkflow(bgCtx, wfID)
	if err != nil || wf.Status.IsTerminal() {
		return
	}

	now := time.Now()
	roots := workflow.RootTasks(&wf.Definition)
	for _, name := range roots {
		o.dispatchWorkflowTask(actorCtx, bgCtx, wf, name, &now)
	}

	if _, err := o.store.UpdateWorkflow(bgCtx, wf, rev); err != nil {
		o.logger.Warn().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save workflow after retry dispatch")
	}

	o.logger.Info().String("workflow_id", wfID).Int("roots", len(roots)).Msg("orchestrator: dispatched root tasks for workflow retry")
}

// ---------------------------------------------------------------------------
// Batch: job completed
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) handleBatchJobCompleted(actorCtx *actor.Context, event types.JobEvent, job *types.Job) {
	batchID := *job.BatchID
	role := ""
	if job.BatchRole != nil {
		role = *job.BatchRole
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	batch, rev, err := o.store.GetBatch(bgCtx, batchID)
	if err != nil {
		o.logger.Error().String("batch_id", batchID).Err(err).Msg("orchestrator: failed to get batch")
		return
	}
	if batch.Status.IsTerminal() {
		return
	}

	now := time.Now()

	switch role {
	case "onetime":
		// Onetime preprocessing completed — start item dispatch.
		o.logger.Info().String("batch_id", batchID).Msg("orchestrator: batch onetime completed")

		if batch.OnetimeState != nil {
			batch.OnetimeState.Status = types.JobStatusCompleted
			batch.OnetimeState.FinishedAt = &now
		}

		// Capture onetime output.
		if result, err := o.store.GetResult(bgCtx, event.JobID); err == nil && result != nil {
			if batch.OnetimeState != nil {
				batch.OnetimeState.ResultData = result.Output
			}
		}

		o.publishEvent(actorCtx, types.JobEvent{
			Type:      types.EventBatchOnetimeCompleted,
			JobID:     batchID,
			Timestamp: now,
		})

		// Start dispatching item chunks.
		batch.Status = types.WorkflowStatusRunning
		batch.UpdatedAt = now
		o.dispatchBatchChunk(actorCtx, bgCtx, batch, &now)
		o.store.UpdateBatch(bgCtx, batch, rev)

	case "item":
		// Individual item completed.
		itemID := ""
		if job.BatchItem != nil {
			itemID = *job.BatchItem
		}

		o.logger.Debug().String("batch_id", batchID).String("item_id", itemID).Msg("orchestrator: batch item completed")

		if state, ok := batch.ItemStates[itemID]; ok {
			state.Status = types.JobStatusCompleted
			state.FinishedAt = &now
			batch.ItemStates[itemID] = state
		}
		batch.CompletedItems++
		if batch.RunningItems > 0 {
			batch.RunningItems--
		}

		// Save item result.
		itemResult := &types.BatchItemResult{
			BatchID: batchID,
			ItemID:  itemID,
			Success: true,
		}
		if result, err := o.store.GetResult(bgCtx, event.JobID); err == nil && result != nil {
			itemResult.Output = result.Output
		}
		o.store.SaveBatchItemResult(bgCtx, itemResult)

		o.publishEvent(actorCtx, types.JobEvent{
			Type:      types.EventBatchItemCompleted,
			JobID:     batchID,
			Timestamp: now,
		})

		o.publishBatchProgress(actorCtx, batch)

		// Check if we need more chunks or if batch is done.
		if o.isBatchDone(batch) {
			o.finalizeBatch(actorCtx, bgCtx, batch, rev, &now)
		} else {
			o.dispatchBatchChunk(actorCtx, bgCtx, batch, &now)
			batch.UpdatedAt = now
			o.store.UpdateBatch(bgCtx, batch, rev)
		}
	}
}

// ---------------------------------------------------------------------------
// Batch: job failed
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) handleBatchJobFailed(actorCtx *actor.Context, event types.JobEvent, job *types.Job) {
	batchID := *job.BatchID
	role := ""
	if job.BatchRole != nil {
		role = *job.BatchRole
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	batch, rev, err := o.store.GetBatch(bgCtx, batchID)
	if err != nil {
		o.logger.Error().String("batch_id", batchID).Err(err).Msg("orchestrator: failed to get batch")
		return
	}
	if batch.Status.IsTerminal() {
		return
	}

	now := time.Now()
	errStr := "execution failed"
	if event.Error != nil {
		errStr = *event.Error
	}

	switch role {
	case "onetime":
		// Onetime failure -> entire batch fails.
		o.logger.Warn().String("batch_id", batchID).Msg("orchestrator: batch onetime failed - failing batch")

		if batch.OnetimeState != nil {
			batch.OnetimeState.Status = types.JobStatusFailed
			batch.OnetimeState.FinishedAt = &now
			batch.OnetimeState.Error = &errStr
		}

		batch.Status = types.WorkflowStatusFailed
		batch.UpdatedAt = now
		batch.CompletedAt = &now
		o.store.UpdateBatch(bgCtx, batch, rev)

		o.publishEvent(actorCtx, types.JobEvent{
			Type:      types.EventBatchOnetimeFailed,
			JobID:     batchID,
			Timestamp: now,
		})
		o.publishEvent(actorCtx, types.JobEvent{
			Type:      types.EventBatchFailed,
			JobID:     batchID,
			Timestamp: now,
		})

	case "item":
		itemID := ""
		if job.BatchItem != nil {
			itemID = *job.BatchItem
		}

		o.logger.Warn().String("batch_id", batchID).String("item_id", itemID).Msg("orchestrator: batch item failed")

		if state, ok := batch.ItemStates[itemID]; ok {
			state.Status = types.JobStatusFailed
			state.FinishedAt = &now
			state.Error = &errStr
			batch.ItemStates[itemID] = state
		}
		batch.FailedItems++
		if batch.RunningItems > 0 {
			batch.RunningItems--
		}

		// Save item result.
		o.store.SaveBatchItemResult(bgCtx, &types.BatchItemResult{
			BatchID: batchID,
			ItemID:  itemID,
			Success: false,
			Error:   &errStr,
		})

		o.publishEvent(actorCtx, types.JobEvent{
			Type:      types.EventBatchItemFailed,
			JobID:     batchID,
			Timestamp: now,
		})

		o.publishBatchProgress(actorCtx, batch)

		// Check failure policy.
		if batch.Definition.FailurePolicy == types.BatchFailFast {
			batch.Status = types.WorkflowStatusFailed
			batch.UpdatedAt = now
			batch.CompletedAt = &now
			o.store.UpdateBatch(bgCtx, batch, rev)

			o.publishEvent(actorCtx, types.JobEvent{
				Type:      types.EventBatchFailed,
				JobID:     batchID,
				Timestamp: now,
			})
			o.logger.Info().String("batch_id", batchID).Msg("orchestrator: batch failed (fail_fast)")
			return
		}

		// continue_on_error: check if batch is done.
		if o.isBatchDone(batch) {
			o.finalizeBatch(actorCtx, bgCtx, batch, rev, &now)
		} else {
			o.dispatchBatchChunk(actorCtx, bgCtx, batch, &now)
			batch.UpdatedAt = now
			o.store.UpdateBatch(bgCtx, batch, rev)
		}
	}
}

// ---------------------------------------------------------------------------
// Batch: chunk dispatch
// ---------------------------------------------------------------------------

// dispatchBatchChunk dispatches the next chunk of batch items.
func (o *OrchestratorActor) dispatchBatchChunk(actorCtx *actor.Context, bgCtx context.Context, batch *types.BatchInstance, now *time.Time) {
	chunkSize := batch.Definition.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 100
	}

	items := batch.Definition.Items
	dispatched := 0

	for batch.NextChunkIndex < len(items) && dispatched < chunkSize && batch.RunningItems < chunkSize {
		item := items[batch.NextChunkIndex]
		batch.NextChunkIndex++

		// Skip already processed items (e.g. on retry).
		if state, ok := batch.ItemStates[item.ID]; ok && state.Status != types.JobStatusPending {
			continue
		}

		job := &types.Job{
			ID:          xid.New().String(),
			TaskType:    batch.Definition.ItemTaskType,
			Payload:     item.Payload,
			Schedule:    types.Schedule{Type: types.ScheduleImmediate},
			Status:      types.JobStatusPending,
			Priority:    batch.Definition.DefaultPriority,
			BatchID:     &batch.ID,
			BatchItem:   &item.ID,
			BatchRole:   strPtr("item"),
			RetryPolicy: batch.Definition.ItemRetryPolicy,
			CreatedAt:   *now,
			UpdatedAt:   *now,
		}

		if _, err := o.store.CreateJob(bgCtx, job); err != nil {
			o.logger.Error().String("batch_id", batch.ID).String("item_id", item.ID).Err(err).Msg("orchestrator: failed to create batch item job")
			continue
		}

		if pid := o.resolveDispatcherPID(); pid != nil {
			dispatchMsg := messages.JobToDispatchMsg(job, 0)
			actorCtx.Send(pid, dispatchMsg)
		}

		batch.ItemStates[item.ID] = types.BatchItemState{
			ItemID:    item.ID,
			JobID:     job.ID,
			Status:    types.JobStatusRunning,
			StartedAt: now,
		}
		batch.RunningItems++
		batch.PendingItems--
		dispatched++
	}

	if dispatched > 0 {
		o.logger.Info().String("batch_id", batch.ID).Int("dispatched", dispatched).Int("running", batch.RunningItems).Int("remaining", len(items)-batch.NextChunkIndex).Msg("orchestrator: batch chunk dispatched")
	}
}

// ---------------------------------------------------------------------------
// Batch: finalization
// ---------------------------------------------------------------------------

// isBatchDone returns true if all items have been completed or failed.
func (o *OrchestratorActor) isBatchDone(batch *types.BatchInstance) bool {
	return batch.CompletedItems+batch.FailedItems >= batch.TotalItems
}

// finalizeBatch marks the batch as completed or failed (with auto-retry check).
func (o *OrchestratorActor) finalizeBatch(actorCtx *actor.Context, bgCtx context.Context, batch *types.BatchInstance, rev uint64, now *time.Time) {
	batch.UpdatedAt = *now

	if batch.FailedItems > 0 {
		// Check if batch-level retry is available.
		if batch.Definition.RetryPolicy != nil && batch.Attempt < batch.Definition.RetryPolicy.MaxAttempts {
			o.retryBatch(actorCtx, bgCtx, batch, rev, now, true)
			return
		}

		batch.Status = types.WorkflowStatusFailed
		batch.CompletedAt = now
		o.publishEvent(actorCtx, types.JobEvent{
			Type:      types.EventBatchFailed,
			JobID:     batch.ID,
			Timestamp: *now,
		})
		o.logger.Info().String("batch_id", batch.ID).Int("completed", batch.CompletedItems).Int("failed", batch.FailedItems).Msg("orchestrator: batch finished with failures")
	} else {
		batch.Status = types.WorkflowStatusCompleted
		batch.CompletedAt = now
		o.publishEvent(actorCtx, types.JobEvent{
			Type:      types.EventBatchCompleted,
			JobID:     batch.ID,
			Timestamp: *now,
		})
		o.logger.Info().String("batch_id", batch.ID).Int("completed", batch.CompletedItems).Msg("orchestrator: batch completed successfully")
	}

	o.store.UpdateBatch(bgCtx, batch, rev)
	o.publishBatchProgress(actorCtx, batch)
}

// ---------------------------------------------------------------------------
// Batch: retry
// ---------------------------------------------------------------------------

// retryBatch resets failed items and re-dispatches them.
func (o *OrchestratorActor) retryBatch(actorCtx *actor.Context, bgCtx context.Context, batch *types.BatchInstance, rev uint64, now *time.Time, failedOnly bool) {
	batch.Attempt++
	batch.Status = types.WorkflowStatusRunning
	batch.UpdatedAt = *now
	batch.CompletedAt = nil

	// Recalculate deadline if ExecutionTimeout is set.
	if batch.Definition.ExecutionTimeout != nil {
		deadline := now.Add(batch.Definition.ExecutionTimeout.Std())
		batch.Deadline = &deadline
	}

	// Reset items.
	batch.FailedItems = 0
	batch.RunningItems = 0
	batch.PendingItems = 0
	batch.CompletedItems = 0

	for id, state := range batch.ItemStates {
		if failedOnly && state.Status != types.JobStatusFailed {
			if state.Status == types.JobStatusCompleted {
				batch.CompletedItems++
			}
			continue
		}
		state.Status = types.JobStatusPending
		state.JobID = ""
		state.Error = nil
		state.StartedAt = nil
		state.FinishedAt = nil
		batch.ItemStates[id] = state
		batch.PendingItems++
	}

	batch.NextChunkIndex = 0

	newRev, err := o.store.UpdateBatch(bgCtx, batch, rev)
	if err != nil {
		o.logger.Error().String("batch_id", batch.ID).Err(err).Msg("orchestrator: failed to save batch retry reset")
		return
	}

	// Re-dispatch chunks.
	o.dispatchBatchChunk(actorCtx, bgCtx, batch, now)
	o.store.UpdateBatch(bgCtx, batch, newRev)

	o.publishEvent(actorCtx, types.JobEvent{
		Type:      types.EventBatchRetrying,
		JobID:     batch.ID,
		Attempt:   batch.Attempt,
		Timestamp: *now,
	})

	o.logger.Info().String("batch_id", batch.ID).Int("attempt", batch.Attempt).Msg("orchestrator: batch retrying")
}

// ---------------------------------------------------------------------------
// Batch retry dispatch (API-triggered)
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) handleBatchRetry(actorCtx *actor.Context, event types.JobEvent) {
	batchID := event.JobID
	if batchID == "" {
		return
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	batch, rev, err := o.store.GetBatch(bgCtx, batchID)
	if err != nil || batch.Status.IsTerminal() {
		return
	}

	now := time.Now()
	o.dispatchBatchChunk(actorCtx, bgCtx, batch, &now)

	if _, err := o.store.UpdateBatch(bgCtx, batch, rev); err != nil {
		o.logger.Warn().String("batch_id", batchID).Err(err).Msg("orchestrator: failed to save batch after retry dispatch")
	}

	o.logger.Info().String("batch_id", batchID).Msg("orchestrator: dispatched chunk for batch retry")
}

// ---------------------------------------------------------------------------
// Batch progress
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) publishBatchProgress(actorCtx *actor.Context, batch *types.BatchInstance) {
	step := "processing"
	if batch.OnetimeState != nil && batch.OnetimeState.Status != types.JobStatusCompleted {
		step = "onetime"
	}
	if batch.Status.IsTerminal() {
		step = "done"
	}

	progress := &types.BatchProgress{
		BatchID: batch.ID,
		Done:    batch.CompletedItems + batch.FailedItems,
		Total:   batch.TotalItems,
		Failed:  batch.FailedItems,
		Step:    step,
	}

	o.publishEvent(actorCtx, types.JobEvent{
		Type:          types.EventBatchProgress,
		JobID:         batch.ID,
		Timestamp:     time.Now(),
		BatchProgress: progress,
	})
}

// ---------------------------------------------------------------------------
// Timeout checks
// ---------------------------------------------------------------------------

func (o *OrchestratorActor) onTimeoutCheck(ctx *actor.Context) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	o.checkWorkflowTimeouts(ctx, bgCtx)
	o.checkBatchTimeouts(ctx, bgCtx)
}

func (o *OrchestratorActor) checkWorkflowTimeouts(actorCtx *actor.Context, bgCtx context.Context) {
	workflows, err := o.store.ListWorkflows(bgCtx)
	if err != nil {
		return
	}

	now := time.Now()
	for _, wf := range workflows {
		if wf.Status.IsTerminal() || wf.Deadline == nil {
			continue
		}
		if now.After(*wf.Deadline) {
			o.timeoutWorkflow(actorCtx, bgCtx, wf)
		}
	}
}

func (o *OrchestratorActor) timeoutWorkflow(actorCtx *actor.Context, bgCtx context.Context, wf *types.WorkflowInstance) {
	_, rev, err := o.store.GetWorkflow(bgCtx, wf.ID)
	if err != nil {
		return
	}

	now := time.Now()
	wf.Status = types.WorkflowStatusFailed
	wf.CompletedAt = &now
	wf.UpdatedAt = now

	// Cancel all non-terminal tasks.
	for name, state := range wf.Tasks {
		if !state.Status.IsTerminal() {
			state.Status = types.JobStatusCancelled
			state.FinishedAt = &now
			errStr := types.ErrExecutionTimedOut.Error()
			state.Error = &errStr
			wf.Tasks[name] = state
		}
	}

	o.store.UpdateWorkflow(bgCtx, wf, rev)

	o.publishEvent(actorCtx, types.JobEvent{
		Type:      types.EventWorkflowTimedOut,
		JobID:     wf.ID,
		Timestamp: now,
	})

	o.logger.Warn().String("workflow_id", wf.ID).Msg("orchestrator: workflow timed out")
}

func (o *OrchestratorActor) checkBatchTimeouts(actorCtx *actor.Context, bgCtx context.Context) {
	batches, err := o.store.ListBatches(bgCtx)
	if err != nil {
		return
	}

	now := time.Now()
	for _, batch := range batches {
		if batch.Status.IsTerminal() || batch.Deadline == nil {
			continue
		}
		if now.After(*batch.Deadline) {
			o.timeoutBatch(actorCtx, bgCtx, batch)
		}
	}
}

func (o *OrchestratorActor) timeoutBatch(actorCtx *actor.Context, bgCtx context.Context, batch *types.BatchInstance) {
	_, rev, err := o.store.GetBatch(bgCtx, batch.ID)
	if err != nil {
		return
	}

	now := time.Now()
	batch.Status = types.WorkflowStatusFailed
	batch.CompletedAt = &now
	batch.UpdatedAt = now

	o.store.UpdateBatch(bgCtx, batch, rev)

	o.publishEvent(actorCtx, types.JobEvent{
		Type:      types.EventBatchTimedOut,
		JobID:     batch.ID,
		Timestamp: now,
	})

	o.logger.Warn().String("batch_id", batch.ID).Msg("orchestrator: batch timed out")
}

// ---------------------------------------------------------------------------
// Drain pending events from Redis stream
// ---------------------------------------------------------------------------

// drainPendingEvents processes any unacknowledged messages in the Redis events
// stream from a previous orchestrator run. This ensures no events are lost
// when the singleton migrates between nodes.
func (o *OrchestratorActor) drainPendingEvents(actorCtx *actor.Context) {
	streamKey := store.EventsStreamKey(o.store.Prefix())
	consumerName := "orchestrator-actor"

	bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for bgCtx.Err() == nil {
		cmd := o.store.Client().B().Xreadgroup().Group(store.OrchestratorConsumerGroup, consumerName).Count(100).Streams().Key(streamKey).Id("0").Build()
		xStreams, err := o.store.Client().Do(bgCtx, cmd).AsXRead()
		if err != nil {
			return
		}

		empty := true
		for _, entries := range xStreams {
			if len(entries) > 0 {
				empty = false
				for _, entry := range entries {
					o.processStreamEvent(actorCtx, bgCtx, entry)
					o.store.Client().Do(bgCtx, o.store.Client().B().Xack().Key(streamKey).Group(store.OrchestratorConsumerGroup).Id(entry.ID).Build())
				}
			}
		}
		if empty {
			return
		}
	}
}

// processStreamEvent extracts the event JSON from a stream message and routes it.
func (o *OrchestratorActor) processStreamEvent(actorCtx *actor.Context, bgCtx context.Context, entry rueidis.XRangeEntry) {
	dataStr, ok := entry.FieldValues["data"]
	if !ok {
		return
	}

	var event types.JobEvent
	if err := sonic.ConfigFastest.Unmarshal([]byte(dataStr), &event); err != nil {
		return
	}

	switch event.Type {
	case types.EventJobCompleted:
		o.handleJobCompleted(actorCtx, event)
	case types.EventJobFailed, types.EventJobDead:
		o.handleJobFailed(actorCtx, event)
	case types.EventWorkflowRetrying:
		o.handleWorkflowRetry(actorCtx, event)
	case types.EventBatchRetrying:
		o.handleBatchRetry(actorCtx, event)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// resolveDispatcherPID lazily looks up the DispatcherActor PID via the cluster.
func (o *OrchestratorActor) resolveDispatcherPID() *actor.PID {
	if o.dispatcherPID != nil {
		return o.dispatcherPID
	}
	if o.cluster == nil {
		return nil
	}
	pids := o.cluster.GetActiveByKind(KindDispatcher)
	if len(pids) > 0 && pids[0] != nil {
		o.dispatcherPID = pids[0]
	}
	return o.dispatcherPID
}

// publishEvent publishes a domain event directly to the Redis store.
func (o *OrchestratorActor) publishEvent(_ *actor.Context, event types.JobEvent) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := o.store.PublishEvent(bgCtx, event); err != nil {
		o.logger.Warn().String("type", string(event.Type)).Err(err).Msg("orchestrator: failed to publish event")
	}
}

// strPtr returns a pointer to the given string.
func strPtr(s string) *string { return &s }
