package workflow

import (
	"context"
	"sync"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/bytedance/sonic"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// Orchestrator runs on the leader and advances workflow state
// when individual tasks complete or fail.
type Orchestrator struct {
	store      *store.RedisStore
	dispatcher *dispatcher.Dispatcher
	logger     gochainedlog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	active bool
}

// OrchestratorConfig holds the configuration for the workflow orchestrator.
type OrchestratorConfig struct {
	Store      *store.RedisStore
	Dispatcher *dispatcher.Dispatcher
	Logger     gochainedlog.Logger
}

// NewOrchestrator creates a new workflow orchestrator.
func NewOrchestrator(cfg OrchestratorConfig) *Orchestrator {
	if cfg.Logger == nil {
		cfg.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return &Orchestrator{
		store:      cfg.Store,
		dispatcher: cfg.Dispatcher,
		logger:     cfg.Logger,
	}
}

// Start begins listening for job completion/failure events.
func (o *Orchestrator) Start(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.active {
		return
	}

	orchCtx, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	o.done = make(chan struct{})
	o.active = true

	go o.eventLoop(orchCtx)
	go o.timeoutCheckLoop(orchCtx)
	o.logger.Info().Msg("workflow orchestrator started")
}

// Stop halts the orchestrator.
func (o *Orchestrator) Stop() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if !o.active {
		return
	}

	o.cancel()
	<-o.done
	o.active = false
	o.logger.Info().Msg("workflow orchestrator stopped")
}

// eventLoop reads events from the durable Redis Stream via XREADGROUP.
// Unlike Pub/Sub, stream-based consumption guarantees no events are lost
// even when the orchestrator restarts or leadership changes.
func (o *Orchestrator) eventLoop(ctx context.Context) {
	defer close(o.done)

	streamKey := store.EventsStreamKey(o.store.Prefix())
	consumerName := "orchestrator"

	// Phase 1: drain any pending (unacknowledged) messages from a previous run.
	o.drainPending(ctx, streamKey, consumerName)

	// Phase 2: read new messages.
	for ctx.Err() == nil {
		cmd := o.store.Client().B().Xreadgroup().Group(store.OrchestratorConsumerGroup, consumerName).Count(100).Block(1000).Streams().Key(streamKey).Id(">").Build()
		xStreams, err := o.store.Client().Do(ctx, cmd).AsXRead()
		if err != nil {
			if !rueidis.IsRedisNil(err) && ctx.Err() == nil {
				o.logger.Warn().Err(err).Msg("orchestrator: XREADGROUP error")
			}
			continue
		}

		for _, entries := range xStreams {
			for _, entry := range entries {
				o.processStreamEvent(ctx, entry)
				o.store.Client().Do(ctx, o.store.Client().B().Xack().Key(streamKey).Group(store.OrchestratorConsumerGroup).Id(entry.ID).Build())
			}
		}
	}
}

// drainPending processes any unacknowledged messages left from a previous orchestrator run.
func (o *Orchestrator) drainPending(ctx context.Context, streamKey, consumerName string) {
	for ctx.Err() == nil {
		cmd := o.store.Client().B().Xreadgroup().Group(store.OrchestratorConsumerGroup, consumerName).Count(100).Streams().Key(streamKey).Id("0").Build()
		xStreams, err := o.store.Client().Do(ctx, cmd).AsXRead()
		if err != nil {
			return
		}

		empty := true
		for _, entries := range xStreams {
			if len(entries) > 0 {
				empty = false
				for _, entry := range entries {
					o.processStreamEvent(ctx, entry)
					o.store.Client().Do(ctx, o.store.Client().B().Xack().Key(streamKey).Group(store.OrchestratorConsumerGroup).Id(entry.ID).Build())
				}
			}
		}
		if empty {
			return
		}
	}
}

// processStreamEvent extracts the event JSON from a stream message and routes it.
func (o *Orchestrator) processStreamEvent(ctx context.Context, entry rueidis.XRangeEntry) {
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
		o.handleJobCompleted(ctx, event)
	case types.EventJobFailed, types.EventJobDead:
		o.handleJobFailed(ctx, event)
	case types.EventWorkflowRetrying:
		o.handleWorkflowRetry(ctx, event)
	case types.EventBatchRetrying:
		o.handleBatchRetry(ctx, event)
	}
}

func (o *Orchestrator) handleJobCompleted(ctx context.Context, event types.JobEvent) {
	if event.JobID == "" {
		return
	}

	// Load the job to check if it's part of a workflow or batch.
	job, _, err := o.store.GetJob(ctx, event.JobID)
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

	o.logger.Info().String("workflow_id", wfID).String("task", taskName).Msg("workflow task completed")

	now := time.Now()

	// Publish task completion event (idempotent, safe to call once).
	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowTaskCompleted,
		JobID:     event.JobID,
		Timestamp: now,
	})

	// Retry loop: re-read workflow on CAS conflict to pick up concurrent changes.
	for attempt := 0; attempt < 5; attempt++ {
		wf, rev, err := o.store.GetWorkflow(ctx, wfID)
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
		if AllTasksCompleted(wf) {
			wf.Status = types.WorkflowStatusCompleted
			wf.CompletedAt = &now
			wf.UpdatedAt = now
			if _, err := o.store.UpdateWorkflow(ctx, wf, rev); err != nil {
				if err == store.ErrCASConflict {
					o.logger.Debug().String("workflow_id", wfID).Int("attempt", attempt).Msg("orchestrator: CAS conflict on workflow completion, retrying")
					continue
				}
				o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save completed workflow")
				return
			}
			o.dispatcher.PublishEvent(types.JobEvent{
				Type:      types.EventWorkflowCompleted,
				JobID:     wfID,
				Timestamp: now,
			})
			o.logger.Info().String("workflow_id", wfID).Msg("workflow completed")
			return
		}

		// Save the task completion first (without dispatching).
		wf.Status = types.WorkflowStatusRunning
		wf.UpdatedAt = now
		newRev, err := o.store.UpdateWorkflow(ctx, wf, rev)
		if err != nil {
			if err == store.ErrCASConflict {
				o.logger.Debug().String("workflow_id", wfID).Int("attempt", attempt).Msg("orchestrator: CAS conflict on task completion, retrying")
				continue
			}
			o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save task completion")
			return
		}

		// Task completion saved. Now dispatch ready tasks.
		ready := ReadyTasks(wf)
		if len(ready) > 0 {
			for _, name := range ready {
				o.dispatchWorkflowTask(ctx, wf, name, &now)
			}
			if _, err := o.store.UpdateWorkflow(ctx, wf, newRev); err != nil {
				o.logger.Warn().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save dispatched task states (non-critical)")
			}
		}
		return
	}

	o.logger.Error().String("workflow_id", wfID).String("task", taskName).Msg("orchestrator: exhausted CAS retries for workflow update")
}

func (o *Orchestrator) handleJobFailed(ctx context.Context, event types.JobEvent) {
	if event.JobID == "" {
		return
	}

	job, _, err := o.store.GetJob(ctx, event.JobID)
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

	o.logger.Warn().String("workflow_id", wfID).String("task", taskName).Msg("workflow task failed")

	now := time.Now()
	errStr := "task execution failed"
	if event.Error != nil {
		errStr = *event.Error
	}

	// Retry loop for CAS conflicts.
	for attempt := 0; attempt < 5; attempt++ {
		wf, rev, err := o.store.GetWorkflow(ctx, wfID)
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

		// Check if workflow-level retry is available before marking as failed.
		if wf.Definition.RetryPolicy != nil && wf.Attempt < wf.Definition.RetryPolicy.MaxAttempts {
			o.retryWorkflow(ctx, wf, rev, &now)
			return
		}

		// Mark workflow as failed.
		wf.Status = types.WorkflowStatusFailed
		wf.UpdatedAt = now
		if _, err := o.store.UpdateWorkflow(ctx, wf, rev); err != nil {
			if err == store.ErrCASConflict {
				o.logger.Debug().String("workflow_id", wfID).Int("attempt", attempt).Msg("orchestrator: CAS conflict on workflow failure, retrying")
				continue
			}
			o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save failed workflow")
			return
		}

		o.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventWorkflowFailed,
			JobID:     wfID,
			Timestamp: now,
		})

		o.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventWorkflowTaskFailed,
			JobID:     event.JobID,
			Timestamp: now,
		})

		o.logger.Info().String("workflow_id", wfID).String("failed_task", taskName).Msg("workflow failed")
		return
	}

	o.logger.Error().String("workflow_id", wfID).String("task", taskName).Msg("orchestrator: exhausted CAS retries for workflow failure update")
}

// dispatchWorkflowTask creates a job for a workflow task and dispatches it.
func (o *Orchestrator) dispatchWorkflowTask(ctx context.Context, wf *types.WorkflowInstance, taskName string, now *time.Time) {
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
		ID:           generateID(),
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

	if _, err := o.store.CreateJob(ctx, job); err != nil {
		o.logger.Error().String("workflow_id", wf.ID).String("task", taskName).Err(err).Msg("workflow orchestrator: failed to create job for task")
		return
	}

	if err := o.dispatcher.Dispatch(ctx, job, 0); err != nil {
		o.logger.Error().String("workflow_id", wf.ID).String("task", taskName).Err(err).Msg("workflow orchestrator: failed to dispatch task")
		return
	}

	// Update task state.
	if state, ok := wf.Tasks[taskName]; ok {
		state.JobID = job.ID
		state.Status = types.JobStatusRunning
		state.StartedAt = now
		wf.Tasks[taskName] = state
	}

	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowTaskDispatched,
		JobID:     job.ID,
		TaskType:  job.TaskType,
		Timestamp: *now,
	})

	o.logger.Info().String("workflow_id", wf.ID).String("task", taskName).String("job_id", job.ID).Msg("workflow task dispatched")
}

// --- Batch processing logic ---

func (o *Orchestrator) handleBatchJobCompleted(ctx context.Context, event types.JobEvent, job *types.Job) {
	batchID := *job.BatchID
	role := ""
	if job.BatchRole != nil {
		role = *job.BatchRole
	}

	batch, rev, err := o.store.GetBatch(ctx, batchID)
	if err != nil {
		o.logger.Error().String("batch_id", batchID).Err(err).Msg("batch orchestrator: failed to get batch")
		return
	}
	if batch.Status.IsTerminal() {
		return
	}

	now := time.Now()

	switch role {
	case "onetime":
		// Onetime preprocessing completed — capture result and start item dispatch.
		o.logger.Info().String("batch_id", batchID).Msg("batch onetime completed")

		if batch.OnetimeState != nil {
			batch.OnetimeState.Status = types.JobStatusCompleted
			batch.OnetimeState.FinishedAt = &now
		}

		// Capture onetime output from the result store.
		if result, err := o.store.GetResult(ctx, event.JobID); err == nil && result != nil {
			if batch.OnetimeState != nil {
				batch.OnetimeState.ResultData = result.Output
			}
		}

		o.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventBatchOnetimeCompleted,
			JobID:     batchID,
			Timestamp: now,
		})

		// Start dispatching item chunks.
		batch.Status = types.WorkflowStatusRunning
		batch.UpdatedAt = now
		o.dispatchBatchChunk(ctx, batch, &now)
		o.store.UpdateBatch(ctx, batch, rev)

	case "item":
		// Individual item completed.
		itemID := ""
		if job.BatchItem != nil {
			itemID = *job.BatchItem
		}

		o.logger.Debug().String("batch_id", batchID).String("item_id", itemID).Msg("batch item completed")

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
		if result, err := o.store.GetResult(ctx, event.JobID); err == nil && result != nil {
			itemResult.Output = result.Output
		}
		o.store.SaveBatchItemResult(ctx, itemResult)

		o.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventBatchItemCompleted,
			JobID:     batchID,
			Timestamp: now,
		})

		o.publishBatchProgress(batch)

		// Check if we need more chunks or if batch is done.
		if o.isBatchDone(batch) {
			o.finalizeBatch(ctx, batch, rev, &now)
		} else {
			// Dispatch next chunk if there's room.
			o.dispatchBatchChunk(ctx, batch, &now)
			batch.UpdatedAt = now
			o.store.UpdateBatch(ctx, batch, rev)
		}
	}
}

func (o *Orchestrator) handleBatchJobFailed(ctx context.Context, event types.JobEvent, job *types.Job) {
	batchID := *job.BatchID
	role := ""
	if job.BatchRole != nil {
		role = *job.BatchRole
	}

	batch, rev, err := o.store.GetBatch(ctx, batchID)
	if err != nil {
		o.logger.Error().String("batch_id", batchID).Err(err).Msg("batch orchestrator: failed to get batch")
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
		// Onetime failure → entire batch fails.
		o.logger.Warn().String("batch_id", batchID).Msg("batch onetime failed — failing batch")

		if batch.OnetimeState != nil {
			batch.OnetimeState.Status = types.JobStatusFailed
			batch.OnetimeState.FinishedAt = &now
			batch.OnetimeState.Error = &errStr
		}

		batch.Status = types.WorkflowStatusFailed
		batch.UpdatedAt = now
		batch.CompletedAt = &now
		o.store.UpdateBatch(ctx, batch, rev)

		o.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventBatchOnetimeFailed,
			JobID:     batchID,
			Timestamp: now,
		})
		o.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventBatchFailed,
			JobID:     batchID,
			Timestamp: now,
		})

	case "item":
		itemID := ""
		if job.BatchItem != nil {
			itemID = *job.BatchItem
		}

		o.logger.Warn().String("batch_id", batchID).String("item_id", itemID).Msg("batch item failed")

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
		o.store.SaveBatchItemResult(ctx, &types.BatchItemResult{
			BatchID: batchID,
			ItemID:  itemID,
			Success: false,
			Error:   &errStr,
		})

		o.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventBatchItemFailed,
			JobID:     batchID,
			Timestamp: now,
		})

		o.publishBatchProgress(batch)

		// Check failure policy.
		if batch.Definition.FailurePolicy == types.BatchFailFast {
			// Fail-fast: stop the batch immediately.
			batch.Status = types.WorkflowStatusFailed
			batch.UpdatedAt = now
			batch.CompletedAt = &now
			o.store.UpdateBatch(ctx, batch, rev)

			o.dispatcher.PublishEvent(types.JobEvent{
				Type:      types.EventBatchFailed,
				JobID:     batchID,
				Timestamp: now,
			})
			o.logger.Info().String("batch_id", batchID).Msg("batch failed (fail_fast)")
			return
		}

		// continue_on_error: check if batch is done.
		if o.isBatchDone(batch) {
			o.finalizeBatch(ctx, batch, rev, &now)
		} else {
			o.dispatchBatchChunk(ctx, batch, &now)
			batch.UpdatedAt = now
			o.store.UpdateBatch(ctx, batch, rev)
		}
	}
}

// dispatchBatchChunk dispatches the next chunk of batch items.
func (o *Orchestrator) dispatchBatchChunk(ctx context.Context, batch *types.BatchInstance, now *time.Time) {
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
			ID:          generateID(),
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

		if _, err := o.store.CreateJob(ctx, job); err != nil {
			o.logger.Error().String("batch_id", batch.ID).String("item_id", item.ID).Err(err).Msg("batch: failed to create item job")
			continue
		}

		if err := o.dispatcher.Dispatch(ctx, job, 0); err != nil {
			o.logger.Error().String("batch_id", batch.ID).String("item_id", item.ID).Err(err).Msg("batch: failed to dispatch item job")
			continue
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
		o.logger.Info().String("batch_id", batch.ID).Int("dispatched", dispatched).Int("running", batch.RunningItems).Int("remaining", len(items)-batch.NextChunkIndex).Msg("batch chunk dispatched")
	}
}

// isBatchDone returns true if all items have been completed or failed.
func (o *Orchestrator) isBatchDone(batch *types.BatchInstance) bool {
	return batch.CompletedItems+batch.FailedItems >= batch.TotalItems
}

// finalizeBatch marks the batch as completed or failed (with auto-retry check).
func (o *Orchestrator) finalizeBatch(ctx context.Context, batch *types.BatchInstance, rev uint64, now *time.Time) {
	batch.UpdatedAt = *now

	if batch.FailedItems > 0 {
		// Check if batch-level retry is available.
		if batch.Definition.RetryPolicy != nil && batch.Attempt < batch.Definition.RetryPolicy.MaxAttempts {
			o.retryBatch(ctx, batch, rev, now, true)
			return
		}

		batch.Status = types.WorkflowStatusFailed
		batch.CompletedAt = now
		o.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventBatchFailed,
			JobID:     batch.ID,
			Timestamp: *now,
		})
		o.logger.Info().String("batch_id", batch.ID).Int("completed", batch.CompletedItems).Int("failed", batch.FailedItems).Msg("batch finished with failures")
	} else {
		batch.Status = types.WorkflowStatusCompleted
		batch.CompletedAt = now
		o.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventBatchCompleted,
			JobID:     batch.ID,
			Timestamp: *now,
		})
		o.logger.Info().String("batch_id", batch.ID).Int("completed", batch.CompletedItems).Msg("batch completed successfully")
	}

	o.store.UpdateBatch(ctx, batch, rev)
	o.publishBatchProgress(batch)
}

// retryWorkflow resets all tasks and re-dispatches root tasks.
func (o *Orchestrator) retryWorkflow(ctx context.Context, wf *types.WorkflowInstance, rev uint64, now *time.Time) {
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

	newRev, err := o.store.UpdateWorkflow(ctx, wf, rev)
	if err != nil {
		o.logger.Error().String("workflow_id", wf.ID).Err(err).Msg("orchestrator: failed to save workflow retry reset")
		return
	}

	// Re-dispatch root tasks.
	roots := RootTasks(&wf.Definition)
	for _, name := range roots {
		o.dispatchWorkflowTask(ctx, wf, name, now)
	}

	// Save again after dispatch updates task states.
	o.store.UpdateWorkflow(ctx, wf, newRev)

	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowRetrying,
		JobID:     wf.ID,
		Attempt:   wf.Attempt,
		Timestamp: *now,
	})

	o.logger.Info().String("workflow_id", wf.ID).Int("attempt", wf.Attempt).Msg("workflow retrying")
}

// retryBatch resets failed items and re-dispatches them.
func (o *Orchestrator) retryBatch(ctx context.Context, batch *types.BatchInstance, rev uint64, now *time.Time, failedOnly bool) {
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
			// Keep non-failed items as-is.
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

	newBatchRev, err := o.store.UpdateBatch(ctx, batch, rev)
	if err != nil {
		o.logger.Error().String("batch_id", batch.ID).Err(err).Msg("orchestrator: failed to save batch retry reset")
		return
	}

	// Re-dispatch chunks.
	o.dispatchBatchChunk(ctx, batch, now)
	o.store.UpdateBatch(ctx, batch, newBatchRev)

	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventBatchRetrying,
		JobID:     batch.ID,
		Attempt:   batch.Attempt,
		Timestamp: *now,
	})

	o.logger.Info().String("batch_id", batch.ID).Int("attempt", batch.Attempt).Msg("batch retrying")
}

// handleWorkflowRetry dispatches root tasks after an API-triggered workflow retry.
func (o *Orchestrator) handleWorkflowRetry(ctx context.Context, event types.JobEvent) {
	wfID := event.JobID
	if wfID == "" {
		return
	}

	wf, rev, err := o.store.GetWorkflow(ctx, wfID)
	if err != nil || wf.Status.IsTerminal() {
		return
	}

	now := time.Now()
	roots := RootTasks(&wf.Definition)
	for _, name := range roots {
		o.dispatchWorkflowTask(ctx, wf, name, &now)
	}

	if _, err := o.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		o.logger.Warn().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save workflow after retry dispatch")
	}

	o.logger.Info().String("workflow_id", wfID).Int("roots", len(roots)).Msg("orchestrator: dispatched root tasks for workflow retry")
}

// handleBatchRetry dispatches the first chunk after an API-triggered batch retry.
func (o *Orchestrator) handleBatchRetry(ctx context.Context, event types.JobEvent) {
	batchID := event.JobID
	if batchID == "" {
		return
	}

	batch, rev, err := o.store.GetBatch(ctx, batchID)
	if err != nil || batch.Status.IsTerminal() {
		return
	}

	now := time.Now()
	o.dispatchBatchChunk(ctx, batch, &now)

	if _, err := o.store.UpdateBatch(ctx, batch, rev); err != nil {
		o.logger.Warn().String("batch_id", batchID).Err(err).Msg("orchestrator: failed to save batch after retry dispatch")
	}

	o.logger.Info().String("batch_id", batchID).Msg("orchestrator: dispatched chunk for batch retry")
}

// publishBatchProgress publishes a batch progress event.
func (o *Orchestrator) publishBatchProgress(batch *types.BatchInstance) {
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

	o.dispatcher.PublishEvent(types.JobEvent{
		Type:          types.EventBatchProgress,
		JobID:         batch.ID,
		Timestamp:     time.Now(),
		BatchProgress: progress,
	})
}

// timeoutCheckLoop periodically checks workflows and batches with deadlines.
func (o *Orchestrator) timeoutCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.checkWorkflowTimeouts(ctx)
			o.checkBatchTimeouts(ctx)
		}
	}
}

func (o *Orchestrator) checkWorkflowTimeouts(ctx context.Context) {
	workflows, err := o.store.ListWorkflows(ctx)
	if err != nil {
		return
	}

	now := time.Now()
	for _, wf := range workflows {
		if wf.Status.IsTerminal() || wf.Deadline == nil {
			continue
		}
		if now.After(*wf.Deadline) {
			o.timeoutWorkflow(ctx, wf)
		}
	}
}

func (o *Orchestrator) timeoutWorkflow(ctx context.Context, wf *types.WorkflowInstance) {
	_, rev, err := o.store.GetWorkflow(ctx, wf.ID)
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

	o.store.UpdateWorkflow(ctx, wf, rev)

	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowTimedOut,
		JobID:     wf.ID,
		Timestamp: now,
	})

	o.logger.Warn().String("workflow_id", wf.ID).Msg("workflow timed out")
}

func (o *Orchestrator) checkBatchTimeouts(ctx context.Context) {
	batches, err := o.store.ListBatches(ctx)
	if err != nil {
		return
	}

	now := time.Now()
	for _, batch := range batches {
		if batch.Status.IsTerminal() || batch.Deadline == nil {
			continue
		}
		if now.After(*batch.Deadline) {
			o.timeoutBatch(ctx, batch)
		}
	}
}

func (o *Orchestrator) timeoutBatch(ctx context.Context, batch *types.BatchInstance) {
	_, rev, err := o.store.GetBatch(ctx, batch.ID)
	if err != nil {
		return
	}

	now := time.Now()
	batch.Status = types.WorkflowStatusFailed
	batch.CompletedAt = &now
	batch.UpdatedAt = now

	o.store.UpdateBatch(ctx, batch, rev)

	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventBatchTimedOut,
		JobID:     batch.ID,
		Timestamp: now,
	})

	o.logger.Warn().String("batch_id", batch.ID).Msg("batch timed out")
}

func strPtr(s string) *string { return &s }

func generateID() string {
	return xid.New().String()
}
