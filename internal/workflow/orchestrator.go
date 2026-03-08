package workflow

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
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
				if ctx.Err() == nil {
					o.store.Client().Do(ctx, o.store.Client().B().Xack().Key(streamKey).Group(store.OrchestratorConsumerGroup).Id(entry.ID).Build())
				}
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
					if ctx.Err() == nil {
						o.store.Client().Do(ctx, o.store.Client().B().Xack().Key(streamKey).Group(store.OrchestratorConsumerGroup).Id(entry.ID).Build())
					}
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
	if err := json.Unmarshal([]byte(dataStr), &event); err != nil {
		return
	}

	switch event.Type {
	case types.EventJobCompleted:
		o.handleJobCompleted(ctx, event)
	case types.EventJobFailed, types.EventJobDead:
		o.handleJobFailed(ctx, event)
	case types.EventWorkflowCompleted:
		o.handleChildWorkflowCompleted(ctx, event)
	case types.EventWorkflowFailed:
		o.handleChildWorkflowFailed(ctx, event)
	case types.EventWorkflowSignalReceived:
		o.handleWorkflowSignal(ctx, event)
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
		} else if AnyTaskFailed(wf) {
			// No ready tasks and some task has failed — the workflow is stuck
			// because downstream tasks can never have their dependencies met.
			// Check if workflow-level retry is available.
			if wf.Definition.RetryPolicy != nil && wf.Attempt < wf.Definition.RetryPolicy.MaxAttempts {
				o.retryWorkflow(ctx, wf, newRev, &now)
				return
			}

			wf.Status = types.WorkflowStatusFailed
			wf.UpdatedAt = now
			if _, err := o.store.UpdateWorkflow(ctx, wf, newRev); err != nil {
				o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save stuck workflow as failed")
				return
			}
			o.dispatcher.PublishEvent(types.JobEvent{
				Type:      types.EventWorkflowFailed,
				JobID:     wfID,
				Timestamp: now,
			})
			o.logger.Info().String("workflow_id", wfID).Msg("workflow failed: no ready tasks and some tasks failed (stuck)")
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

	// Job is still being retried — update item state to retrying but don't fail the batch/workflow.
	if job.Status != types.JobStatusDead {
		if job.BatchID != nil {
			o.updateBatchItemRetrying(ctx, job)
		} else if job.WorkflowID != nil {
			o.updateWorkflowTaskRetrying(ctx, job)
		}
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
// If the task has a ChildWorkflowDef, it spawns a child workflow instead.
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

	// If this task has a child workflow definition, spawn a child workflow instead.
	if taskDef.ChildWorkflowDef != nil {
		o.dispatchChildWorkflow(ctx, wf, taskName, taskDef, now)
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
		o.store.DeleteJob(ctx, job.ID)
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

// dispatchChildWorkflow spawns a child workflow for a workflow task.
func (o *Orchestrator) dispatchChildWorkflow(ctx context.Context, parentWf *types.WorkflowInstance, taskName string, taskDef *types.WorkflowTask, now *time.Time) {
	childDef := *taskDef.ChildWorkflowDef

	// Initialize child task states.
	childTasks := make(map[string]types.WorkflowTaskState, len(childDef.Tasks))
	for _, t := range childDef.Tasks {
		childTasks[t.Name] = types.WorkflowTaskState{
			Status: types.JobStatusPending,
		}
	}

	childWf := &types.WorkflowInstance{
		ID:               generateID(),
		WorkflowName:     childDef.Name,
		Definition:       childDef,
		Status:           types.WorkflowStatusRunning,
		Tasks:            childTasks,
		Attempt:          1,
		ParentWorkflowID: &parentWf.ID,
		ParentTaskName:   &taskName,
		CreatedAt:        *now,
		UpdatedAt:        *now,
	}

	// Set deadline if execution timeout is configured.
	if childDef.ExecutionTimeout != nil {
		deadline := now.Add(childDef.ExecutionTimeout.Std())
		childWf.Deadline = &deadline
	}

	// Use the task payload as the child workflow input.
	if taskDef.Payload != nil {
		childWf.Input = taskDef.Payload
	}

	rev, err := o.store.SaveWorkflow(ctx, childWf)
	if err != nil {
		o.logger.Error().String("workflow_id", parentWf.ID).String("task", taskName).Err(err).Msg("orchestrator: failed to save child workflow")
		return
	}

	// Update parent task state.
	if state, ok := parentWf.Tasks[taskName]; ok {
		state.ChildWorkflowID = childWf.ID
		state.Status = types.JobStatusRunning
		state.StartedAt = now
		parentWf.Tasks[taskName] = state
	}

	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowStarted,
		JobID:     childWf.ID,
		Timestamp: *now,
	})

	// Dispatch root tasks of the child workflow.
	roots := RootTasks(&childDef)
	for _, rootName := range roots {
		o.dispatchWorkflowTask(ctx, childWf, rootName, now)
	}

	// Save child workflow after dispatching root tasks.
	o.store.UpdateWorkflow(ctx, childWf, rev)

	o.logger.Info().String("parent_workflow_id", parentWf.ID).String("task", taskName).String("child_workflow_id", childWf.ID).Msg("child workflow spawned")
}

// --- Workflow Signals ---

// handleWorkflowSignal consumes pending signals for a workflow and logs them.
// Signals are stored on the per-workflow signal stream and can be consumed by
// handlers via the client's ConsumeSignals API. The orchestrator acknowledges
// the event to ensure delivery even across leader changes.
func (o *Orchestrator) handleWorkflowSignal(ctx context.Context, event types.JobEvent) {
	wfID := event.JobID
	if wfID == "" {
		return
	}

	signals, err := o.store.ConsumeSignals(ctx, wfID)
	if err != nil {
		o.logger.Warn().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to consume signals")
		return
	}

	for _, sig := range signals {
		o.logger.Info().String("workflow_id", wfID).String("signal", sig.Name).Msg("workflow signal received")
	}
}

// --- Child workflow completion ---

// handleChildWorkflowCompleted is triggered when a workflow completes.
// If it has a parent workflow, treat the completion as a task completion in the parent.
func (o *Orchestrator) handleChildWorkflowCompleted(ctx context.Context, event types.JobEvent) {
	childWfID := event.JobID
	if childWfID == "" {
		return
	}

	childWf, _, err := o.store.GetWorkflow(ctx, childWfID)
	if err != nil || childWf.ParentWorkflowID == nil {
		return // not a child workflow
	}

	// Synthesize a job-completed-like event for the parent workflow.
	o.logger.Info().String("child_workflow_id", childWfID).String("parent_workflow_id", *childWf.ParentWorkflowID).Msg("child workflow completed — progressing parent")

	o.progressParentWorkflowTask(ctx, childWf, types.JobStatusCompleted, nil)
}

// handleChildWorkflowFailed is triggered when a workflow fails.
// If it has a parent workflow, treat the failure as a task failure in the parent.
func (o *Orchestrator) handleChildWorkflowFailed(ctx context.Context, event types.JobEvent) {
	childWfID := event.JobID
	if childWfID == "" {
		return
	}

	childWf, _, err := o.store.GetWorkflow(ctx, childWfID)
	if err != nil || childWf.ParentWorkflowID == nil {
		return // not a child workflow
	}

	errStr := "child workflow failed"
	if event.Error != nil {
		errStr = *event.Error
	}

	o.logger.Warn().String("child_workflow_id", childWfID).String("parent_workflow_id", *childWf.ParentWorkflowID).Msg("child workflow failed — failing parent task")

	o.progressParentWorkflowTask(ctx, childWf, types.JobStatusFailed, &errStr)
}

// progressParentWorkflowTask updates the parent workflow task state based on the child outcome.
func (o *Orchestrator) progressParentWorkflowTask(ctx context.Context, childWf *types.WorkflowInstance, status types.JobStatus, errStr *string) {
	parentID := *childWf.ParentWorkflowID
	taskName := ""
	if childWf.ParentTaskName != nil {
		taskName = *childWf.ParentTaskName
	}

	now := time.Now()

	for attempt := 0; attempt < 5; attempt++ {
		parentWf, rev, err := o.store.GetWorkflow(ctx, parentID)
		if err != nil || parentWf.Status.IsTerminal() {
			return
		}

		// Update the parent task state.
		if state, ok := parentWf.Tasks[taskName]; ok {
			state.Status = status
			state.FinishedAt = &now
			state.Error = errStr
			parentWf.Tasks[taskName] = state
		}

		if status == types.JobStatusCompleted {
			// Check if all parent tasks are done.
			if AllTasksCompleted(parentWf) {
				parentWf.Status = types.WorkflowStatusCompleted
				parentWf.CompletedAt = &now
				parentWf.UpdatedAt = now
				if _, err := o.store.UpdateWorkflow(ctx, parentWf, rev); err != nil {
					if err == store.ErrCASConflict {
						continue
					}
					return
				}
				o.dispatcher.PublishEvent(types.JobEvent{
					Type:      types.EventWorkflowCompleted,
					JobID:     parentID,
					Timestamp: now,
				})
				return
			}

			// Save and dispatch next ready tasks.
			parentWf.UpdatedAt = now
			newRev, err := o.store.UpdateWorkflow(ctx, parentWf, rev)
			if err != nil {
				if err == store.ErrCASConflict {
					continue
				}
				return
			}

			ready := ReadyTasks(parentWf)
			for _, name := range ready {
				o.dispatchWorkflowTask(ctx, parentWf, name, &now)
			}
			if len(ready) > 0 {
				o.store.UpdateWorkflow(ctx, parentWf, newRev)
			}
		} else {
			// Child failed — check if parent has retry policy.
			if parentWf.Definition.RetryPolicy != nil && parentWf.Attempt < parentWf.Definition.RetryPolicy.MaxAttempts {
				o.retryWorkflow(ctx, parentWf, rev, &now)
				return
			}

			parentWf.Status = types.WorkflowStatusFailed
			parentWf.UpdatedAt = now
			if _, err := o.store.UpdateWorkflow(ctx, parentWf, rev); err != nil {
				if err == store.ErrCASConflict {
					continue
				}
				return
			}
			o.dispatcher.PublishEvent(types.JobEvent{
				Type:      types.EventWorkflowFailed,
				JobID:     parentID,
				Timestamp: now,
			})
		}
		return
	}

	o.logger.Error().String("parent_workflow_id", parentID).String("task", taskName).Msg("orchestrator: exhausted CAS retries for child workflow progress")
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

// updateBatchItemRetrying updates a batch item's status to retrying so the UI
// reflects that the item is being retried rather than stuck in running.
func (o *Orchestrator) updateBatchItemRetrying(ctx context.Context, job *types.Job) {
	if job.BatchID == nil || job.BatchItem == nil {
		return
	}
	batch, rev, err := o.store.GetBatch(ctx, *job.BatchID)
	if err != nil || batch.Status.IsTerminal() {
		return
	}
	itemID := *job.BatchItem
	if state, ok := batch.ItemStates[itemID]; ok && state.Status == types.JobStatusRunning {
		state.Status = types.JobStatusRetrying
		batch.ItemStates[itemID] = state
		batch.UpdatedAt = time.Now()
		o.store.UpdateBatch(ctx, batch, rev)
		o.publishBatchProgress(batch)
	}
}

// updateWorkflowTaskRetrying updates a workflow task's status to retrying.
func (o *Orchestrator) updateWorkflowTaskRetrying(ctx context.Context, job *types.Job) {
	if job.WorkflowID == nil || job.WorkflowTask == nil {
		return
	}
	wf, rev, err := o.store.GetWorkflow(ctx, *job.WorkflowID)
	if err != nil || wf.Status.IsTerminal() {
		return
	}
	taskName := *job.WorkflowTask
	if state, ok := wf.Tasks[taskName]; ok && state.Status == types.JobStatusRunning {
		state.Status = types.JobStatusRetrying
		wf.Tasks[taskName] = state
		wf.UpdatedAt = time.Now()
		o.store.UpdateWorkflow(ctx, wf, rev)
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

		// Skip already processed items (e.g. on retry).
		if state, ok := batch.ItemStates[item.ID]; ok && state.Status != types.JobStatusPending {
			batch.NextChunkIndex++
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
			break
		}

		if err := o.dispatcher.Dispatch(ctx, job, 0); err != nil {
			o.logger.Error().String("batch_id", batch.ID).String("item_id", item.ID).Err(err).Msg("batch: failed to dispatch item job")
			o.store.DeleteJob(ctx, job.ID)
			break
		}

		batch.ItemStates[item.ID] = types.BatchItemState{
			ItemID:    item.ID,
			JobID:     job.ID,
			Status:    types.JobStatusRunning,
			StartedAt: now,
		}
		batch.NextChunkIndex++
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

// retryWorkflow resets only failed/dead/cancelled tasks and re-dispatches ready tasks.
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

	// Reset only failed/dead/cancelled tasks — keep completed tasks.
	for name, state := range wf.Tasks {
		if state.Status == types.JobStatusFailed || state.Status == types.JobStatusDead || state.Status == types.JobStatusCancelled {
			state.Status = types.JobStatusPending
			state.JobID = ""
			state.Error = nil
			state.StartedAt = nil
			state.FinishedAt = nil
			wf.Tasks[name] = state
		}
	}

	newRev, err := o.store.UpdateWorkflow(ctx, wf, rev)
	if err != nil {
		o.logger.Error().String("workflow_id", wf.ID).Err(err).Msg("orchestrator: failed to save workflow retry reset")
		return
	}

	// Dispatch tasks whose dependencies are all completed.
	ready := ReadyTasks(wf)
	for _, name := range ready {
		o.dispatchWorkflowTask(ctx, wf, name, now)
	}

	// Save again after dispatch updates task states.
	o.store.UpdateWorkflow(ctx, wf, newRev)

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

	// Re-dispatch: onetime first if needed, otherwise chunks.
	if needsBatchOnetime(batch) {
		batch.OnetimeState = nil
		o.dispatchBatchOnetime(ctx, batch, now)
	} else {
		o.dispatchBatchChunk(ctx, batch, now)
	}
	o.store.UpdateBatch(ctx, batch, newBatchRev)

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
	ready := ReadyTasks(wf)
	for _, name := range ready {
		o.dispatchWorkflowTask(ctx, wf, name, &now)
	}

	if _, err := o.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		o.logger.Warn().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save workflow after retry dispatch")
	}

	o.logger.Info().String("workflow_id", wfID).Int("ready", len(ready)).Msg("orchestrator: dispatched ready tasks for workflow retry")
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
	if needsBatchOnetime(batch) {
		batch.OnetimeState = nil
		o.dispatchBatchOnetime(ctx, batch, &now)
	} else {
		o.dispatchBatchChunk(ctx, batch, &now)
	}

	if _, err := o.store.UpdateBatch(ctx, batch, rev); err != nil {
		o.logger.Warn().String("batch_id", batchID).Err(err).Msg("orchestrator: failed to save batch after retry dispatch")
	}

	o.logger.Info().String("batch_id", batchID).Msg("orchestrator: dispatched batch retry")
}

// publishBatchProgress publishes a batch progress event.
func (o *Orchestrator) signalCancelActiveRuns(ctx context.Context, jobID string) {
	runs, _ := o.store.ListActiveRunsByJobID(ctx, jobID)
	for _, run := range runs {
		o.store.Client().Do(ctx, o.store.Client().B().Publish().Channel(o.store.CancelChannel()).Message(run.ID).Build())
	}
}

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
	workflows, err := o.store.ListWorkflowsByStatus(ctx, types.WorkflowStatusRunning)
	if err != nil {
		return
	}

	now := time.Now()
	for _, wf := range workflows {
		if wf.Deadline == nil {
			continue
		}
		if now.After(*wf.Deadline) {
			o.timeoutWorkflow(ctx, wf)
		}
	}
}

func (o *Orchestrator) timeoutWorkflow(ctx context.Context, wf *types.WorkflowInstance) {
	freshWf, rev, err := o.store.GetWorkflow(ctx, wf.ID)
	if err != nil {
		return
	}
	wf = freshWf
	if wf.Status.IsTerminal() {
		return
	}

	now := time.Now()
	wf.Status = types.WorkflowStatusFailed
	wf.CompletedAt = &now
	wf.UpdatedAt = now

	// Cancel all non-terminal tasks and signal running handlers.
	for name, state := range wf.Tasks {
		if !state.Status.IsTerminal() {
			if state.JobID != "" {
				o.signalCancelActiveRuns(ctx, state.JobID)
			}
			// Cancel child workflows spawned by this task.
			if state.ChildWorkflowID != "" {
				o.cancelChildWorkflow(ctx, state.ChildWorkflowID)
			}
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
	batches, err := o.store.ListBatchesByStatus(ctx, types.WorkflowStatusRunning)
	if err != nil {
		return
	}

	now := time.Now()
	for _, batch := range batches {
		if batch.Deadline == nil {
			continue
		}
		if now.After(*batch.Deadline) {
			o.timeoutBatch(ctx, batch)
		}
	}
}

func (o *Orchestrator) timeoutBatch(ctx context.Context, batch *types.BatchInstance) {
	freshBatch, rev, err := o.store.GetBatch(ctx, batch.ID)
	if err != nil {
		return
	}
	batch = freshBatch
	if batch.Status.IsTerminal() {
		return
	}

	now := time.Now()
	batch.Status = types.WorkflowStatusFailed
	batch.CompletedAt = &now
	batch.UpdatedAt = now

	// Signal cancel to running child jobs (items + onetime).
	for _, state := range batch.ItemStates {
		if !state.Status.IsTerminal() && state.JobID != "" {
			o.signalCancelActiveRuns(ctx, state.JobID)
		}
	}
	if batch.OnetimeState != nil && !batch.OnetimeState.Status.IsTerminal() && batch.OnetimeState.JobID != "" {
		o.signalCancelActiveRuns(ctx, batch.OnetimeState.JobID)
	}

	o.store.UpdateBatch(ctx, batch, rev)

	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventBatchTimedOut,
		JobID:     batch.ID,
		Timestamp: now,
	})

	o.logger.Warn().String("batch_id", batch.ID).Msg("batch timed out")
}

// dispatchBatchOnetime creates and dispatches the onetime preprocessing job for a batch.
func (o *Orchestrator) dispatchBatchOnetime(ctx context.Context, batch *types.BatchInstance, now *time.Time) {
	job := &types.Job{
		ID:        generateID(),
		TaskType:  *batch.Definition.OnetimeTaskType,
		Payload:   batch.Definition.OnetimePayload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		Status:    types.JobStatusPending,
		Priority:  batch.Definition.DefaultPriority,
		BatchID:   &batch.ID,
		BatchRole: strPtr("onetime"),
		CreatedAt: *now,
		UpdatedAt: *now,
	}

	if _, err := o.store.CreateJob(ctx, job); err != nil {
		o.logger.Error().String("batch_id", batch.ID).Err(err).Msg("batch: failed to create onetime job")
		return
	}
	if err := o.dispatcher.Dispatch(ctx, job, 0); err != nil {
		o.logger.Error().String("batch_id", batch.ID).Err(err).Msg("batch: failed to dispatch onetime job")
		o.store.DeleteJob(ctx, job.ID)
		return
	}

	batch.OnetimeState = &types.BatchOnetimeState{
		JobID:     job.ID,
		Status:    types.JobStatusRunning,
		StartedAt: now,
	}
}

// needsBatchOnetime returns true if the batch has a onetime task that hasn't completed yet.
func needsBatchOnetime(batch *types.BatchInstance) bool {
	return batch.Definition.OnetimeTaskType != nil && *batch.Definition.OnetimeTaskType != "" &&
		(batch.OnetimeState == nil || batch.OnetimeState.Status != types.JobStatusCompleted)
}

// cancelChildWorkflow cancels a child workflow and all its tasks.
func (o *Orchestrator) cancelChildWorkflow(ctx context.Context, childWfID string) {
	childWf, rev, err := o.store.GetWorkflow(ctx, childWfID)
	if err != nil || childWf.Status.IsTerminal() {
		return
	}

	now := time.Now()
	childWf.Status = types.WorkflowStatusCancelled
	childWf.CompletedAt = &now
	childWf.UpdatedAt = now

	for name, state := range childWf.Tasks {
		if !state.Status.IsTerminal() {
			if state.JobID != "" {
				o.signalCancelActiveRuns(ctx, state.JobID)
			}
			if state.ChildWorkflowID != "" {
				o.cancelChildWorkflow(ctx, state.ChildWorkflowID)
			}
			state.Status = types.JobStatusCancelled
			state.FinishedAt = &now
			childWf.Tasks[name] = state
		}
	}

	o.store.UpdateWorkflow(ctx, childWf, rev)
	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowCancelled,
		JobID:     childWfID,
		Timestamp: now,
	})
}

func strPtr(s string) *string { return &s }

func generateID() string {
	return xid.New().String()
}
