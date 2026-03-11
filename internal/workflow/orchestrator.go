package workflow

import (
	"context"
	"encoding/json"
	"fmt"
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
				if err := o.processStreamEvent(ctx, entry); err != nil {
					o.logger.Warn().Err(err).String("stream_id", entry.ID).Msg("orchestrator: event processing failed, leaving for retry")
					continue
				}
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
					if err := o.processStreamEvent(ctx, entry); err != nil {
						o.logger.Warn().Err(err).String("stream_id", entry.ID).Msg("orchestrator: pending event processing failed, leaving for retry")
						continue
					}
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
// Returns an error if the event could not be processed and should be retried.
func (o *Orchestrator) processStreamEvent(ctx context.Context, entry rueidis.XRangeEntry) error {
	dataStr, ok := entry.FieldValues["data"]
	if !ok {
		return nil // malformed message, ACK it to avoid infinite loop
	}

	var event types.JobEvent
	if err := json.Unmarshal([]byte(dataStr), &event); err != nil {
		return nil // unparseable message, ACK it
	}

	var handlerErr error
	switch event.Type {
	case types.EventJobCompleted:
		handlerErr = o.handleJobCompleted(ctx, event)
	case types.EventJobFailed, types.EventJobDead:
		handlerErr = o.handleJobFailed(ctx, event)
	case types.EventWorkflowCompleted:
		handlerErr = o.handleChildWorkflowCompleted(ctx, event)
	case types.EventWorkflowFailed:
		handlerErr = o.handleChildWorkflowFailed(ctx, event)
	case types.EventWorkflowSignalReceived:
		o.handleWorkflowSignal(ctx, event)
	case types.EventWorkflowResumed:
		o.handleWorkflowResumed(ctx, event)
	case types.EventWorkflowRetrying:
		handlerErr = o.handleWorkflowRetry(ctx, event)
	case types.EventBatchRetrying:
		handlerErr = o.handleBatchRetry(ctx, event)
	}
	return handlerErr
}

func (o *Orchestrator) handleJobCompleted(ctx context.Context, event types.JobEvent) error {
	if event.JobID == "" {
		return nil
	}

	// Load the job to check if it's part of a workflow or batch.
	job, _, err := o.store.GetJob(ctx, event.JobID)
	if err != nil {
		return err
	}

	// Route to batch handler if this is a batch job.
	if job.BatchID != nil {
		return o.handleBatchJobCompleted(ctx, event, job)
	}

	if job.WorkflowID == nil {
		return nil
	}

	wfID := *job.WorkflowID
	taskName := ""
	if job.WorkflowTask != nil {
		taskName = *job.WorkflowTask
	}

	// Check if this is a hook job — if so, update hook state only.
	if hookName, ok := isHookJob(taskName); ok {
		o.handleHookCompleted(ctx, wfID, hookName, event.JobID)
		return nil
	}

	o.logger.Info().String("workflow_id", wfID).String("task", taskName).Msg("workflow task completed")

	now := time.Now()

	// Publish task completion event (idempotent, safe to call once).
	o.dispatcher.PublishEvent(types.JobEvent{
		Type:       types.EventWorkflowTaskCompleted,
		JobID:      event.JobID,
		WorkflowID: wfID,
		Timestamp:  now,
	})

	// Retry loop: re-read workflow on CAS conflict to pick up concurrent changes.
	for attempt := 0; attempt < 5; attempt++ {
		wf, rev, err := o.store.GetWorkflow(ctx, wfID)
		if err != nil {
			o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to get workflow")
			return err
		}
		if wf.Status.IsTerminal() {
			return nil
		}

		// Find task definition.
		var taskDef *types.WorkflowTask
		for i := range wf.Definition.Tasks {
			if wf.Definition.Tasks[i].Name == taskName {
				taskDef = &wf.Definition.Tasks[i]
				break
			}
		}

		// Mark task as completed.
		if state, ok := wf.Tasks[taskName]; ok {
			state.Status = types.JobStatusCompleted
			state.FinishedAt = &now
			wf.Tasks[taskName] = state
		}

		// Handle condition node: read route from job result.
		if taskDef != nil && taskDef.Type == types.WorkflowTaskCondition {
			o.processConditionResult(ctx, wf, taskName, taskDef, event.JobID, &now)
		}

		// Handle subflow node: read generated tasks from job result.
		if taskDef != nil && taskDef.Type == types.WorkflowTaskSubflow {
			o.processSubflowResult(ctx, wf, taskName, event.JobID, &now)
		}

		// Update reference counting.
		DecrementPendingDeps(wf, taskName)

		// Check if all tasks are done.
		if AllTasksCompleted(wf) {
			wf.Status = types.WorkflowStatusCompleted
			wf.CompletedAt = &now
			wf.UpdatedAt = now
			o.setWorkflowOutput(ctx, wf)
			// Dispatch lifecycle hooks before saving.
			if wf.Definition.Hooks != nil {
				o.dispatchWorkflowHook(ctx, wf, "on_success", wf.Definition.Hooks.OnSuccess, &now)
				o.dispatchWorkflowHook(ctx, wf, "on_exit", wf.Definition.Hooks.OnExit, &now)
			}
			if _, err := o.store.UpdateWorkflow(ctx, wf, rev); err != nil {
				if err == store.ErrCASConflict {
					o.logger.Debug().String("workflow_id", wfID).Int("attempt", attempt).Msg("orchestrator: CAS conflict on workflow completion, retrying")
					continue
				}
				o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save completed workflow")
				return err
			}
			o.dispatcher.PublishEvent(types.JobEvent{
				Type:      types.EventWorkflowCompleted,
				JobID:     wfID,
				Timestamp: now,
			})
			o.logger.Info().String("workflow_id", wfID).Msg("workflow completed")
			return nil
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
			return err
		}

		// If workflow is suspended, don't dispatch any new tasks.
		if wf.Status == types.WorkflowStatusSuspended {
			o.logger.Info().String("workflow_id", wfID).String("task", taskName).Msg("workflow is suspended, not dispatching ready tasks")
			return nil
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
		} else if AnyTaskFatallyFailed(wf) {
			// No ready tasks and some task has fatally failed — the workflow is stuck
			// because downstream tasks can never have their dependencies met.
			// Check if workflow-level retry is available.
			if wf.Definition.RetryPolicy != nil && wf.Attempt < wf.Definition.RetryPolicy.MaxAttempts {
				o.retryWorkflow(ctx, wf, newRev, &now)
				return nil
			}

			wf.Status = types.WorkflowStatusFailed
			wf.UpdatedAt = now
			// Dispatch lifecycle hooks before saving.
			if wf.Definition.Hooks != nil {
				o.dispatchWorkflowHook(ctx, wf, "on_failure", wf.Definition.Hooks.OnFailure, &now)
				o.dispatchWorkflowHook(ctx, wf, "on_exit", wf.Definition.Hooks.OnExit, &now)
			}
			if _, err := o.store.UpdateWorkflow(ctx, wf, newRev); err != nil {
				o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save stuck workflow as failed")
				return err
			}
			o.dispatcher.PublishEvent(types.JobEvent{
				Type:      types.EventWorkflowFailed,
				JobID:     wfID,
				Timestamp: now,
			})
			o.logger.Info().String("workflow_id", wfID).Msg("workflow failed: no ready tasks and some tasks failed (stuck)")
		}
		return nil
	}

	o.logger.Error().String("workflow_id", wfID).String("task", taskName).Msg("orchestrator: exhausted CAS retries for workflow update")
	return fmt.Errorf("exhausted CAS retries for workflow %s task %s", wfID, taskName)
}

// processConditionResult reads the condition handler's route selection from the job result
// and stores it in the task state. The route determines which branch is activated.
func (o *Orchestrator) processConditionResult(ctx context.Context, wf *types.WorkflowInstance, taskName string, taskDef *types.WorkflowTask, jobID string, now *time.Time) {
	// Check iteration limit.
	if wf.ConditionIterations == nil {
		wf.ConditionIterations = make(map[string]int)
	}
	wf.ConditionIterations[taskName]++
	maxIter := MaxIterationsForTask(taskDef)
	if wf.ConditionIterations[taskName] > maxIter {
		errMsg := fmt.Sprintf("condition %q exceeded max iterations (%d)", taskName, maxIter)
		o.logger.Error().String("workflow_id", wf.ID).String("task", taskName).Int("iterations", wf.ConditionIterations[taskName]).Msg(errMsg)
		state := wf.Tasks[taskName]
		state.Status = types.JobStatusFailed
		state.Error = &errMsg
		wf.Tasks[taskName] = state
		return
	}

	result, err := o.store.GetResult(ctx, jobID)
	if err != nil || result == nil || result.Output == nil {
		o.logger.Warn().String("workflow_id", wf.ID).String("task", taskName).Msg("orchestrator: condition node completed but no result available")
		return
	}

	var condResult types.ConditionResult
	if err := json.Unmarshal(result.Output, &condResult); err != nil {
		o.logger.Error().String("workflow_id", wf.ID).String("task", taskName).Err(err).Msg("orchestrator: failed to parse condition result")
		return
	}

	state := wf.Tasks[taskName]
	state.ConditionRoute = &condResult.Route
	wf.Tasks[taskName] = state

	targetName, ok := taskDef.ConditionRoutes[condResult.Route]
	if !ok {
		o.logger.Error().String("workflow_id", wf.ID).String("task", taskName).Int("route", int(condResult.Route)).Msg("orchestrator: condition returned unknown route")
		return
	}

	o.logger.Info().String("workflow_id", wf.ID).String("task", taskName).Int("route", int(condResult.Route)).String("target", targetName).Msg("condition evaluated")

	// Loop-back handling: if the target task was already completed (cycle via condition),
	// reset the entire loop body (target → ... → this condition) to pending.
	if targetState, exists := wf.Tasks[targetName]; exists && targetState.Status == types.JobStatusCompleted {
		// Reset the target task.
		targetState.Status = types.JobStatusPending
		targetState.JobID = ""
		targetState.Error = nil
		targetState.StartedAt = nil
		targetState.FinishedAt = nil
		wf.Tasks[targetName] = targetState
		if wf.PendingDeps != nil {
			wf.PendingDeps[targetName] = 0
		}

		// Reset the condition node itself so it re-evaluates after the loop body completes.
		condState := wf.Tasks[taskName]
		condState.Status = types.JobStatusPending
		condState.JobID = ""
		condState.Error = nil
		condState.ConditionRoute = nil
		condState.StartedAt = nil
		condState.FinishedAt = nil
		wf.Tasks[taskName] = condState

		// Find the condition node's dependency count from definition and restore it.
		if wf.PendingDeps != nil {
			if condDef := findTaskDef(&wf.Definition, taskName); condDef != nil {
				wf.PendingDeps[taskName] = len(condDef.DependsOn)
			}
		}

		// Also reset any intermediate tasks between the target and the condition node.
		// Walk the dependency chain: find tasks that are in the path from target to condition.
		resetLoopBody(wf, targetName, taskName)
	}

	// Skip non-selected branches for simple if-else branching (no loops).
	// Only apply skip logic when no route creates a back-edge (loop).
	hasBackEdge := false
	for _, routeTarget := range taskDef.ConditionRoutes {
		if isBackEdge(&wf.Definition, taskName, routeTarget) {
			hasBackEdge = true
			break
		}
	}

	if !hasBackEdge {
		for route, otherTarget := range taskDef.ConditionRoutes {
			if route == condResult.Route {
				continue
			}
			if otherState, exists := wf.Tasks[otherTarget]; exists && otherState.Status == types.JobStatusPending {
				otherState.Status = types.JobStatusCompleted
				otherState.FinishedAt = now
				wf.Tasks[otherTarget] = otherState

				// Decrement PendingDeps for tasks depending on the skipped branch.
				DecrementPendingDeps(wf, otherTarget)
			}
		}
	}
}

// processSubflowResult reads dynamically generated tasks from the subflow handler result
// and injects them into the workflow instance.
func (o *Orchestrator) processSubflowResult(ctx context.Context, wf *types.WorkflowInstance, taskName string, jobID string, now *time.Time) {
	result, err := o.store.GetResult(ctx, jobID)
	if err != nil || result == nil || result.Output == nil {
		o.logger.Warn().String("workflow_id", wf.ID).String("task", taskName).Msg("orchestrator: subflow node completed but no result available")
		return
	}

	var subResult types.SubflowResult
	if err := json.Unmarshal(result.Output, &subResult); err != nil {
		o.logger.Error().String("workflow_id", wf.ID).String("task", taskName).Err(err).Msg("orchestrator: failed to parse subflow result")
		return
	}

	if len(subResult.Tasks) == 0 {
		return
	}

	// Check for name collisions with existing tasks.
	for _, newTask := range subResult.Tasks {
		if _, exists := wf.Tasks[newTask.Name]; exists {
			o.logger.Error().String("workflow_id", wf.ID).String("task", taskName).String("collision", newTask.Name).Msg("orchestrator: subflow task name collides with existing task")
			return
		}
	}

	// Inject subflow tasks: add to definition and initialize runtime state.
	injectedNames := make([]string, 0, len(subResult.Tasks))
	for _, newTask := range subResult.Tasks {
		// Ensure subflow tasks depend on the subflow generator.
		hasDep := false
		for _, dep := range newTask.DependsOn {
			if dep == taskName {
				hasDep = true
				break
			}
		}
		if !hasDep {
			newTask.DependsOn = append([]string{taskName}, newTask.DependsOn...)
		}

		wf.Definition.Tasks = append(wf.Definition.Tasks, newTask)
		wf.Tasks[newTask.Name] = types.WorkflowTaskState{
			Name:   newTask.Name,
			Status: types.JobStatusPending,
		}
		// Initialize reference count for the new task.
		if wf.PendingDeps != nil {
			wf.PendingDeps[newTask.Name] = len(newTask.DependsOn)
			// The subflow generator is already completed, so decrement its count.
			if wf.PendingDeps[newTask.Name] > 0 {
				wf.PendingDeps[newTask.Name]--
			}
		}
		injectedNames = append(injectedNames, newTask.Name)
	}

	// Update the subflow generator's state to track injected tasks.
	state := wf.Tasks[taskName]
	state.SubflowTasks = injectedNames
	wf.Tasks[taskName] = state

	// Update downstream tasks: any task that depends on the subflow generator
	// should also wait for the injected tasks. Add the injected tasks as additional
	// dependencies so downstream tasks won't be dispatched prematurely.
	for i := range wf.Definition.Tasks {
		t := &wf.Definition.Tasks[i]
		dependsOnGenerator := false
		for _, dep := range t.DependsOn {
			if dep == taskName {
				dependsOnGenerator = true
				break
			}
		}
		if dependsOnGenerator && t.Name != taskName {
			// Skip injected tasks themselves (they already depend on the generator).
			isInjected := false
			for _, injName := range injectedNames {
				if t.Name == injName {
					isInjected = true
					break
				}
			}
			if !isInjected {
				t.DependsOn = append(t.DependsOn, injectedNames...)
				if wf.PendingDeps != nil {
					wf.PendingDeps[t.Name] += len(injectedNames)
				}
			}
		}
	}

	o.logger.Info().String("workflow_id", wf.ID).String("task", taskName).Int("injected", len(injectedNames)).Msg("subflow tasks injected")
}

// findTaskDef looks up a WorkflowTask by name in the definition.
func findTaskDef(def *types.WorkflowDefinition, name string) *types.WorkflowTask {
	for i := range def.Tasks {
		if def.Tasks[i].Name == name {
			return &def.Tasks[i]
		}
	}
	return nil
}

func (o *Orchestrator) handleJobFailed(ctx context.Context, event types.JobEvent) error {
	if event.JobID == "" {
		return nil
	}

	job, _, err := o.store.GetJob(ctx, event.JobID)
	if err != nil {
		return err
	}

	// Job is still being retried — update item state to retrying but don't fail the batch/workflow.
	if job.Status != types.JobStatusDead {
		if job.BatchID != nil {
			o.updateBatchItemRetrying(ctx, job)
		} else if job.WorkflowID != nil {
			o.updateWorkflowTaskRetrying(ctx, job)
		}
		return nil
	}

	// Route to batch handler if this is a batch job.
	if job.BatchID != nil {
		return o.handleBatchJobFailed(ctx, event, job)
	}

	if job.WorkflowID == nil {
		return nil
	}

	wfID := *job.WorkflowID
	taskName := ""
	if job.WorkflowTask != nil {
		taskName = *job.WorkflowTask
	}

	// Check if this is a hook job — if so, update hook state only.
	if hookName, ok := isHookJob(taskName); ok {
		o.handleHookFailed(ctx, wfID, hookName, event.JobID)
		return nil
	}

	o.logger.Warn().String("workflow_id", wfID).String("task", taskName).Msg("workflow task failed")

	now := time.Now()
	errStr := "task execution failed"
	if event.Error != nil {
		errStr = *event.Error
	}

	// Check if this task has AllowFailure set.
	isAllowFailure := false

	// Retry loop for CAS conflicts.
	for attempt := 0; attempt < 5; attempt++ {
		wf, rev, err := o.store.GetWorkflow(ctx, wfID)
		if err != nil {
			return err
		}
		if wf.Status.IsTerminal() {
			return nil
		}

		// Mark the failed task.
		if state, ok := wf.Tasks[taskName]; ok {
			state.Status = types.JobStatusFailed
			state.FinishedAt = &now
			state.Error = &errStr
			wf.Tasks[taskName] = state
		}

		// Check if this task has AllowFailure=true.
		if taskDef := findTaskDef(&wf.Definition, taskName); taskDef != nil && taskDef.AllowFailure {
			isAllowFailure = true
		}

		// If AllowFailure, treat as "completed" for dependency resolution:
		// decrement dependents' PendingDeps so downstream tasks can proceed.
		if isAllowFailure {
			DecrementPendingDeps(wf, taskName)

			// Check if all tasks are done (AllowFailure failures count as done).
			if AllTasksCompleted(wf) {
				wf.Status = types.WorkflowStatusCompleted
				wf.CompletedAt = &now
				wf.UpdatedAt = now
				o.setWorkflowOutput(ctx, wf)
				if _, err := o.store.UpdateWorkflow(ctx, wf, rev); err != nil {
					if err == store.ErrCASConflict {
						continue
					}
					o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save completed workflow (allow-failure)")
					return err
				}
				o.dispatcher.PublishEvent(types.JobEvent{
					Type:      types.EventWorkflowCompleted,
					JobID:     wfID,
					Timestamp: now,
				})
				o.logger.Info().String("workflow_id", wfID).Msg("workflow completed (with allowed failures)")
				return nil
			}

			// Save task failure and dispatch next ready tasks.
			wf.Status = types.WorkflowStatusRunning
			wf.UpdatedAt = now
			newRev, err := o.store.UpdateWorkflow(ctx, wf, rev)
			if err != nil {
				if err == store.ErrCASConflict {
					continue
				}
				o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save allow-failure task state")
				return err
			}

			// If workflow is suspended, don't dispatch any new tasks.
			if wf.Status != types.WorkflowStatusSuspended {
				ready := ReadyTasks(wf)
				if len(ready) > 0 {
					for _, name := range ready {
						o.dispatchWorkflowTask(ctx, wf, name, &now)
					}
					o.store.UpdateWorkflow(ctx, wf, newRev)
				}
			}

			o.dispatcher.PublishEvent(types.JobEvent{
				Type:       types.EventWorkflowTaskFailed,
				JobID:      event.JobID,
				WorkflowID: wfID,
				Timestamp:  now,
			})
			o.logger.Info().String("workflow_id", wfID).String("task", taskName).Msg("task failed (allow_failure=true), workflow continues")
			return nil
		}

		// Check if workflow-level retry is available before marking as failed.
		if wf.Definition.RetryPolicy != nil && wf.Attempt < wf.Definition.RetryPolicy.MaxAttempts {
			o.retryWorkflow(ctx, wf, rev, &now)
			return nil
		}

		// Mark workflow as failed.
		wf.Status = types.WorkflowStatusFailed
		wf.UpdatedAt = now
		// Dispatch lifecycle hooks before saving.
		if wf.Definition.Hooks != nil {
			o.dispatchWorkflowHook(ctx, wf, "on_failure", wf.Definition.Hooks.OnFailure, &now)
			o.dispatchWorkflowHook(ctx, wf, "on_exit", wf.Definition.Hooks.OnExit, &now)
		}
		if _, err := o.store.UpdateWorkflow(ctx, wf, rev); err != nil {
			if err == store.ErrCASConflict {
				o.logger.Debug().String("workflow_id", wfID).Int("attempt", attempt).Msg("orchestrator: CAS conflict on workflow failure, retrying")
				continue
			}
			o.logger.Error().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save failed workflow")
			return err
		}

		o.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventWorkflowFailed,
			JobID:     wfID,
			Timestamp: now,
		})

		o.dispatcher.PublishEvent(types.JobEvent{
			Type:       types.EventWorkflowTaskFailed,
			JobID:      event.JobID,
			WorkflowID: wfID,
			Timestamp:  now,
		})

		o.logger.Info().String("workflow_id", wfID).String("failed_task", taskName).Msg("workflow failed")
		return nil
	}

	o.logger.Error().String("workflow_id", wfID).String("task", taskName).Msg("orchestrator: exhausted CAS retries for workflow failure update")
	return fmt.Errorf("exhausted CAS retries for workflow %s task %s failure", wfID, taskName)
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

	// Evaluate preconditions before dispatching.
	if len(taskDef.Preconditions) > 0 {
		if reason := o.evaluatePreconditions(ctx, wf, taskDef); reason != "" {
			// Skip this task: mark as completed with Skipped=true.
			if state, ok := wf.Tasks[taskName]; ok {
				state.Status = types.JobStatusCompleted
				state.FinishedAt = now
				state.Skipped = true
				state.SkipReason = reason
				wf.Tasks[taskName] = state
			}
			DecrementPendingDeps(wf, taskName)
			o.logger.Info().String("workflow_id", wf.ID).String("task", taskName).String("reason", reason).Msg("task skipped due to precondition")
			// Dispatch newly ready tasks.
			ready := ReadyTasks(wf)
			for _, name := range ready {
				o.dispatchWorkflowTask(ctx, wf, name, now)
			}
			return
		}
	}

	// If this task has a child workflow definition, spawn a child workflow instead.
	if taskDef.ChildWorkflowDef != nil {
		o.dispatchChildWorkflow(ctx, wf, taskName, taskDef, now)
		return
	}

	// Resolve payload: if ResultFrom is set, inject upstream task's result.
	payload := taskDef.Payload
	if taskDef.ResultFrom != "" {
		payload = o.resolveResultFromPayload(ctx, wf, taskDef)
	}

	// Create a job for this workflow task.
	job := &types.Job{
		ID:           generateID(),
		TaskType:     taskDef.TaskType,
		Payload:      payload,
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
		Type:       types.EventWorkflowTaskDispatched,
		JobID:      job.ID,
		WorkflowID: wf.ID,
		TaskType:   job.TaskType,
		Timestamp:  *now,
	})

	o.logger.Info().String("workflow_id", wf.ID).String("task", taskName).String("job_id", job.ID).Msg("workflow task dispatched")
}

// resolveResultFromPayload builds the payload for a task that has ResultFrom set.
// It wraps the upstream result and original payload into an UpstreamPayload envelope.
func (o *Orchestrator) resolveResultFromPayload(ctx context.Context, wf *types.WorkflowInstance, taskDef *types.WorkflowTask) json.RawMessage {
	upstreamState, ok := wf.Tasks[taskDef.ResultFrom]
	if !ok || upstreamState.JobID == "" {
		o.logger.Warn().String("workflow_id", wf.ID).String("task", taskDef.Name).String("result_from", taskDef.ResultFrom).Msg("ResultFrom: upstream task has no job ID, using original payload")
		return taskDef.Payload
	}

	result, err := o.store.GetResult(ctx, upstreamState.JobID)
	if err != nil || result == nil || result.Output == nil {
		o.logger.Warn().String("workflow_id", wf.ID).String("task", taskDef.Name).String("result_from", taskDef.ResultFrom).Msg("ResultFrom: upstream result not found, using original payload")
		return taskDef.Payload
	}

	// Build UpstreamPayload envelope: {"upstream": <result>, "original": <payload>}
	envelope := map[string]json.RawMessage{
		"upstream": result.Output,
		"original": taskDef.Payload,
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		o.logger.Warn().String("workflow_id", wf.ID).String("task", taskDef.Name).Err(err).Msg("ResultFrom: failed to marshal envelope, using original payload")
		return taskDef.Payload
	}
	return data
}

// setWorkflowOutput finds the leaf task(s) and copies the last completed leaf's
// result into the workflow's Output field.
func (o *Orchestrator) setWorkflowOutput(ctx context.Context, wf *types.WorkflowInstance) {
	// Build set of tasks that are depended upon.
	depended := make(map[string]bool)
	for _, t := range wf.Definition.Tasks {
		for _, dep := range t.DependsOn {
			depended[dep] = true
		}
	}

	// Find leaf tasks (not depended upon by anyone).
	var leafJobID string
	for _, t := range wf.Definition.Tasks {
		if !depended[t.Name] {
			if state, ok := wf.Tasks[t.Name]; ok && state.Status == types.JobStatusCompleted && state.JobID != "" {
				leafJobID = state.JobID
			}
		}
	}

	if leafJobID == "" {
		return
	}

	result, err := o.store.GetResult(ctx, leafJobID)
	if err != nil || result == nil || result.Output == nil {
		return
	}
	wf.Output = result.Output
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

	// Initialize reference counting for the child workflow.
	InitPendingDeps(childWf)

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

// handleWorkflowSignal is triggered when a signal is sent to a workflow.
// Signals are stored on the per-workflow signal stream and are consumed by
// user handlers via the client's ConsumeSignals API. The orchestrator does NOT
// consume (XDEL) them — that is the responsibility of user code.
func (o *Orchestrator) handleWorkflowSignal(ctx context.Context, event types.JobEvent) {
	wfID := event.JobID
	if wfID == "" {
		return
	}

	o.logger.Info().String("workflow_id", wfID).Msg("workflow signal event received")
}

// --- Child workflow completion ---

// handleChildWorkflowCompleted is triggered when a workflow completes.
// If it has a parent workflow, treat the completion as a task completion in the parent.
func (o *Orchestrator) handleChildWorkflowCompleted(ctx context.Context, event types.JobEvent) error {
	childWfID := event.JobID
	if childWfID == "" {
		return nil
	}

	childWf, _, err := o.store.GetWorkflow(ctx, childWfID)
	if err != nil {
		return err
	}
	if childWf.ParentWorkflowID == nil {
		return nil // not a child workflow
	}

	// Synthesize a job-completed-like event for the parent workflow.
	o.logger.Info().String("child_workflow_id", childWfID).String("parent_workflow_id", *childWf.ParentWorkflowID).Msg("child workflow completed — progressing parent")

	o.progressParentWorkflowTask(ctx, childWf, types.JobStatusCompleted, nil)
	return nil
}

// handleChildWorkflowFailed is triggered when a workflow fails.
// If it has a parent workflow, treat the failure as a task failure in the parent.
func (o *Orchestrator) handleChildWorkflowFailed(ctx context.Context, event types.JobEvent) error {
	childWfID := event.JobID
	if childWfID == "" {
		return nil
	}

	childWf, _, err := o.store.GetWorkflow(ctx, childWfID)
	if err != nil {
		return err
	}
	if childWf.ParentWorkflowID == nil {
		return nil // not a child workflow
	}

	errStr := "child workflow failed"
	if event.Error != nil {
		errStr = *event.Error
	}

	o.logger.Warn().String("child_workflow_id", childWfID).String("parent_workflow_id", *childWf.ParentWorkflowID).Msg("child workflow failed — failing parent task")

	o.progressParentWorkflowTask(ctx, childWf, types.JobStatusFailed, &errStr)
	return nil
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

func (o *Orchestrator) handleBatchJobCompleted(ctx context.Context, event types.JobEvent, job *types.Job) error {
	batchID := *job.BatchID
	role := ""
	if job.BatchRole != nil {
		role = *job.BatchRole
	}

	batch, rev, err := o.store.GetBatch(ctx, batchID)
	if err != nil {
		o.logger.Error().String("batch_id", batchID).Err(err).Msg("batch orchestrator: failed to get batch")
		return err
	}
	if batch.Status.IsTerminal() {
		return nil
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
	return nil
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

func (o *Orchestrator) handleBatchJobFailed(ctx context.Context, event types.JobEvent, job *types.Job) error {
	batchID := *job.BatchID
	role := ""
	if job.BatchRole != nil {
		role = *job.BatchRole
	}

	batch, rev, err := o.store.GetBatch(ctx, batchID)
	if err != nil {
		o.logger.Error().String("batch_id", batchID).Err(err).Msg("batch orchestrator: failed to get batch")
		return err
	}
	if batch.Status.IsTerminal() {
		return nil
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
			return nil
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
	return nil
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
			state.ConditionRoute = nil
			state.SubflowTasks = nil
			wf.Tasks[name] = state
		}
	}

	// Re-initialize reference counting after retry reset.
	InitPendingDeps(wf)
	for name, state := range wf.Tasks {
		if state.Status == types.JobStatusCompleted {
			DecrementPendingDeps(wf, name)
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

// handleWorkflowResumed dispatches ready tasks after a workflow is resumed from suspended state.
func (o *Orchestrator) handleWorkflowResumed(ctx context.Context, event types.JobEvent) {
	wfID := event.JobID
	if wfID == "" {
		return
	}

	wf, rev, err := o.store.GetWorkflow(ctx, wfID)
	if err != nil || wf.Status.IsTerminal() || wf.Status == types.WorkflowStatusSuspended {
		return
	}

	now := time.Now()
	ready := ReadyTasks(wf)
	for _, name := range ready {
		o.dispatchWorkflowTask(ctx, wf, name, &now)
	}

	if _, err := o.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		o.logger.Warn().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save workflow after resume dispatch")
	}

	o.logger.Info().String("workflow_id", wfID).Int("ready", len(ready)).Msg("orchestrator: dispatched ready tasks for resumed workflow")
}

// handleWorkflowRetry dispatches root tasks after an API-triggered workflow retry.
func (o *Orchestrator) handleWorkflowRetry(ctx context.Context, event types.JobEvent) error {
	wfID := event.JobID
	if wfID == "" {
		return nil
	}

	wf, rev, err := o.store.GetWorkflow(ctx, wfID)
	if err != nil {
		return err
	}
	if wf.Status.IsTerminal() {
		return nil
	}

	now := time.Now()

	// Dispatch on_init hook if this is the first attempt.
	if wf.Attempt <= 1 && wf.Definition.Hooks != nil {
		o.dispatchWorkflowHook(ctx, wf, "on_init", wf.Definition.Hooks.OnInit, &now)
	}

	ready := ReadyTasks(wf)
	for _, name := range ready {
		o.dispatchWorkflowTask(ctx, wf, name, &now)
	}

	if _, err := o.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		o.logger.Warn().String("workflow_id", wfID).Err(err).Msg("orchestrator: failed to save workflow after retry dispatch")
		return err
	}

	o.logger.Info().String("workflow_id", wfID).Int("ready", len(ready)).Msg("orchestrator: dispatched ready tasks for workflow retry")
	return nil
}

// handleBatchRetry dispatches the first chunk after an API-triggered batch retry.
func (o *Orchestrator) handleBatchRetry(ctx context.Context, event types.JobEvent) error {
	batchID := event.JobID
	if batchID == "" {
		return nil
	}

	batch, rev, err := o.store.GetBatch(ctx, batchID)
	if err != nil {
		return err
	}
	if batch.Status.IsTerminal() {
		return nil
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
		return err
	}

	o.logger.Info().String("batch_id", batchID).Msg("orchestrator: dispatched batch retry")
	return nil
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

	if _, err := o.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		o.logger.Error().String("workflow_id", wf.ID).Err(err).Msg("orchestrator: failed to save workflow timeout")
		return
	}

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

	// Cancel all non-terminal items and signal running handlers.
	errStr := types.ErrExecutionTimedOut.Error()
	for id, state := range batch.ItemStates {
		if !state.Status.IsTerminal() {
			if state.JobID != "" {
				o.signalCancelActiveRuns(ctx, state.JobID)
			}
			state.Status = types.JobStatusCancelled
			state.FinishedAt = &now
			state.Error = &errStr
			batch.ItemStates[id] = state
		}
	}
	// Cancel onetime task if still running.
	if batch.OnetimeState != nil && !batch.OnetimeState.Status.IsTerminal() {
		if batch.OnetimeState.JobID != "" {
			o.signalCancelActiveRuns(ctx, batch.OnetimeState.JobID)
		}
		batch.OnetimeState.Status = types.JobStatusCancelled
		batch.OnetimeState.FinishedAt = &now
		batch.OnetimeState.Error = &errStr
	}

	if _, err := o.store.UpdateBatch(ctx, batch, rev); err != nil {
		o.logger.Error().String("batch_id", batch.ID).Err(err).Msg("orchestrator: failed to save batch timeout")
		return
	}

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

// --- Precondition Evaluation ---

// evaluatePreconditions checks all preconditions for a task. Returns an empty string
// if all preconditions pass, or a reason string if any fails.
func (o *Orchestrator) evaluatePreconditions(ctx context.Context, wf *types.WorkflowInstance, taskDef *types.WorkflowTask) string {
	for _, pc := range taskDef.Preconditions {
		if pc.Type != "upstream_output" {
			continue
		}
		upstreamState, ok := wf.Tasks[pc.Task]
		if !ok || upstreamState.JobID == "" {
			return fmt.Sprintf("precondition not met: upstream task %q has no result", pc.Task)
		}
		result, err := o.store.GetResult(ctx, upstreamState.JobID)
		if err != nil || result == nil || result.Output == nil {
			return fmt.Sprintf("precondition not met: upstream task %q result not found", pc.Task)
		}
		actual := extractSimpleJSONPath(result.Output, pc.Path)
		if actual != pc.Expected {
			return fmt.Sprintf("precondition not met: %s.%s = %q, expected %q", pc.Task, pc.Path, actual, pc.Expected)
		}
	}
	return ""
}

// extractSimpleJSONPath extracts a value from JSON using a simple dot-separated path.
// Supports paths like "$.status" or "status" or "result.code".
// Returns the value as a string, or empty string if not found.
func extractSimpleJSONPath(data json.RawMessage, path string) string {
	// Strip leading "$." if present.
	if len(path) > 2 && path[:2] == "$." {
		path = path[2:]
	}

	// Split by "." and traverse.
	parts := splitDotPath(path)
	var current interface{}
	if err := json.Unmarshal(data, &current); err != nil {
		return ""
	}

	for _, part := range parts {
		obj, ok := current.(map[string]interface{})
		if !ok {
			return ""
		}
		current, ok = obj[part]
		if !ok {
			return ""
		}
	}

	// Convert to string.
	switch v := current.(type) {
	case string:
		return v
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%g", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// splitDotPath splits a path like "a.b.c" into ["a", "b", "c"].
func splitDotPath(path string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			if i > start {
				parts = append(parts, path[start:i])
			}
			start = i + 1
		}
	}
	if start < len(path) {
		parts = append(parts, path[start:])
	}
	return parts
}

// --- Hook Event Handlers ---

// handleHookCompleted updates the hook state when a hook job completes.
func (o *Orchestrator) handleHookCompleted(ctx context.Context, wfID, hookName, jobID string) {
	wf, rev, err := o.store.GetWorkflow(ctx, wfID)
	if err != nil {
		return
	}
	now := time.Now()
	if wf.HookStates != nil {
		if state, ok := wf.HookStates[hookName]; ok {
			state.Status = types.JobStatusCompleted
			state.FinishedAt = &now
			wf.HookStates[hookName] = state
			wf.UpdatedAt = now
			o.store.UpdateWorkflow(ctx, wf, rev)
		}
	}
	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowHookCompleted,
		JobID:     jobID,
		Timestamp: now,
	})
	o.logger.Info().String("workflow_id", wfID).String("hook", hookName).Msg("workflow hook completed")
}

// handleHookFailed updates the hook state when a hook job fails.
// Hook failures do not change the workflow status.
func (o *Orchestrator) handleHookFailed(ctx context.Context, wfID, hookName, jobID string) {
	wf, rev, err := o.store.GetWorkflow(ctx, wfID)
	if err != nil {
		return
	}
	now := time.Now()
	if wf.HookStates != nil {
		if state, ok := wf.HookStates[hookName]; ok {
			state.Status = types.JobStatusFailed
			state.FinishedAt = &now
			wf.HookStates[hookName] = state
			wf.UpdatedAt = now
			o.store.UpdateWorkflow(ctx, wf, rev)
		}
	}
	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowHookFailed,
		JobID:     jobID,
		Timestamp: now,
	})
	o.logger.Warn().String("workflow_id", wfID).String("hook", hookName).Msg("workflow hook failed (non-fatal)")
}

// --- Lifecycle Hooks ---

// dispatchWorkflowHook dispatches a lifecycle hook job for a workflow.
// Hook jobs are fire-and-forget: their success or failure does not affect workflow status.
func (o *Orchestrator) dispatchWorkflowHook(ctx context.Context, wf *types.WorkflowInstance, hookName string, hookDef *types.WorkflowHookDef, now *time.Time) {
	if hookDef == nil {
		return
	}

	taskName := "__hook:" + hookName
	job := &types.Job{
		ID:           generateID(),
		TaskType:     hookDef.TaskType,
		Payload:      hookDef.Payload,
		Schedule:     types.Schedule{Type: types.ScheduleImmediate},
		Status:       types.JobStatusPending,
		Priority:     wf.Definition.DefaultPriority,
		WorkflowID:   &wf.ID,
		WorkflowTask: &taskName,
		CreatedAt:    *now,
		UpdatedAt:    *now,
	}

	if _, err := o.store.CreateJob(ctx, job); err != nil {
		o.logger.Error().String("workflow_id", wf.ID).String("hook", hookName).Err(err).Msg("orchestrator: failed to create hook job")
		return
	}

	if err := o.dispatcher.Dispatch(ctx, job, 0); err != nil {
		o.logger.Error().String("workflow_id", wf.ID).String("hook", hookName).Err(err).Msg("orchestrator: failed to dispatch hook job")
		o.store.DeleteJob(ctx, job.ID)
		return
	}

	// Track hook state.
	if wf.HookStates == nil {
		wf.HookStates = make(map[string]types.WorkflowTaskState)
	}
	wf.HookStates[hookName] = types.WorkflowTaskState{
		Name:      hookName,
		JobID:     job.ID,
		Status:    types.JobStatusRunning,
		StartedAt: now,
	}

	o.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowHookDispatched,
		JobID:     job.ID,
		TaskType:  job.TaskType,
		Timestamp: *now,
	})

	o.logger.Info().String("workflow_id", wf.ID).String("hook", hookName).String("job_id", job.ID).Msg("workflow hook dispatched")
}

// isHookJob checks if a workflow task name represents a hook job.
func isHookJob(taskName string) (string, bool) {
	const prefix = "__hook:"
	if len(taskName) > len(prefix) && taskName[:len(prefix)] == prefix {
		return taskName[len(prefix):], true
	}
	return "", false
}

func strPtr(s string) *string { return &s }

func generateID() string {
	return xid.New().String()
}
