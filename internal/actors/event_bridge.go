package actors

import (
	"context"
	"encoding/json"
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq"
	"github.com/FDK0901/dureq/internal/messages"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/anthdm/hollywood/actor"
)

// EventBridgeActor runs one per node. It receives job results and domain
// events, persists them to Redis, and forwards domain events to the
// OrchestratorActor singleton.
type EventBridgeActor struct {
	nodeID          string
	store           *store.RedisStore
	orchestratorPID *actor.PID
	logger          gochainedlog.Logger
}

// NewEventBridgeActor returns a Hollywood Producer that creates an
// EventBridgeActor for the given node.
func NewEventBridgeActor(nodeID string, s *store.RedisStore, logger gochainedlog.Logger) actor.Producer {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return func() actor.Receiver {
		return &EventBridgeActor{
			nodeID: nodeID,
			store:  s,
			logger: logger,
		}
	}
}

// SetOrchestratorPID sets the PID of the OrchestratorActor so domain events
// can be forwarded to it. Called by the server during wiring.
func (eb *EventBridgeActor) SetOrchestratorPID(pid *actor.PID) {
	eb.orchestratorPID = pid
}

// Receive implements actor.Receiver.
func (eb *EventBridgeActor) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case actor.Started:
		eb.onStarted()
	case actor.Stopped:
		eb.onStopped()
	case messages.WireOrchestratorPIDMsg:
		eb.orchestratorPID = msg.PID
		eb.logger.Info().String("pid", msg.PID.String()).Msg("event bridge: orchestrator PID wired")
	case *messages.WorkerDoneMsg:
		eb.onWorkerDone(ctx, msg)
	case *pb.JobResultMsg:
		eb.onJobResult(ctx, msg)
	case *pb.DomainEventMsg:
		eb.onDomainEvent(ctx, msg)
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (eb *EventBridgeActor) onStarted() {
	eb.logger.Info().String("node_id", eb.nodeID).Msg("event bridge actor started")
}

func (eb *EventBridgeActor) onStopped() {
	eb.logger.Info().String("node_id", eb.nodeID).Msg("event bridge actor stopped")
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

// onWorkerDone handles a local WorkerDoneMsg using the batched CompleteRun pipeline.
// This replaces separate SaveRun + PublishEvent + PublishResult with a single pipeline.
func (eb *EventBridgeActor) onWorkerDone(ctx *actor.Context, msg *messages.WorkerDoneMsg) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Build domain event.
	eventType := types.EventJobCompleted
	dailyStat := "processed"
	if !msg.Success {
		eventType = types.EventJobFailed
		dailyStat = "failed"
	}

	event := types.JobEvent{
		Type:      eventType,
		JobID:     msg.JobID,
		RunID:     msg.RunID,
		NodeID:    msg.NodeID,
		TaskType:  types.TaskType(msg.TaskType),
		Attempt:   msg.Attempt,
		Timestamp: time.Now(),
	}
	if msg.Error != "" {
		errStr := msg.Error
		event.Error = &errStr
	}

	result := types.WorkResult{
		RunID:   msg.RunID,
		JobID:   msg.JobID,
		Success: msg.Success,
		Output:  msg.Output,
	}
	if msg.Error != "" {
		errStr := msg.Error
		result.Error = &errStr
	}

	// Use the run record from the worker if available.
	run := msg.Run
	if run == nil {
		// Fallback: reconstruct minimal run from message fields.
		run = &types.JobRun{
			ID:        msg.RunID,
			JobID:     msg.JobID,
			NodeID:    msg.NodeID,
			Attempt:   msg.Attempt,
			StartedAt: msg.StartedAt,
		}
		finishedAt := msg.FinishedAt
		run.FinishedAt = &finishedAt
		run.Duration = msg.FinishedAt.Sub(msg.StartedAt)
		if msg.Success {
			run.Status = types.RunStatusSucceeded
		} else {
			run.Status = types.RunStatusFailed
			if msg.Error != "" {
				errStr := msg.Error
				run.Error = &errStr
			}
		}
	}

	batch := &store.CompletionBatch{
		Run:            run,
		Event:          event,
		Result:         result,
		DailyStatField: dailyStat,
	}

	if err := eb.store.CompleteRun(bgCtx, batch); err != nil {
		eb.logger.Error().String("run_id", msg.RunID).String("job_id", msg.JobID).Err(err).Msg("event bridge: CompleteRun pipeline failed")
	}

	// Evaluate retry policy for standalone jobs, or just update status for batch/workflow.
	eb.handleRetryOrFinalize(bgCtx, msg)

	// Forward to OrchestratorActor if workflow/batch job.
	if eb.orchestratorPID != nil && (msg.WorkflowID != "" || msg.BatchID != "") {
		domainEvent := messages.JobEventToDomainEvent(event)
		ctx.Send(eb.orchestratorPID, domainEvent)
	}

	eb.logger.Debug().String("run_id", msg.RunID).String("job_id", msg.JobID).Bool("success", msg.Success).Msg("event bridge: completed run (batched)")
}

func (eb *EventBridgeActor) onJobResult(ctx *actor.Context, msg *pb.JobResultMsg) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Build domain event from the result.
	eventType := types.EventJobCompleted
	if !msg.Success {
		eventType = types.EventJobFailed
	}

	event := types.JobEvent{
		Type:      eventType,
		JobID:     msg.JobId,
		RunID:     msg.RunId,
		NodeID:    msg.NodeId,
		TaskType:  types.TaskType(msg.TaskType),
		Attempt:   int(msg.Attempt),
		Timestamp: time.Now(),
	}

	if msg.Error != "" {
		errStr := msg.Error
		event.Error = &errStr
	}

	// 2. Persist the event via store.
	if err := eb.store.PublishEvent(bgCtx, event); err != nil {
		eb.logger.Error().String("run_id", msg.RunId).String("job_id", msg.JobId).String("type", string(eventType)).Err(err).Msg("event bridge: failed to publish event")
	}

	// 3. Persist the result via store.
	result := types.WorkResult{
		RunID:   msg.RunId,
		JobID:   msg.JobId,
		Success: msg.Success,
		Output:  json.RawMessage(msg.Output),
	}
	if msg.Error != "" {
		errStr := msg.Error
		result.Error = &errStr
	}

	if err := eb.store.PublishResult(bgCtx, result); err != nil {
		eb.logger.Error().String("run_id", msg.RunId).String("job_id", msg.JobId).Err(err).Msg("event bridge: failed to publish result")
	}

	// 4. Forward as DomainEventMsg to OrchestratorActor if relevant.
	if eb.orchestratorPID != nil && (msg.WorkflowId != "" || msg.BatchId != "") {
		domainEvent := messages.JobEventToDomainEvent(event)
		ctx.Send(eb.orchestratorPID, domainEvent)
	}

	eb.logger.Debug().String("run_id", msg.RunId).String("job_id", msg.JobId).Bool("success", msg.Success).Msg("event bridge: processed job result")
}

func (eb *EventBridgeActor) onDomainEvent(ctx *actor.Context, msg *pb.DomainEventMsg) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Convert to domain event.
	event := messages.DomainEventToJobEvent(msg)

	// 2. Persist the event via store.
	if err := eb.store.PublishEvent(bgCtx, event); err != nil {
		eb.logger.Error().String("type", string(msg.Type)).String("job_id", msg.JobId).Err(err).Msg("event bridge: failed to publish domain event")
	}

	// 3. Forward to OrchestratorActor.
	if eb.orchestratorPID != nil {
		ctx.Send(eb.orchestratorPID, msg)
	}

	eb.logger.Debug().String("type", string(msg.Type)).String("job_id", msg.JobId).Msg("event bridge: forwarded domain event")
}

// handleRetryOrFinalize evaluates whether a failed standalone job should be retried
// (with exponential backoff via the delayed sorted set) or moved to the DLQ.
// Batch/workflow jobs delegate to the orchestrator — we only update their status here.
func (eb *EventBridgeActor) handleRetryOrFinalize(ctx context.Context, msg *messages.WorkerDoneMsg) {
	// Batch/workflow jobs: orchestrator handles retry logic.
	if msg.BatchID != "" || msg.WorkflowID != "" {
		eb.updateJobStatus(ctx, msg.JobID, msg.Success, msg.Error)
		return
	}
	if msg.Success {
		eb.updateJobStatus(ctx, msg.JobID, true, "")
		return
	}

	// Failed standalone job — check retry policy.
	job, rev, err := eb.store.GetJob(ctx, msg.JobID)
	if err != nil || job.Status.IsTerminal() {
		return
	}

	retryPolicy := job.RetryPolicy
	if retryPolicy == nil {
		retryPolicy = types.DefaultRetryPolicy()
	}

	now := time.Now()
	job.Attempt = msg.Attempt + 1
	errStr := msg.Error
	job.LastError = &errStr
	job.LastRunAt = &now
	job.UpdatedAt = now

	maxAttempts := retryPolicy.MaxAttempts
	if job.DLQAfter != nil && *job.DLQAfter > 0 && *job.DLQAfter < maxAttempts {
		maxAttempts = *job.DLQAfter
	}

	if job.Attempt >= maxAttempts {
		// Exhausted retries — move to DLQ.
		job.Status = types.JobStatusDead
		if _, err := eb.store.UpdateJob(ctx, job, rev); err != nil {
			eb.logger.Debug().String("job_id", job.ID).Err(err).Msg("event bridge: failed to update job to dead")
		}
		wm := &types.WorkMessage{
			JobID:    job.ID,
			TaskType: job.TaskType,
			Payload:  job.Payload,
		}
		if err := eb.store.DispatchToDLQ(ctx, wm); err != nil {
			eb.logger.Error().String("job_id", job.ID).Err(err).Msg("event bridge: failed to dispatch to DLQ")
		}
		return
	}

	// Schedule retry with backoff via delayed sorted set.
	job.Status = types.JobStatusRetrying
	if _, err := eb.store.UpdateJob(ctx, job, rev); err != nil {
		eb.logger.Debug().String("job_id", job.ID).Err(err).Msg("event bridge: failed to update job to retrying")
	}

	priority := int(types.PriorityNormal)
	if job.Priority != nil {
		priority = int(*job.Priority)
	}
	tierName := store.ResolveTier(eb.store.Config().Tiers, priority)
	delay := types.CalculateBackoff(msg.Attempt, retryPolicy)

	wm := &types.WorkMessage{
		JobID:    job.ID,
		TaskType: job.TaskType,
		Payload:  job.Payload,
		Attempt:  job.Attempt,
	}
	if err := eb.store.AddDelayed(ctx, tierName, wm, now.Add(delay)); err != nil {
		eb.logger.Error().String("job_id", job.ID).Err(err).Msg("event bridge: failed to add delayed retry")
	}

	eb.logger.Debug().String("job_id", job.ID).Int("attempt", job.Attempt).Int("max", maxAttempts).Msg("event bridge: scheduled retry")
}

// updateJobStatus sets the Job record to completed or failed after its run finishes.
// For recurring jobs with an active schedule, success sets the status back to "scheduled"
// instead of "completed" so the scheduler doesn't delete the schedule.
// Uses best-effort CAS — a conflict means another update already happened, which is fine.
func (eb *EventBridgeActor) updateJobStatus(ctx context.Context, jobID string, success bool, errMsg string) {
	job, rev, err := eb.store.GetJob(ctx, jobID)
	if err != nil {
		return
	}
	if job.Status.IsTerminal() {
		return // already in a terminal state
	}

	now := time.Now()
	if success {
		// Check for active schedule — recurring jobs return to "scheduled", not "completed".
		_, _, schedErr := eb.store.GetSchedule(ctx, jobID)
		if schedErr == types.ErrScheduleNotFound {
			job.Status = types.JobStatusCompleted
		} else {
			job.Status = types.JobStatusScheduled
		}
	} else {
		job.Status = types.JobStatusFailed
		if errMsg != "" {
			job.LastError = &errMsg
		}
	}
	job.UpdatedAt = now

	if _, err := eb.store.UpdateJob(ctx, job, rev); err != nil {
		eb.logger.Debug().String("job_id", jobID).Err(err).Msg("event bridge: best-effort job status update failed")
	}
}
