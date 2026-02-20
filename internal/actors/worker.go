package actors

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"errors"

	pb "github.com/FDK0901/dureq/gen/dureq"
	"github.com/FDK0901/dureq/internal/lock"
	"github.com/FDK0901/dureq/internal/messages"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/anthdm/hollywood/actor"
)

const workerHeartbeatInterval = 3 * time.Second

// heartbeatTickMsg is a local tick for the per-run heartbeat inside a WorkerActor.
type heartbeatTickMsg struct{}

// WorkerActor is a short-lived, per-job child of WorkerSupervisorActor.
// It executes a single job handler, manages run lifecycle bookkeeping, and
// reports the result back to its parent.
type WorkerActor struct {
	work       *pb.ExecuteJobMsg
	handler    types.HandlerDefinition
	store      *store.RedisStore
	nodeID     string
	locker     *lock.Locker
	runLock    *lock.RedisLock
	run        *types.JobRun
	execCancel context.CancelFunc
	repeater   actor.SendRepeater
	done       bool

	// progressCh buffers progress updates from the handler goroutine.
	// Size 1 so the latest progress is available on the next heartbeat tick.
	progressCh chan json.RawMessage

	logger gochainedlog.Logger

	// Cached from actor.Started so goroutines can send messages back.
	engine *actor.Engine
	pid    *actor.PID
}

// NewWorkerActor returns a Hollywood Producer that creates a WorkerActor
// for the given job execution message.
func NewWorkerActor(work *pb.ExecuteJobMsg, handler types.HandlerDefinition, s *store.RedisStore, nodeID string, locker *lock.Locker, logger gochainedlog.Logger) actor.Producer {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return func() actor.Receiver {
		return &WorkerActor{
			work:    work,
			handler: handler,
			store:   s,
			nodeID:  nodeID,
			locker:  locker,
			logger:  logger,
		}
	}
}

// Receive implements actor.Receiver.
func (w *WorkerActor) Receive(ctx *actor.Context) {
	switch m := ctx.Message().(type) {
	case actor.Started:
		w.onStarted(ctx)
	case actor.Stopped:
		w.onStopped()
	case heartbeatTickMsg:
		w.onHeartbeatTick()
	case *messages.ExecutionDoneMsg:
		w.onExecutionDone(ctx, m)
	case messages.ExecutionDoneMsg:
		w.onExecutionDone(ctx, &m)
	case *pb.CancelRunMsg:
		w.onCancelRun(m)
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (w *WorkerActor) onStarted(ctx *actor.Context) {
	w.engine = ctx.Engine()
	w.pid = ctx.PID()
	w.progressCh = make(chan json.RawMessage, 1)
	now := time.Now()

	// 1. Schedule-to-start timeout check.
	if w.work.ScheduleToStartTimeoutMs > 0 && w.work.DispatchedAtUnixMs > 0 {
		dispatchedAt := time.UnixMilli(w.work.DispatchedAtUnixMs)
		waited := now.Sub(dispatchedAt)
		timeout := time.Duration(w.work.ScheduleToStartTimeoutMs) * time.Millisecond
		if waited > timeout {
			w.logger.Warn().String("job_id", w.work.JobId).String("run_id", w.work.RunId).Duration("waited", waited).Duration("timeout", timeout).Msg("worker: schedule-to-start timeout exceeded")
			w.failImmediately(ctx, now, types.ErrScheduleToStartTimeout.Error())
			return
		}
	}

	// 2. Per-run lock (exactly-once guard).
	lockOwner := fmt.Sprintf("%s:%d", w.nodeID, now.UnixNano())
	bgLockCtx, lockCancel := context.WithTimeout(context.Background(), 3*time.Second)
	rl, lockErr := w.locker.Lock(bgLockCtx, "run:"+w.work.RunId, lockOwner)
	lockCancel()
	if lockErr != nil {
		if errors.Is(lockErr, types.ErrLockFailed) {
			w.logger.Info().String("run_id", w.work.RunId).Msg("worker: run already locked by another worker, skipping")
			w.failImmediately(ctx, now, "run already locked by another worker")
			return
		}
		w.logger.Warn().String("run_id", w.work.RunId).Err(lockErr).Msg("worker: lock acquisition error")
		w.failImmediately(ctx, now, "lock acquisition failed: "+lockErr.Error())
		return
	}
	w.runLock = rl

	// 3. Create run record.
	w.run = &types.JobRun{
		ID:        w.work.RunId,
		JobID:     w.work.JobId,
		NodeID:    w.nodeID,
		Status:    types.RunStatusRunning,
		Attempt:   int(w.work.Attempt),
		StartedAt: now,
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := w.store.SaveRun(bgCtx, w.run); err != nil {
		w.logger.Error().String("run_id", w.run.ID).Err(err).Msg("worker: failed to save initial run")
	}

	// 4. Start heartbeat repeater for this run.
	w.repeater = ctx.SendRepeat(ctx.PID(), heartbeatTickMsg{}, workerHeartbeatInterval)

	// 5. Wire lock-lost callback to cancel the handler.
	rl.SetOnLost(func() {
		w.logger.Warn().String("run_id", w.work.RunId).Msg("worker: run lock lost, cancelling handler")
		if w.execCancel != nil {
			w.execCancel()
		}
	})

	// 6. Launch handler execution in a goroutine.
	w.startExecution()

	w.logger.Info().String("run_id", w.work.RunId).String("job_id", w.work.JobId).String("task_type", string(w.work.TaskType)).Int("attempt", int(w.work.Attempt)).Msg("worker actor started")
}

func (w *WorkerActor) onStopped() {
	// Only stop repeater if not already stopped by onExecutionDone/failImmediately.
	if !w.done {
		w.repeater.Stop()
	}
	if w.execCancel != nil {
		w.execCancel()
	}
	if w.runLock != nil {
		w.runLock.Unlock(context.Background())
	}
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

func (w *WorkerActor) onHeartbeatTick() {
	if w.done || w.run == nil {
		return
	}
	now := time.Now()
	w.run.LastHeartbeatAt = &now

	// Drain progress channel — take latest value.
	select {
	case progress := <-w.progressCh:
		w.run.Progress = progress
	default:
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := w.store.SaveRun(bgCtx, w.run); err != nil {
		w.logger.Warn().String("run_id", w.run.ID).Err(err).Msg("worker: heartbeat save failed")
	}
}

func (w *WorkerActor) onExecutionDone(ctx *actor.Context, result *messages.ExecutionDoneMsg) {
	if w.done {
		return
	}
	w.done = true

	// 1. Stop heartbeat repeater.
	w.repeater.Stop()

	// 2. Update run status (terminal).
	now := time.Now()
	w.run.FinishedAt = &now
	w.run.Duration = now.Sub(w.run.StartedAt)

	// Drain any final progress from the handler goroutine.
	select {
	case progress := <-w.progressCh:
		w.run.Progress = progress
	default:
	}

	if result.Success {
		w.run.Status = types.RunStatusSucceeded
	} else {
		w.run.Status = types.RunStatusFailed
		if result.Error != "" {
			errStr := result.Error
			w.run.Error = &errStr
		}
	}

	// 3. Release per-run lock.
	if w.runLock != nil {
		w.runLock.Unlock(context.Background())
	}

	// 4. Notify parent with complete run record for batched completion.
	// Terminal SaveRun is handled by EventBridge's CompleteRun pipeline.
	if parent := ctx.Parent(); parent != nil {
		ctx.Send(parent, &messages.WorkerDoneMsg{
			RunID:      w.work.RunId,
			JobID:      w.work.JobId,
			TaskType:   w.work.TaskType,
			Success:    result.Success,
			Error:      result.Error,
			Output:     result.Output,
			Attempt:    int(w.work.Attempt),
			StartedAt:  w.run.StartedAt,
			FinishedAt: now,
			WorkflowID: w.work.WorkflowId,
			BatchID:    w.work.BatchId,
			NodeID:     w.nodeID,
			Run:        w.run,
		})
	}

	// 4. Poison self — process remaining inbox then shut down.
	ctx.Engine().Poison(ctx.PID())
}

func (w *WorkerActor) onCancelRun(msg *pb.CancelRunMsg) {
	if w.done {
		return
	}
	w.logger.Info().String("run_id", msg.RunId).Msg("worker: cancel requested")
	if w.execCancel != nil {
		w.execCancel()
	}
}

// ---------------------------------------------------------------------------
// Handler execution goroutine
// ---------------------------------------------------------------------------

func (w *WorkerActor) startExecution() {
	engine := w.engine
	self := w.pid

	go func() {
		var execErr error
		var output json.RawMessage

		defer func() {
			if r := recover(); r != nil {
				execErr = &types.PanicError{Value: r}
			}

			success := execErr == nil
			errStr := ""
			if execErr != nil {
				errStr = execErr.Error()
			}

			engine.Send(self, &messages.ExecutionDoneMsg{
				Success: success,
				Error:   errStr,
				Output:  output,
			})
		}()

		// Build handler context with deadline/timeout.
		handlerCtx, cancel := context.WithCancel(context.Background())
		w.execCancel = cancel

		if w.work.DeadlineUnixMs > 0 {
			deadline := time.UnixMilli(w.work.DeadlineUnixMs)
			handlerCtx, cancel = context.WithDeadline(handlerCtx, deadline)
			w.execCancel = cancel
		} else if w.handler.Timeout > 0 {
			handlerCtx, cancel = context.WithTimeout(handlerCtx, w.handler.Timeout)
			w.execCancel = cancel
		}

		// Enrich context with job metadata.
		handlerCtx = types.WithJobID(handlerCtx, w.work.JobId)
		handlerCtx = types.WithRunID(handlerCtx, w.work.RunId)
		handlerCtx = types.WithAttempt(handlerCtx, int(w.work.Attempt))
		handlerCtx = types.WithTaskType(handlerCtx, types.TaskType(w.work.TaskType))
		handlerCtx = types.WithPriority(handlerCtx, types.Priority(w.work.Priority))
		handlerCtx = types.WithNodeID(handlerCtx, w.nodeID)
		if w.work.Headers != nil {
			handlerCtx = types.WithHeaders(handlerCtx, w.work.Headers)
		}

		// Inject progress reporter — writes to buffered channel, drained on heartbeat tick.
		progressCh := w.progressCh
		handlerCtx = types.WithProgressReporter(handlerCtx, func(_ context.Context, data json.RawMessage) error {
			select {
			case progressCh <- data:
			default:
				// Channel full — replace with latest value.
				select {
				case <-progressCh:
				default:
				}
				progressCh <- data
			}
			return nil
		})

		payload := json.RawMessage(w.work.Payload)

		// Execute the handler.
		if w.handler.HandlerWithResult != nil {
			output, execErr = w.handler.HandlerWithResult(handlerCtx, payload)
		} else if w.handler.Handler != nil {
			execErr = w.handler.Handler(handlerCtx, payload)
		} else {
			execErr = fmt.Errorf("no handler function registered for task type %s", w.work.TaskType)
		}
	}()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// failImmediately records a failed run and poisons self without executing the handler.
func (w *WorkerActor) failImmediately(ctx *actor.Context, now time.Time, errStr string) {
	w.done = true

	w.run = &types.JobRun{
		ID:        w.work.RunId,
		JobID:     w.work.JobId,
		NodeID:    w.nodeID,
		Status:    types.RunStatusFailed,
		Attempt:   int(w.work.Attempt),
		StartedAt: now,
	}
	finishedAt := now
	w.run.FinishedAt = &finishedAt
	w.run.Error = &errStr

	bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, saveErr := w.store.SaveRun(bgCtx, w.run); saveErr != nil {
		w.logger.Error().String("run_id", w.run.ID).Err(saveErr).Msg("worker: failed to save timeout run")
	}

	// Notify parent of the failure.
	if parent := ctx.Parent(); parent != nil {
		ctx.Send(parent, &messages.WorkerDoneMsg{
			RunID:      w.work.RunId,
			JobID:      w.work.JobId,
			TaskType:   w.work.TaskType,
			Success:    false,
			Error:      errStr,
			Attempt:    int(w.work.Attempt),
			StartedAt:  now,
			FinishedAt: finishedAt,
			WorkflowID: w.work.WorkflowId,
			BatchID:    w.work.BatchId,
		})
	}

	ctx.Engine().Poison(ctx.PID())
}
