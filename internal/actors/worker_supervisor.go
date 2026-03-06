package actors

import (
	"fmt"
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq"
	"github.com/FDK0901/dureq/internal/lock"
	"github.com/FDK0901/dureq/internal/messages"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/anthdm/hollywood/actor"
)

const capacityTickInterval = 2 * time.Second

// HandlerRegistry is the interface for looking up task handlers by type.
type HandlerRegistry interface {
	Get(taskType types.TaskType) (*types.HandlerDefinition, bool)
	TaskTypes() []string
}

// WorkerSupervisorActor runs one per node. It manages the pool of WorkerActor
// children, enforces concurrency limits, and routes job results to the
// EventBridgeActor.
type WorkerSupervisorActor struct {
	nodeID         string
	store          *store.RedisStore
	registry       HandlerRegistry
	maxConcurrency int
	locker         *lock.Locker
	running        int
	activeRuns     map[string]*actor.PID // runID -> child WorkerActor PID
	dispatcherPID  *actor.PID
	eventBridgePID *actor.PID
	repeater       actor.SendRepeater
	draining       bool
	logger         gochainedlog.Logger
}

// NewWorkerSupervisorActor returns a Hollywood Producer that creates a
// WorkerSupervisorActor for the given node.
func NewWorkerSupervisorActor(nodeID string, s *store.RedisStore, registry HandlerRegistry, maxConcurrency int, locker *lock.Locker, logger gochainedlog.Logger) actor.Producer {
	return func() actor.Receiver {
		return &WorkerSupervisorActor{
			nodeID:         nodeID,
			store:          s,
			registry:       registry,
			maxConcurrency: maxConcurrency,
			locker:         locker,
			activeRuns:     make(map[string]*actor.PID),
			logger:         logger,
		}
	}
}

// SetDispatcherPID sets the PID of the DispatcherActor so capacity reports
// can be sent to it. Called by the server during wiring.
func (ws *WorkerSupervisorActor) SetDispatcherPID(pid *actor.PID) {
	ws.dispatcherPID = pid
}

// SetEventBridgePID sets the PID of the EventBridgeActor so job results
// can be forwarded to it. Called by the server during wiring.
func (ws *WorkerSupervisorActor) SetEventBridgePID(pid *actor.PID) {
	ws.eventBridgePID = pid
}

// Receive implements actor.Receiver.
func (ws *WorkerSupervisorActor) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case actor.Started:
		ws.onStarted(ctx)
	case actor.Stopped:
		ws.onStopped()
	case messages.WireDispatcherPIDMsg:
		ws.dispatcherPID = msg.PID
		ws.logger.Info().String("pid", msg.PID.String()).Msg("worker supervisor: dispatcher PID wired")
		ws.onCapacityTick(ctx) // immediate report so dispatcher knows about us
	case messages.WireEventBridgePIDMsg:
		ws.eventBridgePID = msg.PID
		ws.logger.Info().String("pid", msg.PID.String()).Msg("worker supervisor: event bridge PID wired")
	case *pb.ExecuteJobMsg:
		ws.onExecuteJob(ctx, msg)
	case *messages.WorkerDoneMsg:
		ws.onWorkerDone(ctx, msg)
	case messages.WorkerDoneMsg:
		ws.onWorkerDone(ctx, &msg)
	case messages.CapacityTickMsg:
		ws.onCapacityTick(ctx)
	case messages.DrainMsg:
		ws.onDrain()
	case *pb.CancelRunMsg:
		ws.onCancelRun(ctx, msg)
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (ws *WorkerSupervisorActor) onStarted(ctx *actor.Context) {
	ws.repeater = ctx.SendRepeat(ctx.PID(), messages.CapacityTickMsg{}, capacityTickInterval)

	// Send an immediate capacity report so the dispatcher knows about us.
	ws.onCapacityTick(ctx)

	ws.logger.Info().String("node_id", ws.nodeID).Int("max_concurrency", ws.maxConcurrency).Strs("task_types", ws.registry.TaskTypes()).Msg("worker supervisor started")
}

func (ws *WorkerSupervisorActor) onStopped() {
	ws.repeater.Stop()
	ws.draining = false

	ws.logger.Info().String("node_id", ws.nodeID).Int("running", ws.running).Msg("worker supervisor stopped")
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

func (ws *WorkerSupervisorActor) onExecuteJob(ctx *actor.Context, msg *pb.ExecuteJobMsg) {
	// Reject if draining or at capacity.
	if ws.draining || ws.running >= ws.maxConcurrency {
		reason := "at max concurrency"
		if ws.draining {
			reason = "node is draining"
		}
		ws.logger.Warn().String("run_id", msg.RunId).String("job_id", msg.JobId).String("reason", reason).Int("running", ws.running).Int("max", ws.maxConcurrency).Msg("worker supervisor: rejecting job")
		if ctx.Sender() != nil {
			ctx.Send(ctx.Sender(), &pb.RejectJobMsg{
				RunId:  msg.RunId,
				JobId:  msg.JobId,
				Reason: reason,
			})
		}
		return
	}

	// Look up the handler for this task type.
	handler, ok := ws.registry.Get(types.TaskType(msg.TaskType))
	if !ok {
		ws.logger.Warn().String("task_type", string(msg.TaskType)).String("job_id", msg.JobId).Msg("worker supervisor: no handler for task type")
		if ctx.Sender() != nil {
			ctx.Send(ctx.Sender(), &pb.RejectJobMsg{
				RunId:  msg.RunId,
				JobId:  msg.JobId,
				Reason: fmt.Sprintf("no handler registered for task type %s", msg.TaskType),
			})
		}
		return
	}

	// Spawn a child WorkerActor for this job.
	childPID := ctx.SpawnChild(
		NewWorkerActor(msg, *handler, ws.store, ws.nodeID, ws.locker, ws.logger),
		fmt.Sprintf("worker/%s", msg.RunId),
	)

	ws.activeRuns[msg.RunId] = childPID
	ws.running++

	ws.logger.Debug().String("run_id", msg.RunId).String("job_id", msg.JobId).String("task_type", string(msg.TaskType)).Int("running", ws.running).Int("max", ws.maxConcurrency).Msg("worker supervisor: spawned worker")
}

func (ws *WorkerSupervisorActor) onWorkerDone(ctx *actor.Context, msg *messages.WorkerDoneMsg) {
	// Remove from tracking.
	delete(ws.activeRuns, msg.RunID)
	if ws.running > 0 {
		ws.running--
	}

	ws.logger.Debug().String("run_id", msg.RunID).String("job_id", msg.JobID).Bool("success", msg.Success).Int("running", ws.running).Msg("worker supervisor: worker done")

	// Forward directly to EventBridgeActor. EventBridge is always local so
	// we send the rich WorkerDoneMsg (carries Run for batched completion)
	// instead of converting to proto.
	if ws.eventBridgePID != nil {
		msg.NodeID = ws.nodeID
		ctx.Send(ws.eventBridgePID, msg)
	}
}

func (ws *WorkerSupervisorActor) onCapacityTick(ctx *actor.Context) {
	if ws.dispatcherPID == nil {
		return
	}

	idle := ws.maxConcurrency - ws.running
	if idle < 0 {
		idle = 0
	}

	// Include explicit PID so the dispatcher can route ExecuteJobMsg back
	// even if ctx.Sender() is not preserved through cluster singleton routing.
	myPID := ctx.PID()
	ctx.Send(ws.dispatcherPID, &pb.WorkerStatusMsg{
		NodeId:         ws.nodeID,
		RunningWorkers: int32(ws.running),
		IdleWorkers:    int32(idle),
		MaxConcurrency: int32(ws.maxConcurrency),
		TaskTypes:      ws.registry.TaskTypes(),
		SenderAddress:  myPID.Address,
		SenderId:       myPID.ID,
	})
}

func (ws *WorkerSupervisorActor) onDrain() {
	ws.draining = true
	ws.logger.Info().String("node_id", ws.nodeID).Int("running", ws.running).Msg("worker supervisor: draining")
}

func (ws *WorkerSupervisorActor) onCancelRun(ctx *actor.Context, msg *pb.CancelRunMsg) {
	childPID, ok := ws.activeRuns[msg.RunId]
	if !ok {
		ws.logger.Debug().String("run_id", msg.RunId).Msg("worker supervisor: cancel run not found")
		return
	}

	ctx.Send(childPID, msg)

	ws.logger.Info().String("run_id", msg.RunId).Msg("worker supervisor: forwarding cancel to worker")
}
