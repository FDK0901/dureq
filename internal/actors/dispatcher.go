package actors

import (
	"context"
	"strings"
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq"
	"github.com/FDK0901/dureq/internal/messages"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	chainedslog "github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/anthdm/hollywood/actor"
	"github.com/rs/xid"
)

const (
	staleCheckInterval = 15 * time.Second
	nodeStaleThreshold = 30 * time.Second

	// dispatchDedupTTL is how long a job ID stays in the dedup map after dispatch.
	// Must be long enough to cover the window between notification and worker start.
	dispatchDedupTTL = 30 * time.Second
)

// nodeStatus tracks the capacity and health of a remote WorkerSupervisorActor.
type nodeStatus struct {
	pid            *actor.PID
	runningWorkers int
	idleWorkers    int
	maxConcurrency int
	taskTypes      []string
	lastUpdate     time.Time
}

// DispatcherActor is a cluster singleton that receives job dispatch requests
// and routes them to the WorkerSupervisorActor on the node with the most
// available capacity for the requested task type.
type DispatcherActor struct {
	store          *store.RedisStore
	nodeCapacity   map[string]*nodeStatus // nodeID -> capacity info
	eventBridgePID *actor.PID
	repeater       actor.SendRepeater
	logger         gochainedlog.Logger

	// recentDispatches tracks recently dispatched job IDs to prevent
	// duplicate dispatches from multiple NotifierActors forwarding the
	// same Pub/Sub notification.
	recentDispatches map[string]time.Time
}

// NewDispatcherActor returns a Hollywood Producer that creates a DispatcherActor.
func NewDispatcherActor(s *store.RedisStore, logger gochainedlog.Logger) actor.Producer {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return func() actor.Receiver {
		return &DispatcherActor{
			store:            s,
			nodeCapacity:     make(map[string]*nodeStatus),
			recentDispatches: make(map[string]time.Time),
			logger:           logger,
		}
	}
}

// SetEventBridgePID sets the PID of the EventBridgeActor for publishing events.
func (d *DispatcherActor) SetEventBridgePID(pid *actor.PID) {
	d.eventBridgePID = pid
}

// Receive implements actor.Receiver.
func (d *DispatcherActor) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case actor.Started:
		d.onStarted(ctx)
	case actor.Stopped:
		d.onStopped()
	case *pb.SingletonCheckMsg:
		ctx.Respond(&pb.SingletonCheckMsg{})
	case *pb.DispatchJobMsg:
		d.onDispatchJob(ctx, msg)
	case *pb.NewJobNotification:
		d.onNewJobNotification(ctx, msg)
	case *pb.WorkerStatusMsg:
		d.onWorkerStatus(ctx, msg)
	case *pb.RejectJobMsg:
		d.onRejectJob(ctx, msg)
	case messages.StaleCheckMsg:
		d.onStaleCheck()
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (d *DispatcherActor) onStarted(ctx *actor.Context) {
	d.repeater = ctx.SendRepeat(ctx.PID(), messages.StaleCheckMsg{}, staleCheckInterval)

	d.logger.Info().Int("stale_check_interval", int(staleCheckInterval.Seconds())).Msg("dispatcher actor started")
}

func (d *DispatcherActor) onStopped() {
	d.repeater.Stop()
	d.logger.Info().Msg("dispatcher actor stopped")
}

// ---------------------------------------------------------------------------
// Message handlers
// ---------------------------------------------------------------------------

// onDispatchJob selects a target node and sends an ExecuteJobMsg.
// Returns true if a node was found and the message was sent.
func (d *DispatcherActor) onDispatchJob(ctx *actor.Context, msg *pb.DispatchJobMsg) bool {
	runID := xid.New().String()

	target := d.selectNode(msg.TaskType)
	if target == nil {
		d.logger.Warn().String("job_id", msg.JobId).String("task_type", string(msg.TaskType)).Msg("dispatcher: no node with capacity for task type")
		// No node available — the job stays in "pending" and will be picked up
		// on a future dispatch attempt (e.g., scheduler tick, delayed retry poller,
		// or manual retry).
		return false
	}

	execMsg := messages.DispatchToExecuteMsg(msg, runID)
	ctx.Send(target.pid, execMsg)

	d.logger.Debug().String("job_id", msg.JobId).String("run_id", runID).String("task_type", string(msg.TaskType)).Msg("dispatcher: dispatched job")
	return true
}

// onNewJobNotification loads a job from the store and dispatches it.
// Deduplicates notifications from multiple NotifierActors on different nodes.
func (d *DispatcherActor) onNewJobNotification(ctx *actor.Context, msg *pb.NewJobNotification) {
	// Dedup: skip if this job was recently dispatched.
	if dispatchedAt, ok := d.recentDispatches[msg.JobId]; ok {
		if time.Since(dispatchedAt) < dispatchDedupTTL {
			d.logger.Debug().String("job_id", msg.JobId).Msg("dispatcher: duplicate notification, skipping")
			return
		}
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Check if the queue/tier for this job's priority is paused.
	tierName := store.ResolveTier(d.store.Config().Tiers, int(msg.Priority))
	if paused, err := d.store.IsQueuePaused(bgCtx, tierName); err == nil && paused {
		d.logger.Debug().String("job_id", msg.JobId).String("tier", tierName).Msg("dispatcher: queue paused, skipping")
		return
	}

	job, _, err := d.store.GetJob(bgCtx, msg.JobId)
	if err != nil {
		d.logger.Error().String("job_id", msg.JobId).Err(err).Msg("dispatcher: failed to load job for notification")
		return
	}

	// Skip if job is no longer pending (already dispatched, completed, cancelled, etc.).
	if job.Status != types.JobStatusPending {
		d.logger.Debug().String("job_id", msg.JobId).String("status", string(job.Status)).Msg("dispatcher: job not pending, skipping notification")
		return
	}

	dispatchMsg := messages.JobToDispatchMsg(job, job.Attempt)
	if d.onDispatchJob(ctx, dispatchMsg) {
		// Only record dedup after successful dispatch so that retry
		// notifications are not suppressed when no node had capacity.
		d.recentDispatches[msg.JobId] = time.Now()
	}
}

// onWorkerStatus updates the node capacity table.
func (d *DispatcherActor) onWorkerStatus(ctx *actor.Context, msg *pb.WorkerStatusMsg) {
	ns, ok := d.nodeCapacity[msg.NodeId]
	if !ok {
		ns = &nodeStatus{}
		d.nodeCapacity[msg.NodeId] = ns
	}

	// Use the sender PID as the WorkerSupervisor PID for this node.
	if sender := ctx.Sender(); sender != nil {
		ns.pid = sender
	}

	ns.runningWorkers = int(msg.RunningWorkers)
	ns.idleWorkers = int(msg.IdleWorkers)
	ns.maxConcurrency = int(msg.MaxConcurrency)
	ns.taskTypes = msg.TaskTypes
	ns.lastUpdate = time.Now()
}

// onRejectJob tries to dispatch the rejected job to another node.
func (d *DispatcherActor) onRejectJob(ctx *actor.Context, msg *pb.RejectJobMsg) {
	d.logger.Warn().String("job_id", msg.JobId).String("run_id", msg.RunId).String("reason", string(msg.Reason)).Msg("dispatcher: job rejected by worker")

	// Re-load the job and try dispatching to a different node.
	bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	job, _, err := d.store.GetJob(bgCtx, msg.JobId)
	if err != nil {
		d.logger.Error().String("job_id", msg.JobId).Err(err).Msg("dispatcher: failed to reload rejected job")
		return
	}

	dispatchMsg := messages.JobToDispatchMsg(job, job.Attempt)
	d.onDispatchJob(ctx, dispatchMsg)
}

// onStaleCheck removes nodes that haven't reported in more than nodeStaleThreshold
// and evicts expired entries from the dispatch dedup map.
func (d *DispatcherActor) onStaleCheck() {
	now := time.Now()
	for nodeID, ns := range d.nodeCapacity {
		if now.Sub(ns.lastUpdate) > nodeStaleThreshold {
			d.logger.Warn().String("node_id", nodeID).Time("last_update", ns.lastUpdate).Msg("dispatcher: removing stale node")
			delete(d.nodeCapacity, nodeID)
		}
	}

	// Evict expired dedup entries.
	for jobID, dispatchedAt := range d.recentDispatches {
		if now.Sub(dispatchedAt) > dispatchDedupTTL {
			delete(d.recentDispatches, jobID)
		}
	}
}

// ---------------------------------------------------------------------------
// Node selection
// ---------------------------------------------------------------------------

// selectNode picks the node with the most idle workers that supports the
// given task type. Returns nil if no suitable node is found.
func (d *DispatcherActor) selectNode(taskType string) *nodeStatus {
	var best *nodeStatus

	for _, ns := range d.nodeCapacity {
		if ns.pid == nil {
			continue
		}
		if ns.idleWorkers <= 0 {
			continue
		}
		if !hasTaskType(ns.taskTypes, taskType) {
			continue
		}
		if best == nil || ns.idleWorkers > best.idleWorkers {
			best = ns
		}
	}

	return best
}

// hasTaskType checks if the slice contains the given task type.
// Supports both exact matches ("email.send") and pattern matches ("image.*").
func hasTaskType(types []string, target string) bool {
	for _, t := range types {
		if t == target {
			return true
		}
		// Pattern match: "image.*" matches "image.download_template".
		if strings.HasSuffix(t, "*") {
			prefix := strings.TrimSuffix(t, "*")
			if strings.HasPrefix(target, prefix) {
				return true
			}
		}
	}
	return false
}
