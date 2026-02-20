package actors

import (
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	chainedslog "github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/anthdm/hollywood/actor"
	"github.com/anthdm/hollywood/cluster"
)

const (
	singletonCheckInterval = 10 * time.Second

	KindScheduler    = "scheduler"
	KindOrchestrator = "orchestrator"
	KindDispatcher   = "dispatcher"
)

// singletonCheckMsg is a local tick message for periodic singleton health checks.
type singletonCheckMsg struct{}

// ClusterGuardActor runs one per node. It activates the cluster singleton
// actors (Scheduler, Orchestrator, Dispatcher) and monitors their health.
// If a singleton becomes unreachable (e.g. the hosting member crashed),
// it re-activates the singleton so the cluster auto-places it on a live member.
type ClusterGuardActor struct {
	cluster  *cluster.Cluster
	repeater actor.SendRepeater
	logger   gochainedlog.Logger

	// Tracked singleton PIDs.
	schedulerPID    *actor.PID
	orchestratorPID *actor.PID
	dispatcherPID   *actor.PID

	// Callbacks for wiring singleton PIDs to local actors.
	onSingletonsReady func(scheduler, orchestrator, dispatcher *actor.PID)
}

// NewClusterGuardActor returns a Hollywood Producer that creates a ClusterGuardActor.
func NewClusterGuardActor(c *cluster.Cluster, onReady func(scheduler, orchestrator, dispatcher *actor.PID), logger gochainedlog.Logger) actor.Producer {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return func() actor.Receiver {
		return &ClusterGuardActor{
			cluster:           c,
			onSingletonsReady: onReady,
			logger:            logger,
		}
	}
}

// Receive implements actor.Receiver.
func (g *ClusterGuardActor) Receive(ctx *actor.Context) {
	switch ctx.Message().(type) {
	case actor.Started:
		g.onStarted(ctx)
	case actor.Stopped:
		g.onStopped()
	case singletonCheckMsg:
		g.onSingletonCheck(ctx)
	case *pb.SingletonCheckMsg:
		// Response to a ping — the singleton is alive.
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (g *ClusterGuardActor) onStarted(ctx *actor.Context) {
	g.activateSingletons()
	g.repeater = ctx.SendRepeat(ctx.PID(), singletonCheckMsg{}, singletonCheckInterval)

	g.logger.Info().String("scheduler", g.schedulerPID.String()).String("orchestrator", g.orchestratorPID.String()).String("dispatcher", g.dispatcherPID.String()).Msg("cluster guard actor started")
}

func (g *ClusterGuardActor) onStopped() {
	g.repeater.Stop()
	g.logger.Info().Msg("cluster guard actor stopped")
}

// ---------------------------------------------------------------------------
// Singleton management
// ---------------------------------------------------------------------------

func (g *ClusterGuardActor) activateSingletons() {
	config := cluster.NewActivationConfig().WithID("singleton")

	g.schedulerPID = g.cluster.Activate(KindScheduler, config)
	g.orchestratorPID = g.cluster.Activate(KindOrchestrator, config)
	g.dispatcherPID = g.cluster.Activate(KindDispatcher, config)

	if g.onSingletonsReady != nil {
		g.onSingletonsReady(g.schedulerPID, g.orchestratorPID, g.dispatcherPID)
	}
}

func (g *ClusterGuardActor) onSingletonCheck(ctx *actor.Context) {
	reactivated := false

	if g.schedulerPID == nil || !g.isAlive(ctx, g.schedulerPID) {
		g.logger.Warn().Msg("cluster guard: scheduler singleton missing, re-activating")
		g.schedulerPID = g.cluster.Activate(KindScheduler,
			cluster.NewActivationConfig().WithID("singleton"))
		reactivated = true
	}

	if g.orchestratorPID == nil || !g.isAlive(ctx, g.orchestratorPID) {
		g.logger.Warn().Msg("cluster guard: orchestrator singleton missing, re-activating")
		g.orchestratorPID = g.cluster.Activate(KindOrchestrator,
			cluster.NewActivationConfig().WithID("singleton"))
		reactivated = true
	}

	if g.dispatcherPID == nil || !g.isAlive(ctx, g.dispatcherPID) {
		g.logger.Warn().Msg("cluster guard: dispatcher singleton missing, re-activating")
		g.dispatcherPID = g.cluster.Activate(KindDispatcher,
			cluster.NewActivationConfig().WithID("singleton"))
		reactivated = true
	}

	if reactivated && g.onSingletonsReady != nil {
		g.onSingletonsReady(g.schedulerPID, g.orchestratorPID, g.dispatcherPID)
	}
}

// isAlive sends a ping to the given PID and checks if it responds within
// a short timeout.
func (g *ClusterGuardActor) isAlive(ctx *actor.Context, pid *actor.PID) bool {
	resp, err := ctx.Engine().Request(pid, &pb.SingletonCheckMsg{}, 2*time.Second).Result()
	if err != nil {
		return false
	}
	_ = resp
	return true
}
