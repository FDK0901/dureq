package server

import (
	"context"
	"sync"

	"github.com/FDK0901/dureq/internal/messages"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/anthdm/hollywood/actor"
)

// actorDispatcher implements monitor.Dispatcher for the v2 actor model.
// Dispatch uses PublishJobNotification (triggers NotifierActor → DispatcherActor push path)
// instead of DispatchWork (Redis Streams pull path that actor workers don't read).
type actorDispatcher struct {
	store  *store.RedisStore
	logger gochainedlog.Logger

	mu              sync.RWMutex
	engine          *actor.Engine
	orchestratorPID *actor.PID
}

func (d *actorDispatcher) Dispatch(ctx context.Context, job *types.Job, _ int) error {
	priority := int(types.PriorityNormal)
	if job.Priority != nil {
		priority = int(*job.Priority)
	}
	return d.store.PublishJobNotification(ctx, job.ID, string(job.TaskType), priority)
}

func (d *actorDispatcher) PublishEvent(event types.JobEvent) {
	ctx := context.Background()
	if err := d.store.PublishEvent(ctx, event); err != nil {
		d.logger.Warn().String("type", string(event.Type)).Err(err).Msg("failed to publish event")
	}

	// Forward orchestrator-relevant events through the actor system.
	// The orchestrator is a cluster singleton that only receives DomainEventMsg
	// via the actor network — Redis pub/sub alone does not reach it at runtime.
	d.mu.RLock()
	eng := d.engine
	pid := d.orchestratorPID
	d.mu.RUnlock()

	if eng != nil && pid != nil {
		switch event.Type {
		case types.EventBatchRetrying, types.EventWorkflowRetrying:
			eng.Send(pid, messages.JobEventToDomainEvent(event))
		}
	}
}

// SetOrchestratorPID wires the orchestrator singleton PID so that API-triggered
// events (batch retry, workflow retry) can be forwarded to it.
func (d *actorDispatcher) SetOrchestratorPID(engine *actor.Engine, pid *actor.PID) {
	d.mu.Lock()
	d.engine = engine
	d.orchestratorPID = pid
	d.mu.Unlock()
}
