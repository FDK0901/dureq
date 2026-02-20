package server

import (
	"context"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
)

// actorDispatcher implements monitor.Dispatcher for the v2 actor model.
// Dispatch uses PublishJobNotification (triggers NotifierActor → DispatcherActor push path)
// instead of DispatchWork (Redis Streams pull path that actor workers don't read).
type actorDispatcher struct {
	store  *store.RedisStore
	logger gochainedlog.Logger
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
}
