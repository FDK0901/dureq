package dispatcher

import (
	"context"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/rs/xid"
)

// Dispatcher publishes work messages to Redis Streams
// and events/results via the RedisStore.
type Dispatcher struct {
	store  *store.RedisStore
	logger gochainedlog.Logger
}

// New creates a new dispatcher backed by Redis.
func New(s *store.RedisStore, logger gochainedlog.Logger) *Dispatcher {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return &Dispatcher{
		store:  s,
		logger: logger,
	}
}

// Dispatch publishes a work message for a job to the tier-appropriate Redis Stream.
func (d *Dispatcher) Dispatch(ctx context.Context, job *types.Job, attempt int) error {
	deadline := time.Now().Add(5 * time.Minute)

	priority := types.PriorityNormal
	if job.Priority != nil {
		priority = *job.Priority
	}

	now := time.Now()
	msg := types.WorkMessage{
		RunID:        xid.New().String(),
		JobID:        job.ID,
		TaskType:     job.TaskType,
		Payload:      job.Payload,
		Attempt:      attempt,
		Deadline:     deadline,
		Headers:      job.Headers,
		Priority:     priority,
		DispatchedAt: now,
	}

	// Resolve the tier name from priority.
	tierName := store.ResolveTier(d.store.Config().Tiers, int(priority))

	msgID, err := d.store.DispatchWork(ctx, tierName, &msg)
	if err != nil {
		return err
	}

	d.logger.Debug().
		String("job_id", job.ID).
		String("run_id", msg.RunID).
		String("task_type", string(job.TaskType)).
		String("tier", tierName).
		String("stream_id", msgID).
		Msg("dispatched work")

	return nil
}

// DispatchToDLQ moves a failed job to the dead letter queue.
func (d *Dispatcher) DispatchToDLQ(ctx context.Context, job *types.Job, lastErr string) error {
	msg := types.WorkMessage{
		RunID:    xid.New().String(),
		JobID:    job.ID,
		TaskType: job.TaskType,
		Payload:  job.Payload,
		Attempt:  job.Attempt,
		Metadata: map[string]string{"error": lastErr},
	}

	if err := d.store.DispatchToDLQ(ctx, &msg); err != nil {
		return err
	}

	d.logger.Info().
		String("job_id", job.ID).
		String("task_type", string(job.TaskType)).
		String("error", lastErr).
		Msg("job moved to DLQ")

	return nil
}

// PublishEvent publishes a monitoring event via Pub/Sub + event stream.
func (d *Dispatcher) PublishEvent(event types.JobEvent) {
	ctx := context.Background()
	if err := d.store.PublishEvent(ctx, event); err != nil {
		d.logger.Warn().
			String("type", string(event.Type)).
			Err(err).
			Msg("failed to publish event")
	}
}

// PublishResult stores a completion result and notifies waiting clients.
func (d *Dispatcher) PublishResult(result types.WorkResult) {
	ctx := context.Background()
	if err := d.store.PublishResult(ctx, result); err != nil {
		d.logger.Warn().
			String("job_id", result.JobID).
			Err(err).
			Msg("failed to publish result")
	}
}

// Store returns the underlying RedisStore.
func (d *Dispatcher) Store() *store.RedisStore {
	return d.store
}

// resolveTier delegates to store.ResolveTier for backward compatibility.
func resolveTier(tiers []store.TierConfig, priority types.Priority) string {
	return store.ResolveTier(tiers, int(priority))
}
