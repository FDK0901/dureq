package dispatcher

import (
	"context"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/rs/xid"
)

// Dispatcher publishes work messages to Redis Streams
// and events/results via the RedisStore.
type Dispatcher struct {
	store           *store.RedisStore
	hooks           *types.Hooks
	logger          chainedlog.Logger
	handlerVersions map[types.TaskType]string
}

// New creates a new dispatcher backed by Redis.
func New(s *store.RedisStore, logger chainedlog.Logger) *Dispatcher {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return &Dispatcher{
		store:  s,
		logger: logger,
	}
}

// SetHooks sets the lifecycle hooks that are fired on each event.
func (d *Dispatcher) SetHooks(h *types.Hooks) {
	d.hooks = h
}

// SetHandlerVersions sets the handler version map used to tag dispatched work messages.
// This enables version-aware routing: workers with a mismatched version will skip and re-enqueue.
func (d *Dispatcher) SetHandlerVersions(versions map[types.TaskType]string) {
	d.handlerVersions = versions
}

// Dispatch publishes a work message for a job to the tier-appropriate Redis Stream.
func (d *Dispatcher) Dispatch(ctx context.Context, job *types.Job, attempt int) error {
	priority := types.PriorityNormal
	if job.Priority != nil {
		priority = *job.Priority
	}

	now := time.Now()
	msg := types.WorkMessage{
		RunID:           xid.New().String(),
		JobID:           job.ID,
		TaskType:        job.TaskType,
		Payload:         job.Payload,
		Attempt:         attempt,
		Headers:         job.Headers,
		Priority:        priority,
		DispatchedAt:    now,
		ConcurrencyKeys: job.ConcurrencyKeys,
	}
	// Extract FiringID from headers (set by scheduler for schedule firing dedup).
	if fid, ok := job.Headers["x-dureq-firing-id"]; ok {
		msg.FiringID = fid
	}
	// Tag with handler version for version-aware routing.
	if d.handlerVersions != nil {
		if v, ok := d.handlerVersions[job.TaskType]; ok {
			msg.Version = v
		}
	}

	// Resolve the tier name from priority.
	tierName := resolveTier(d.store.Config().Tiers, priority)

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

// PublishEvent publishes a monitoring event via Pub/Sub + event stream,
// and fires any registered lifecycle hooks.
func (d *Dispatcher) PublishEvent(event types.JobEvent) {
	ctx := context.Background()
	if err := d.store.PublishEvent(ctx, event); err != nil {
		d.logger.Warn().
			String("type", string(event.Type)).
			Err(err).
			Msg("failed to publish event")
	}
	d.hooks.Fire(ctx, event)
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

// resolveTier maps a numeric priority to a configured tier name.
// Uses the tier list (sorted by weight descending): high priority → first tier, etc.
func resolveTier(tiers []store.TierConfig, priority types.Priority) string {
	if len(tiers) == 0 {
		return "normal"
	}
	if len(tiers) == 1 {
		return tiers[0].Name
	}

	// Map priority 1-10 into tier buckets based on weight distribution.
	// Higher priority values → higher weight tiers.
	p := int(priority)
	if p <= 0 {
		p = 5
	}
	if p > 10 {
		p = 10
	}

	// Simple approach: divide the 1-10 range into N equal buckets.
	// Priority 10 → first tier (highest weight), priority 1 → last tier (lowest weight).
	bucketSize := 10 / len(tiers)
	if bucketSize == 0 {
		bucketSize = 1
	}

	idx := (10 - p) / bucketSize
	if idx >= len(tiers) {
		idx = len(tiers) - 1
	}
	return tiers[idx].Name
}
