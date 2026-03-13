package aggregation

import (
	"context"
	"encoding/json"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
)

// Config configures the group aggregation processor.
type Config struct {
	Store      *store.RedisStore
	Dispatcher *dispatcher.Dispatcher
	Group      types.GroupConfig
	Logger     chainedlog.Logger
}

// Processor periodically flushes ready groups and dispatches the
// aggregated tasks for processing.
type Processor struct {
	store      *store.RedisStore
	dispatcher *dispatcher.Dispatcher
	cfg        types.GroupConfig
	logger     chainedlog.Logger
	cancel     context.CancelFunc
}

// New creates a new group aggregation processor.
func New(cfg Config) *Processor {
	if cfg.Logger == nil {
		cfg.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	gc := cfg.Group
	if gc.GracePeriod == 0 {
		gc.GracePeriod = 5 * time.Second
	}
	if gc.MaxDelay == 0 {
		gc.MaxDelay = 30 * time.Second
	}
	if gc.MaxSize == 0 {
		gc.MaxSize = 100
	}
	return &Processor{
		store:      cfg.Store,
		dispatcher: cfg.Dispatcher,
		cfg:        gc,
		logger:     cfg.Logger,
	}
}

// Start begins the flush loop. Runs until context is cancelled.
func (p *Processor) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)
	go p.loop(ctx)
}

// Stop halts the flush loop.
func (p *Processor) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *Processor) loop(ctx context.Context) {
	// Check every second for groups ready to flush.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.flushReady(ctx)
		}
	}
}

func (p *Processor) flushReady(ctx context.Context) {
	if p.cfg.Aggregator == nil {
		return
	}

	groups, err := p.store.ListReadyGroups(ctx, p.cfg.GracePeriod, p.cfg.MaxDelay, p.cfg.MaxSize)
	if err != nil {
		p.logger.Warn().Err(err).Msg("aggregation: failed to list ready groups")
		return
	}

	for _, group := range groups {
		// Atomic read+delete via Lua script. Only one node wins the flush;
		// concurrent callers on other nodes get an empty result.
		msgs, err := p.store.FlushGroup(ctx, group)
		if err != nil {
			p.logger.Warn().String("group", group).Err(err).Msg("aggregation: failed to flush group")
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		// Collect payloads.
		payloads := make([]json.RawMessage, len(msgs))
		for i, m := range msgs {
			payloads[i] = m.Payload
		}

		// Aggregate.
		aggregated, err := p.cfg.Aggregator.Aggregate(group, payloads)
		if err != nil {
			p.logger.Warn().String("group", group).Int("count", len(msgs)).Err(err).Msg("aggregation: aggregate failed")
			continue
		}

		// Create and dispatch a synthetic job for the aggregated payload.
		taskType := msgs[0].TaskType
		now := time.Now()
		job := &types.Job{
			ID:        "agg-" + group + "-" + now.Format("20060102150405"),
			TaskType:  taskType,
			Payload:   aggregated,
			Status:    types.JobStatusPending,
			CreatedAt: now,
		}

		// CreateJob (not SaveJob) to prevent upsert overwrites from duplicate flushes.
		if _, err := p.store.CreateJob(ctx, job); err != nil {
			p.logger.Warn().String("group", group).Err(err).Msg("aggregation: failed to create aggregated job")
			continue
		}

		if err := p.dispatcher.Dispatch(ctx, job, 0); err != nil {
			p.logger.Warn().String("group", group).Err(err).Msg("aggregation: failed to dispatch aggregated job")
			continue
		}

		p.logger.Info().String("group", group).Int("count", len(msgs)).String("job_id", job.ID).Msg("aggregation: flushed and dispatched")
	}
}
