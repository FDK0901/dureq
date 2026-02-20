package store

import (
	"context"
	"time"

	gochainedlog "github.com/FDK0901/go-chainedlog"
)

// DelayedPoller polls delayed sorted sets and moves ripe messages back to work streams.
type DelayedPoller struct {
	store    *RedisStore
	logger   gochainedlog.Logger
	interval time.Duration
	maxMove  int
}

// NewDelayedPoller creates a delayed retry poller.
func NewDelayedPoller(store *RedisStore, logger gochainedlog.Logger) *DelayedPoller {
	return &DelayedPoller{
		store:    store,
		logger:   logger,
		interval: 1 * time.Second,
		maxMove:  100,
	}
}

// Start begins polling in the background. Blocks until ctx is cancelled.
func (p *DelayedPoller) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.poll(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (p *DelayedPoller) poll(ctx context.Context) {
	for _, tier := range p.store.cfg.Tiers {
		moved, err := p.store.MoveDelayedToStream(ctx, tier.Name, p.maxMove)
		if err != nil {
			p.logger.Warn().Err(err).String("tier", tier.Name).Msg("delayed-poller: failed to move messages")
			continue
		}
		if moved > 0 {
			p.logger.Debug().Int64("moved", moved).String("tier", tier.Name).Msg("delayed-poller: moved ripe messages to work stream")
		}
	}
}
