package store

import (
	"context"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	gochainedlog "github.com/FDK0901/go-chainedlog"
)

// DelayedNotifyPoller polls delayed sorted sets and re-notifies ripe jobs
// via PublishJobNotification (actor push path) instead of XADD (v1 pull path).
type DelayedNotifyPoller struct {
	store    *RedisStore
	logger   gochainedlog.Logger
	interval time.Duration
	maxMove  int
}

// NewDelayedNotifyPoller creates a delayed retry poller for the actor-based server.
func NewDelayedNotifyPoller(store *RedisStore, logger gochainedlog.Logger) *DelayedNotifyPoller {
	return &DelayedNotifyPoller{
		store:    store,
		logger:   logger,
		interval: 1 * time.Second,
		maxMove:  100,
	}
}

// Start begins polling in the background. Blocks until ctx is cancelled.
func (p *DelayedNotifyPoller) Start(ctx context.Context) {
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

func (p *DelayedNotifyPoller) poll(ctx context.Context) {
	for _, tier := range p.store.cfg.Tiers {
		moved, err := p.store.MoveDelayedToNotify(ctx, tier.Name, p.maxMove)
		if err != nil {
			p.logger.Warn().Err(err).String("tier", tier.Name).Msg("delayed-notify-poller: failed to process delayed messages")
			continue
		}
		if moved > 0 {
			p.logger.Debug().Int64("moved", moved).String("tier", tier.Name).Msg("delayed-notify-poller: re-notified ripe delayed jobs")
		}
	}
}

// delayedEntry is the JSON shape stored in the delayed sorted set by AddDelayed.
type delayedEntry struct {
	JobID    string `json:"job_id"`
	TaskType string `json:"task_type"`
	Priority int    `json:"priority"`
}

// MoveDelayedToNotify atomically pops ripe entries from the delayed sorted set
// and re-dispatches them via PublishJobNotification (actor push path).
// Uses a Lua script for atomic ZRANGEBYSCORE+ZREM to prevent duplicate
// processing across multiple poller instances on different nodes.
func (s *RedisStore) MoveDelayedToNotify(ctx context.Context, tierName string, maxMove int) (int64, error) {
	key := DelayedKey(s.prefix, tierName)
	nowScore := strconv.FormatFloat(float64(time.Now().UnixNano()), 'f', 0, 64)

	// Atomically pop ripe entries (read + remove in one Lua call).
	members, err := s.scriptPopDelayed.Exec(ctx, s.rdb,
		[]string{key},
		[]string{nowScore, strconv.Itoa(maxMove)},
	).AsStrSlice()
	if err != nil {
		return 0, nil // empty result or redis nil
	}
	if len(members) == 0 {
		return 0, nil
	}

	var moved int64
	for _, raw := range members {
		var entry delayedEntry
		if err := sonic.ConfigFastest.Unmarshal([]byte(raw), &entry); err != nil {
			continue // already removed from set by Lua
		}

		if err := s.PublishJobNotification(ctx, entry.JobID, entry.TaskType, entry.Priority); err != nil {
			// Entry already removed from sorted set. The job remains in store
			// with "pending" status and can be retried manually or picked up
			// by the scheduler on the next tick.
			continue
		}
		moved++
	}

	return moved, nil
}
