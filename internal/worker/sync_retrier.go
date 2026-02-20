package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	gochainedlog "github.com/FDK0901/go-chainedlog"
)

// syncRequest represents a failed Redis write operation that should be retried.
type syncRequest struct {
	fn       func() error
	desc     string
	deadline time.Time
	retries  int
}

// SyncRetryStats exposes retrier state for the monitoring API.
type SyncRetryStats struct {
	Pending int64           `json:"pending"`
	Failed  int64           `json:"failed"`
	Recent  []SyncRetryItem `json:"recent"`
}

// SyncRetryItem represents a single failed operation for display in the UI.
type SyncRetryItem struct {
	Description string    `json:"description"`
	FailedAt    time.Time `json:"failed_at"`
	Retries     int       `json:"retries"`
}

// SyncRetrier retries failed Redis write operations in the background with
// exponential backoff. Pending/failed counters are exposed for the monitoring UI.
type SyncRetrier struct {
	ch      chan *syncRequest
	pending atomic.Int64
	failed  atomic.Int64
	logger  gochainedlog.Logger

	mu     sync.Mutex
	recent []SyncRetryItem // ring buffer of recent failures (max 20)
}

// NewSyncRetrier creates a SyncRetrier and starts its background loop.
func NewSyncRetrier(ctx context.Context, logger gochainedlog.Logger) *SyncRetrier {
	sr := &SyncRetrier{
		ch:     make(chan *syncRequest, 256),
		logger: logger,
	}
	go sr.loop(ctx)
	return sr
}

// Submit enqueues a failed operation for async retry.
func (sr *SyncRetrier) Submit(req *syncRequest) {
	sr.pending.Add(1)
	select {
	case sr.ch <- req:
	default:
		// Channel full — count as immediate failure.
		sr.pending.Add(-1)
		sr.failed.Add(1)
		sr.addRecent(SyncRetryItem{
			Description: req.desc,
			FailedAt:    time.Now(),
			Retries:     req.retries,
		})
		sr.logger.Error().String("op", req.desc).Msg("sync_retrier: channel full, dropping request")
	}
}

// Stats returns a snapshot of the current retrier state.
func (sr *SyncRetrier) Stats() SyncRetryStats {
	sr.mu.Lock()
	recentCopy := make([]SyncRetryItem, len(sr.recent))
	copy(recentCopy, sr.recent)
	sr.mu.Unlock()

	return SyncRetryStats{
		Pending: sr.pending.Load(),
		Failed:  sr.failed.Load(),
		Recent:  recentCopy,
	}
}

func (sr *SyncRetrier) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			sr.drain()
			return
		case req := <-sr.ch:
			sr.process(ctx, req)
		}
	}
}

func (sr *SyncRetrier) process(ctx context.Context, req *syncRequest) {
	backoff := 500 * time.Millisecond
	const maxBackoff = 30 * time.Second

	for {
		if time.Now().After(req.deadline) {
			sr.pending.Add(-1)
			sr.failed.Add(1)
			sr.addRecent(SyncRetryItem{
				Description: req.desc,
				FailedAt:    time.Now(),
				Retries:     req.retries,
			})
			sr.logger.Error().String("op", req.desc).Int("retries", req.retries).Msg("sync_retrier: deadline exceeded, giving up")
			return
		}

		req.retries++
		if err := req.fn(); err != nil {
			sr.logger.Warn().String("op", req.desc).Int("retry", req.retries).Err(err).Msg("sync_retrier: retry failed")

			select {
			case <-ctx.Done():
				sr.pending.Add(-1)
				sr.failed.Add(1)
				sr.addRecent(SyncRetryItem{
					Description: req.desc,
					FailedAt:    time.Now(),
					Retries:     req.retries,
				})
				return
			case <-time.After(backoff):
			}
			backoff = backoff * 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Success.
		sr.pending.Add(-1)
		sr.logger.Info().String("op", req.desc).Int("retries", req.retries).Msg("sync_retrier: succeeded")
		return
	}
}

// drain processes remaining requests on shutdown with a short deadline.
func (sr *SyncRetrier) drain() {
	for {
		select {
		case req := <-sr.ch:
			if err := req.fn(); err != nil {
				sr.pending.Add(-1)
				sr.failed.Add(1)
				sr.addRecent(SyncRetryItem{
					Description: req.desc,
					FailedAt:    time.Now(),
					Retries:     req.retries,
				})
			} else {
				sr.pending.Add(-1)
			}
		default:
			return
		}
	}
}

func (sr *SyncRetrier) addRecent(item SyncRetryItem) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.recent = append(sr.recent, item)
	if len(sr.recent) > 20 {
		sr.recent = sr.recent[len(sr.recent)-20:]
	}
}

// syncRetryAck wraps AckMessage with SyncRetrier fallback.
func syncRetryAck(sr *SyncRetrier, fn func() error, tierName, msgID string) {
	if err := fn(); err != nil && sr != nil {
		sr.Submit(&syncRequest{
			fn:       fn,
			desc:     fmt.Sprintf("ack %s/%s", tierName, msgID),
			deadline: time.Now().Add(5 * time.Minute),
		})
	}
}

// syncRetrySave wraps a Redis save operation with SyncRetrier fallback.
func syncRetrySave(sr *SyncRetrier, fn func() error, desc string) {
	if err := fn(); err != nil && sr != nil {
		sr.Submit(&syncRequest{
			fn:       fn,
			desc:     desc,
			deadline: time.Now().Add(5 * time.Minute),
		})
	}
}
