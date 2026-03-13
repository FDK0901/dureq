package archival

import (
	"context"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
)

// Config configures the archival cleaner.
type Config struct {
	Store *store.RedisStore
	// RetentionPeriod is how long completed/failed/dead jobs are kept.
	// Jobs older than this are deleted. Default: 7 days.
	RetentionPeriod time.Duration
	// ScanInterval is how often the cleaner runs. Default: 1 hour.
	ScanInterval time.Duration
	// BatchSize is the number of jobs to process per scan iteration. Default: 100.
	BatchSize int
	Logger    chainedlog.Logger
}

// Cleaner periodically removes completed jobs older than the retention period.
type Cleaner struct {
	store           *store.RedisStore
	retentionPeriod time.Duration
	scanInterval    time.Duration
	batchSize       int
	logger          chainedlog.Logger
	cancel          context.CancelFunc
}

// New creates a new archival cleaner.
func New(cfg Config) *Cleaner {
	if cfg.RetentionPeriod < 0 {
		cfg.RetentionPeriod = 7 * 24 * time.Hour
	}
	if cfg.ScanInterval == 0 {
		cfg.ScanInterval = 1 * time.Hour
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.Logger == nil {
		cfg.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return &Cleaner{
		store:           cfg.Store,
		retentionPeriod: cfg.RetentionPeriod,
		scanInterval:    cfg.ScanInterval,
		batchSize:       cfg.BatchSize,
		logger:          cfg.Logger,
	}
}

// Start begins the archival loop. Non-blocking.
// If RetentionPeriod is 0, archival is disabled and Start is a no-op.
func (c *Cleaner) Start(ctx context.Context) {
	if c.retentionPeriod == 0 {
		return
	}
	ctx, c.cancel = context.WithCancel(ctx)
	go c.loop(ctx)
}

// Stop halts the archival loop.
func (c *Cleaner) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *Cleaner) loop(ctx context.Context) {
	ticker := time.NewTicker(c.scanInterval)
	defer ticker.Stop()

	// Run once immediately.
	c.cleanup(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cleanup(ctx)
		}
	}
}

func (c *Cleaner) cleanup(ctx context.Context) {
	cutoff := time.Now().Add(-c.retentionPeriod)
	deleted := 0

	for _, status := range []types.JobStatus{types.JobStatusCompleted, types.JobStatusFailed, types.JobStatusDead, types.JobStatusCancelled} {
		jobs, err := c.store.ListJobsByStatus(ctx, status)
		if err != nil {
			c.logger.Warn().Err(err).String("status", string(status)).Msg("archival: failed to list jobs")
			continue
		}

		for _, job := range jobs {
			if deleted >= c.batchSize {
				break
			}
			if job.UpdatedAt.Before(cutoff) {
				if err := c.store.DeleteJob(ctx, job.ID); err != nil {
					c.logger.Warn().String("job_id", job.ID).Err(err).Msg("archival: failed to delete expired job")
					continue
				}
				// Clean associated schedule/unique key/audit trail.
				c.store.DeleteSchedule(ctx, job.ID)
				c.store.DeleteJobAuditTrail(ctx, job.ID)
				if job.UniqueKey != nil && *job.UniqueKey != "" {
					c.store.DeleteUniqueKey(ctx, *job.UniqueKey)
				}
				deleted++
			}
		}
	}

	if deleted > 0 {
		c.logger.Info().Int("deleted", deleted).Msg("archival: cleaned up expired jobs")
	}
}
