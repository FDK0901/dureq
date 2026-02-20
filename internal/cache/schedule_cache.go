package cache

import (
	"context"
	"sync"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

// ScheduleStore is the interface the cache wraps (satisfied by store.RedisStore).
type ScheduleStore interface {
	ListDueSchedules(ctx context.Context, cutoff time.Time) ([]*types.ScheduleEntry, error)
	GetJob(ctx context.Context, jobID string) (*types.Job, uint64, error)
}

// CacheConfig configures cache TTLs and sizes.
type CacheConfig struct {
	ScheduleTTL time.Duration // default 300ms
	JobTTL      time.Duration // default 5s
	MaxJobCache int           // default 1000
}

// DefaultCacheConfig returns sensible defaults.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		ScheduleTTL: 300 * time.Millisecond,
		JobTTL:      5 * time.Second,
		MaxJobCache: 1000,
	}
}

type jobCacheEntry struct {
	job       *types.Job
	rev       uint64
	expiresAt time.Time
}

// ScheduleCache wraps ListDueSchedules and GetJob with short-TTL in-memory caching.
// Safe for concurrent use. Designed for single-scheduler-per-process scenarios.
type ScheduleCache struct {
	store ScheduleStore
	cfg   CacheConfig

	// Due schedule cache (single entry — one query pattern).
	schedMu      sync.Mutex
	schedEntries []*types.ScheduleEntry
	schedExpiry  time.Time

	// Job cache: jobID → cached result.
	jobMu    sync.RWMutex
	jobCache map[string]*jobCacheEntry
}

// NewScheduleCache creates a cache wrapping the given store.
func NewScheduleCache(store ScheduleStore, cfg CacheConfig) *ScheduleCache {
	if cfg.ScheduleTTL <= 0 {
		cfg.ScheduleTTL = 300 * time.Millisecond
	}
	if cfg.JobTTL <= 0 {
		cfg.JobTTL = 5 * time.Second
	}
	if cfg.MaxJobCache <= 0 {
		cfg.MaxJobCache = 1000
	}
	return &ScheduleCache{
		store:    store,
		cfg:      cfg,
		jobCache: make(map[string]*jobCacheEntry),
	}
}

// ListDueSchedules returns cached results if TTL has not expired,
// otherwise fetches from the underlying store.
func (c *ScheduleCache) ListDueSchedules(ctx context.Context, cutoff time.Time) ([]*types.ScheduleEntry, error) {
	c.schedMu.Lock()
	defer c.schedMu.Unlock()

	now := time.Now()
	if c.schedEntries != nil && now.Before(c.schedExpiry) {
		return c.schedEntries, nil
	}

	entries, err := c.store.ListDueSchedules(ctx, cutoff)
	if err != nil {
		return nil, err
	}

	c.schedEntries = entries
	c.schedExpiry = now.Add(c.cfg.ScheduleTTL)
	return entries, nil
}

// GetJob returns a cached job or fetches from the underlying store.
func (c *ScheduleCache) GetJob(ctx context.Context, jobID string) (*types.Job, uint64, error) {
	c.jobMu.RLock()
	if entry, ok := c.jobCache[jobID]; ok && time.Now().Before(entry.expiresAt) {
		c.jobMu.RUnlock()
		return entry.job, entry.rev, nil
	}
	c.jobMu.RUnlock()

	job, rev, err := c.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, 0, err
	}

	c.jobMu.Lock()
	// Evict expired entries if cache is full.
	if len(c.jobCache) >= c.cfg.MaxJobCache {
		now := time.Now()
		for id, e := range c.jobCache {
			if now.After(e.expiresAt) {
				delete(c.jobCache, id)
			}
		}
	}
	c.jobCache[jobID] = &jobCacheEntry{
		job:       job,
		rev:       rev,
		expiresAt: time.Now().Add(c.cfg.JobTTL),
	}
	c.jobMu.Unlock()

	return job, rev, nil
}

// InvalidateSchedules clears the due-schedule cache.
func (c *ScheduleCache) InvalidateSchedules() {
	c.schedMu.Lock()
	c.schedEntries = nil
	c.schedExpiry = time.Time{}
	c.schedMu.Unlock()
}

// InvalidateJob removes a specific job from the job cache.
func (c *ScheduleCache) InvalidateJob(jobID string) {
	c.jobMu.Lock()
	delete(c.jobCache, jobID)
	c.jobMu.Unlock()
}
