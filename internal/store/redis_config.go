package store

import "time"

// RedisStoreConfig holds configuration for the Redis store.
type RedisStoreConfig struct {
	// KeyPrefix is prepended to all Redis keys (e.g., "prod" → "prod_dureq:...").
	KeyPrefix string

	// NodeTTL is the TTL for node heartbeat entries. Default: 15s.
	NodeTTL time.Duration

	// ElectionTTL is the TTL for the leader election key. Default: 10s.
	ElectionTTL time.Duration

	// LockTTL is the default TTL for distributed lock entries. Default: 30s.
	LockTTL time.Duration

	// EventStreamMaxLen is the max length of the events stream. Default: 50000.
	EventStreamMaxLen int64

	// DLQStreamMaxLen is the max length of the DLQ stream. Default: 10000.
	DLQStreamMaxLen int64

	// ResultTTL is the TTL for job result entries. Default: 1h.
	ResultTTL time.Duration

	// DedupTTL is the TTL for run ID deduplication keys. Default: 5m.
	DedupTTL time.Duration

	// RequestDedupTTL is the TTL for client-level request idempotency keys.
	// Must exceed the maximum expected client retry window. Default: 1h.
	RequestDedupTTL time.Duration

	// OverlapBufferTTL is the TTL for buffered overlap dispatch entries.
	// Must exceed the longest schedule interval (e.g., monthly cron = 31d).
	// Default: 7d.
	OverlapBufferTTL time.Duration

	// WorkflowResultTTL is the TTL for workflow task result entries.
	// Must exceed the longest workflow execution duration. Default: 7d.
	WorkflowResultTTL time.Duration

	// SideEffectTTL is the TTL for side-effect step cache entries.
	// Must exceed the longest handler execution timeout. Default: 24h.
	SideEffectTTL time.Duration

	// SignalDedupTTL is the TTL for workflow signal deduplication keys.
	// Must exceed the longest workflow lifetime. Default: 30d.
	SignalDedupTTL time.Duration

	// Tiers is the list of user-defined priority tiers. Default: [normal].
	Tiers []TierConfig
}

// TierConfig defines a user-customizable priority tier.
type TierConfig struct {
	// Name is the tier identifier (e.g., "urgent", "normal", "background").
	Name string

	// Weight controls fetch priority. Higher weight = polled first and more frequently.
	Weight int

	// FetchBatch is the number of messages to read per XREADGROUP call. Default: 10.
	FetchBatch int

	// RateLimit, if set, limits how many messages per second can be fetched from this tier.
	// Uses a distributed token bucket. Zero = no rate limit.
	RateLimit float64

	// RateBurst is the maximum burst size for the rate limiter. Default: RateLimit.
	RateBurst int
}

func (c *RedisStoreConfig) defaults() {
	if c.NodeTTL == 0 {
		c.NodeTTL = 15 * time.Second
	}
	if c.ElectionTTL == 0 {
		c.ElectionTTL = 10 * time.Second
	}
	if c.LockTTL == 0 {
		c.LockTTL = 30 * time.Second
	}
	if c.EventStreamMaxLen == 0 {
		c.EventStreamMaxLen = 50000
	}
	if c.DLQStreamMaxLen == 0 {
		c.DLQStreamMaxLen = 10000
	}
	if c.ResultTTL == 0 {
		c.ResultTTL = 1 * time.Hour
	}
	if c.DedupTTL == 0 {
		c.DedupTTL = 5 * time.Minute
	}
	if c.RequestDedupTTL == 0 {
		c.RequestDedupTTL = 1 * time.Hour
	}
	if c.OverlapBufferTTL == 0 {
		c.OverlapBufferTTL = 7 * 24 * time.Hour // 7 days
	}
	if c.WorkflowResultTTL == 0 {
		c.WorkflowResultTTL = 7 * 24 * time.Hour // 7 days
	}
	if c.SideEffectTTL == 0 {
		c.SideEffectTTL = 24 * time.Hour
	}
	if c.SignalDedupTTL == 0 {
		c.SignalDedupTTL = 30 * 24 * time.Hour // 30 days
	}
	if len(c.Tiers) == 0 {
		c.Tiers = []TierConfig{
			{Name: "normal", Weight: 10, FetchBatch: 10},
		}
	}
	for i := range c.Tiers {
		if c.Tiers[i].FetchBatch <= 0 {
			c.Tiers[i].FetchBatch = 10
		}
		if c.Tiers[i].Weight <= 0 {
			c.Tiers[i].Weight = 1
		}
	}
}

func (c *RedisStoreConfig) prefix() string {
	if c.KeyPrefix != "" {
		return c.KeyPrefix + "_" + defaultPrefix
	}
	return defaultPrefix
}

// DefaultTiers returns a sensible default tier configuration.
func DefaultTiers() []TierConfig {
	return []TierConfig{
		{Name: "high", Weight: 100, FetchBatch: 10},
		{Name: "normal", Weight: 50, FetchBatch: 10},
		{Name: "low", Weight: 10, FetchBatch: 5},
	}
}
