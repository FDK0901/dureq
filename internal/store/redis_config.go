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

// ResolveTier maps a numeric priority (1-10) to a configured tier name.
// Higher priority → first tier (highest weight), lower → last tier.
func ResolveTier(tiers []TierConfig, priority int) string {
	if len(tiers) == 0 {
		return "normal"
	}
	if len(tiers) == 1 {
		return tiers[0].Name
	}

	p := priority
	if p <= 0 {
		p = 5
	}
	if p > 10 {
		p = 10
	}

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

// DefaultTiers returns a sensible default tier configuration.
func DefaultTiers() []TierConfig {
	return []TierConfig{
		{Name: "high", Weight: 100, FetchBatch: 10},
		{Name: "normal", Weight: 50, FetchBatch: 10},
		{Name: "low", Weight: 10, FetchBatch: 5},
	}
}
