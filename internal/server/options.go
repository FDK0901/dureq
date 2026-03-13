package server

import (
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
)

// WithGroupAggregation enables task group aggregation with the given configuration.
func WithGroupAggregation(cfg types.GroupConfig) Option {
	return func(c *Config) { c.GroupConfig = &cfg }
}

// WithRetentionPeriod sets how long completed/dead/cancelled jobs are kept before archival cleanup.
// Default: 7 days. Set to 0 to disable archival.
func WithRetentionPeriod(d time.Duration) Option {
	return func(c *Config) { c.RetentionPeriod = &d }
}

// Mode controls which subsystems are started.
type Mode int

const (
	ModeQueue     Mode = 1 << 0 // worker + dispatcher
	ModeScheduler Mode = 1 << 1 // scheduler (leader-only)
	ModeWorkflow  Mode = 1 << 2 // workflow orchestrator (leader-only)
	ModeMonitor   Mode = 1 << 3 // HTTP monitoring API
	ModeFull      Mode = ModeQueue | ModeScheduler | ModeWorkflow | ModeMonitor
)

// Has returns true if the mode includes the given flag.
func (m Mode) Has(flag Mode) bool { return m&flag != 0 }

// Option configures the server.
type Option func(*Config)

// Config holds all server configuration.
type Config struct {
	// Redis connection options.
	RedisOptions types.RedisOptions

	// NodeID uniquely identifies this server node. Auto-generated if empty.
	NodeID string

	// MaxConcurrency is the overall worker pool size per node. Default: 100.
	MaxConcurrency int

	// SchedulerTickInterval is how often the leader scans for due schedules. Default: 1s.
	SchedulerTickInterval time.Duration

	// HeartbeatInterval is how often nodes report health. Default: 5s.
	HeartbeatInterval time.Duration

	// ElectionTTL is the leader election key TTL. Default: 10s.
	ElectionTTL time.Duration

	// LockTTL is the distributed lock TTL. Default: 30s.
	LockTTL time.Duration

	// LockAutoExtend is the interval for auto-extending locks. Default: LockTTL/3.
	LockAutoExtend time.Duration

	// NodeTTL is the TTL for the node heartbeat KV entry. Default: 15s.
	NodeTTL time.Duration

	// ShutdownTimeout is the grace period for in-flight tasks before abort.
	// After this duration, workers will requeue their messages and exit.
	// Default: 30s.
	ShutdownTimeout time.Duration

	// StoreConfig holds Redis store configuration.
	StoreConfig store.RedisStoreConfig

	// GroupConfig enables task group aggregation. Nil = disabled.
	GroupConfig *types.GroupConfig

	// RetentionPeriod controls how long completed/dead/cancelled jobs are kept.
	// Nil = default (7 days). Set to pointer to 0 to disable archival.
	RetentionPeriod *time.Duration

	// Logger is the structured logger. Default: slog.Default().
	Logger chainedlog.Logger

	// Mode selects which subsystems are active. Default: ModeFull.
	Mode Mode
}

func (c *Config) defaults() {
	if c.Mode == 0 {
		c.Mode = ModeFull
	}
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = 100
	}
	if c.SchedulerTickInterval == 0 {
		c.SchedulerTickInterval = 1 * time.Second
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.ElectionTTL == 0 {
		c.ElectionTTL = 10 * time.Second
	}
	if c.LockTTL == 0 {
		c.LockTTL = 30 * time.Second
	}
	if c.LockAutoExtend == 0 {
		c.LockAutoExtend = c.LockTTL / 3
	}
	if c.NodeTTL == 0 {
		c.NodeTTL = 15 * time.Second
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}

	// Propagate to store config.
	c.StoreConfig.ElectionTTL = c.ElectionTTL
	c.StoreConfig.LockTTL = c.LockTTL
	c.StoreConfig.NodeTTL = c.NodeTTL
}

// WithRedisURL sets the Redis server URL.
func WithRedisURL(url string) Option {
	return func(c *Config) { c.RedisOptions.URL = url }
}

// WithRedisUsername sets the Redis ACL username (Redis 6+).
func WithRedisUsername(username string) Option {
	return func(c *Config) { c.RedisOptions.Username = username }
}

// WithRedisPassword sets the Redis password.
func WithRedisPassword(password string) Option {
	return func(c *Config) { c.RedisOptions.Password = password }
}

// WithRedisDB sets the Redis database number.
func WithRedisDB(db int) Option {
	return func(c *Config) { c.RedisOptions.DB = db }
}

// WithRedisPoolSize sets the Redis connection pool size.
func WithRedisPoolSize(n int) Option {
	return func(c *Config) { c.RedisOptions.PoolSize = n }
}

func WithNodeID(id string) Option {
	return func(c *Config) { c.NodeID = id }
}

func WithMaxConcurrency(n int) Option {
	return func(c *Config) { c.MaxConcurrency = n }
}

func WithSchedulerTickInterval(d time.Duration) Option {
	return func(c *Config) { c.SchedulerTickInterval = d }
}

func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *Config) { c.HeartbeatInterval = d }
}

func WithElectionTTL(d time.Duration) Option {
	return func(c *Config) { c.ElectionTTL = d }
}

func WithLockTTL(d time.Duration) Option {
	return func(c *Config) { c.LockTTL = d }
}

func WithLogger(l chainedlog.Logger) Option {
	return func(c *Config) { c.Logger = l }
}

func WithShutdownTimeout(d time.Duration) Option {
	return func(c *Config) { c.ShutdownTimeout = d }
}

// WithPriorityTiers sets user-defined priority tiers. Each tier becomes its own Redis Stream.
func WithPriorityTiers(tiers []store.TierConfig) Option {
	return func(c *Config) { c.StoreConfig.Tiers = tiers }
}

func WithKeyPrefix(prefix string) Option {
	return func(c *Config) { c.StoreConfig.KeyPrefix = prefix }
}

// WithMode sets which subsystems are active on this node.
// Default: ModeFull. Example: WithMode(ModeQueue | ModeScheduler).
func WithMode(m Mode) Option {
	return func(c *Config) { c.Mode = m }
}

// WithRedisSentinel configures Redis Sentinel failover mode.
func WithRedisSentinel(masterName string, sentinelAddrs []string, sentinelPassword string) Option {
	return func(c *Config) {
		c.RedisOptions.SentinelConfig = &types.RedisSentinelConfig{
			MasterName:       masterName,
			SentinelAddrs:    sentinelAddrs,
			SentinelPassword: sentinelPassword,
		}
	}
}

// WithRedisCluster configures Redis Cluster mode with the given node addresses.
// Both single-shard and multi-shard clusters are supported. All multi-key Lua
// scripts use hash-tagged keys to ensure slot co-location.
func WithRedisCluster(addrs []string) Option {
	return func(c *Config) {
		c.RedisOptions.ClusterAddrs = addrs
	}
}
