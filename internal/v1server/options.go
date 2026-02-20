package v1server

import (
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
)

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

	// Logger is the structured logger. Default: slog.Default().
	Logger gochainedlog.Logger
}

func (c *Config) defaults() {
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

func WithLogger(l gochainedlog.Logger) Option {
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
// NOTE: only single-shard (single-master) clusters are supported. dureq uses
// multi-key Lua scripts whose keys span different hash slots, so multi-shard
// clusters will fail with CROSSSLOT errors.
func WithRedisCluster(addrs []string) Option {
	return func(c *Config) {
		c.RedisOptions.ClusterAddrs = addrs
	}
}
