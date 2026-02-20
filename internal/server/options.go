package server

import (
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
)

// Option configures the server.
type Option func(*Config)

// Config holds all server configuration for dureq (Hollywood actor-based).
type Config struct {
	// Redis connection options.
	RedisOptions types.RedisOptions

	// NodeID uniquely identifies this server node. Auto-generated if empty.
	NodeID string

	// ListenAddr is the address for Hollywood Remote (dRPC). Default: random port.
	ListenAddr string

	// ClusterRegion is the cluster region identifier. Default: "default".
	ClusterRegion string

	// MaxConcurrency is the overall worker pool size per node. Default: 100.
	MaxConcurrency int

	// ShutdownTimeout is the grace period for in-flight tasks before abort.
	// Default: 30s.
	ShutdownTimeout time.Duration

	// LockTTL is the distributed lock TTL for per-run locks. Default: 30s.
	LockTTL time.Duration

	// LockAutoExtend is the interval for auto-extending per-run locks. Default: LockTTL/3.
	LockAutoExtend time.Duration

	// StoreConfig holds Redis store configuration.
	StoreConfig store.RedisStoreConfig

	// Logger is the structured logger. Default: slog.Default().
	Logger gochainedlog.Logger
}

func (c *Config) defaults() {
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = 100
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.ClusterRegion == "" {
		c.ClusterRegion = "default"
	}
	if c.LockTTL == 0 {
		c.LockTTL = 30 * time.Second
	}
	if c.LockAutoExtend == 0 {
		c.LockAutoExtend = c.LockTTL / 3
	}
	if c.Logger == nil {
		c.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
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

func WithListenAddr(addr string) Option {
	return func(c *Config) { c.ListenAddr = addr }
}

func WithClusterRegion(region string) Option {
	return func(c *Config) { c.ClusterRegion = region }
}

func WithMaxConcurrency(n int) Option {
	return func(c *Config) { c.MaxConcurrency = n }
}

func WithShutdownTimeout(d time.Duration) Option {
	return func(c *Config) { c.ShutdownTimeout = d }
}

func WithKeyPrefix(prefix string) Option {
	return func(c *Config) { c.StoreConfig.KeyPrefix = prefix }
}

func WithLogger(l gochainedlog.Logger) Option {
	return func(c *Config) { c.Logger = l }
}

func WithLockTTL(d time.Duration) Option {
	return func(c *Config) { c.LockTTL = d }
}

func WithLockAutoExtend(d time.Duration) Option {
	return func(c *Config) { c.LockAutoExtend = d }
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

// WithRedisCluster configures Redis Cluster mode.
func WithRedisCluster(addrs []string) Option {
	return func(c *Config) {
		c.RedisOptions.ClusterAddrs = addrs
	}
}
