package types

import "crypto/tls"

// RedisOptions holds connection parameters for the Redis client.
type RedisOptions struct {
	// URL is the Redis server URL (e.g., "redis://localhost:6379", "rediss://host:6380/0").
	URL string

	// Password for Redis authentication. Overridden if set in URL.
	Password string

	// DB is the Redis database number. Default: 0.
	DB int

	// PoolSize is the maximum number of connections in the pool. Default: 10.
	PoolSize int

	// TLSConfig enables TLS for the connection. Set to non-nil to enable.
	TLSConfig *tls.Config

	// SentinelConfig enables Redis Sentinel failover mode.
	// When set, URL is ignored and Sentinel is used instead.
	SentinelConfig *RedisSentinelConfig

	// ClusterAddrs enables Redis Cluster mode.
	// When set, URL is ignored and a cluster client is created.
	ClusterAddrs []string
}

// RedisSentinelConfig holds configuration for Redis Sentinel failover.
type RedisSentinelConfig struct {
	// MasterName is the name of the master as configured in Sentinel.
	MasterName string

	// SentinelAddrs is the list of Sentinel node addresses (host:port).
	SentinelAddrs []string

	// SentinelPassword is the password for Sentinel nodes (if different from master password).
	SentinelPassword string
}
