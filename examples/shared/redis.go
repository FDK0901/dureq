// Package shared provides common Redis configuration for all examples.
// By default it connects to a Redis Cluster on ports 7001-7006.
// Set REDIS_URL to override with a standalone Redis instance (e.g. "redis://localhost:6381").
package shared

import (
	"os"
	"strings"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/server"
)

const defaultPassword = "your-password"

var defaultClusterAddrs = []string{
	"localhost:7001", "localhost:7002", "localhost:7003",
	"localhost:7004", "localhost:7005", "localhost:7006",
}

// ServerOptions returns server options configured for the current Redis environment.
func ServerOptions() []server.Option {
	pw := envOr("REDIS_PASSWORD", defaultPassword)
	user := os.Getenv("REDIS_USERNAME")

	if url := os.Getenv("REDIS_URL"); url != "" {
		opts := []server.Option{
			server.WithRedisURL(url),
			server.WithRedisPassword(pw),
		}
		if user != "" {
			opts = append(opts, server.WithRedisUsername(user))
		}
		if db := os.Getenv("REDIS_DB"); db != "" {
			var dbInt int
			for _, c := range db {
				dbInt = dbInt*10 + int(c-'0')
			}
			opts = append(opts, server.WithRedisDB(dbInt))
		}
		return opts
	}

	addrs := clusterAddrs()
	opts := []server.Option{
		server.WithRedisCluster(addrs),
		server.WithRedisPassword(pw),
	}
	if user != "" {
		opts = append(opts, server.WithRedisUsername(user))
	}
	return opts
}

// ClientOptions returns client options configured for the current Redis environment.
func ClientOptions() []client.Option {
	pw := envOr("REDIS_PASSWORD", defaultPassword)
	user := os.Getenv("REDIS_USERNAME")

	if url := os.Getenv("REDIS_URL"); url != "" {
		opts := []client.Option{
			client.WithRedisURL(url),
			client.WithRedisPassword(pw),
		}
		if user != "" {
			opts = append(opts, client.WithRedisUsername(user))
		}
		if db := os.Getenv("REDIS_DB"); db != "" {
			var dbInt int
			for _, c := range db {
				dbInt = dbInt*10 + int(c-'0')
			}
			opts = append(opts, client.WithRedisDB(dbInt))
		}
		return opts
	}

	addrs := clusterAddrs()
	opts := []client.Option{
		client.WithClusterAddrs(addrs),
		client.WithRedisPassword(pw),
	}
	if user != "" {
		opts = append(opts, client.WithRedisUsername(user))
	}
	return opts
}

func clusterAddrs() []string {
	if env := os.Getenv("REDIS_CLUSTER_ADDRS"); env != "" {
		return strings.Split(env, ",")
	}
	return defaultClusterAddrs
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
