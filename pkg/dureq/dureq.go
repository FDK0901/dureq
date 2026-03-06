// Package dureq provides top-level convenience constructors for the dureq
// distributed job scheduling system backed by Redis.
//
// Server:
//
//	server, _ := dureq.NewServer(
//	    dureq.WithRedisURL("redis://localhost:6379"),
//	    dureq.WithNodeID("node-1"),
//	)
//	server.RegisterHandler(types.HandlerDefinition{...})
//	server.Start(ctx)
//
// Client:
//
//	client, _ := dureq.NewClient(dureq.WithClientRedisURL("redis://localhost:6379"))
//	client.Enqueue(ctx, "email.send", payload)
package dureq

import (
	client "github.com/FDK0901/dureq/clients/go"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/internal/store"
)

// Server options — re-export from internal/server.
var (
	WithRedisURL       = server.WithRedisURL
	WithRedisPassword  = server.WithRedisPassword
	WithRedisDB        = server.WithRedisDB
	WithRedisPoolSize  = server.WithRedisPoolSize
	WithNodeID         = server.WithNodeID
	WithMaxConcurrency = server.WithMaxConcurrency
	//WithSchedulerTickInterval = server.WithSchedulerTickInterval
	//WithHeartbeatInterval     = server.WithHeartbeatInterval
	//WithElectionTTL           = server.WithElectionTTL
	//WithLockTTL               = server.WithLockTTL
	//WithLogger                = server.WithLogger
	WithKeyPrefix     = server.WithKeyPrefix
	WithPriorityTiers = server.WithPriorityTiers
	WithRedisSentinel = server.WithRedisSentinel
	WithRedisCluster  = server.WithRedisCluster
)

// NewServer creates a new dureq server node.
func NewServer(opts ...server.Option) (*server.Server, error) {
	return server.New(opts...)
}

// Client options — re-export from pkg/client.
var (
	WithClientRedisURL   = client.WithRedisURL
	WithClientRedisStore = client.WithStore
	WithClientKeyPrefix  = client.WithKeyPrefix
	WithClientTiers      = client.WithPriorityTiers
)

// NewClient creates a new dureq client for enqueuing jobs.
func NewClient(opts ...client.Option) (*client.Client, error) {
	return client.New(opts...)
}

// Re-export store types commonly needed by users.
type TierConfig = store.TierConfig
