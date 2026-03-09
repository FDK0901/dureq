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
	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/monitor"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
)

// Server options — re-export from internal/server.
var (
	WithRedisURL              = server.WithRedisURL
	WithRedisPassword         = server.WithRedisPassword
	WithRedisDB               = server.WithRedisDB
	WithRedisPoolSize         = server.WithRedisPoolSize
	WithNodeID                = server.WithNodeID
	WithMaxConcurrency        = server.WithMaxConcurrency
	WithSchedulerTickInterval = server.WithSchedulerTickInterval
	WithHeartbeatInterval     = server.WithHeartbeatInterval
	WithElectionTTL           = server.WithElectionTTL
	WithLockTTL               = server.WithLockTTL
	WithLogger                = server.WithLogger
	WithKeyPrefix             = server.WithKeyPrefix
	WithPriorityTiers         = server.WithPriorityTiers
	WithRedisSentinel         = server.WithRedisSentinel
	WithRedisCluster          = server.WithRedisCluster
	WithGroupAggregation      = server.WithGroupAggregation
	WithRetentionPeriod       = server.WithRetentionPeriod
	WithShutdownTimeout       = server.WithShutdownTimeout
)

// NewServer creates a new dureq server node.
func NewServer(opts ...server.Option) (*server.Server, error) {
	return server.New(opts...)
}

// Client options — re-export from pkg/client.
var (
	WithClientRedisURL      = client.WithRedisURL
	WithClientRedisPassword = client.WithRedisPassword
	WithClientRedisDB       = client.WithRedisDB
	WithClientRedisPoolSize = client.WithRedisPoolSize
	WithClientKeyPrefix     = client.WithKeyPrefix
	WithClientTiers         = client.WithPriorityTiers
	WithClientClusterAddrs  = client.WithClusterAddrs
	WithClientRedisStore    = client.WithStore
)

type Client = client.Client

// NewClient creates a new dureq client for enqueuing jobs.
func NewClient(opts ...client.Option) (*client.Client, error) {
	return client.New(opts...)
}

type RedisStore = store.RedisStore

type APIService = monitor.APIService

type Dispatcher = monitor.Dispatcher

// Re-export APIService
func NewAPIService(s *RedisStore, disp Dispatcher, logger gochainedlog.Logger) *APIService {
	return monitor.NewAPIService(s, disp, logger)
}

// Re-export store types commonly needed by users.
type TierConfig = store.TierConfig

// Re-export client types commonly needed by users.
type EnqueueRequest = client.EnqueueRequest

// Re-export types for group aggregation.
type EnqueueGroupOption = types.EnqueueGroupOption
type GroupConfig = types.GroupConfig
type GroupAggregator = types.GroupAggregator
type GroupAggregatorFunc = types.GroupAggregatorFunc
