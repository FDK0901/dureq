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
	"github.com/FDK0901/go-chainedlog"
)

// Server options — re-export from internal/server.
var (
	WithRedisURL              = server.WithRedisURL
	WithRedisUsername         = server.WithRedisUsername
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
	WithMode                  = server.WithMode
)

type Server = server.Server
type Mode = server.Mode

const (
	ModeQueue     = server.ModeQueue
	ModeScheduler = server.ModeScheduler
	ModeWorkflow  = server.ModeWorkflow
	ModeMonitor   = server.ModeMonitor
	ModeFull      = server.ModeFull
)

// NewServer creates a new dureq server node.
func NewServer(opts ...server.Option) (*server.Server, error) {
	return server.New(opts...)
}

// Client options — re-export from pkg/client.
var (
	WithClientRedisURL      = client.WithRedisURL
	WithClientRedisUsername = client.WithRedisUsername
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
type Hub = monitor.Hub

type Dispatcher = monitor.Dispatcher

type BulkRequest = monitor.BulkRequest
type BulkResult = monitor.BulkResult
type GroupInfo = monitor.GroupInfo
type QueueInfo = monitor.QueueInfo
type StatsResponse = monitor.StatsResponse
type DailyStatsEntry = monitor.DailyStatsEntry
type UniqueKeyResult = monitor.UniqueKeyResult
type WorkflowAuditEntry = monitor.WorkflowAuditEntry
type ApiError = monitor.ApiError

// Re-export APIService
func NewAPIService(s *RedisStore, disp Dispatcher, logger chainedlog.Logger) *APIService {
	return monitor.NewAPIService(s, disp, logger)
}

// Re-export WSClient
type WSClient = monitor.WSClient

func NewWSClient() *WSClient {
	return monitor.NewWSClient()
}

// Re-export store types commonly needed by users.
type TierConfig = store.TierConfig

// Re-export filter types for custom API handler composition.
type JobFilter = store.JobFilter
type RunFilter = store.RunFilter
type EventFilter = store.EventFilter
type ScheduleFilter = store.ScheduleFilter
type WorkflowFilter = store.WorkflowFilter
type BatchFilter = store.BatchFilter
type StatsFilter = store.StatsFilter
type JobStats = store.JobStats
type AuditEntry = store.AuditEntry

// Re-export client types commonly needed by users.
type EnqueueRequest = client.EnqueueRequest

// Re-export types for group aggregation.
type EnqueueGroupOption = types.EnqueueGroupOption
type GroupConfig = types.GroupConfig
type GroupAggregator = types.GroupAggregator
type GroupAggregatorFunc = types.GroupAggregatorFunc
