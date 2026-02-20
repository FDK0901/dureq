// Package main demonstrates the dureq festival use case using the mux handler
// pattern — pattern-based routing, global middleware, per-handler middleware,
// and context utilities for metadata access.
//
// Compared to the plain festival example, this version:
//   - Registers a "festival.*" pattern handler for all festival-related task types
//   - Adds global logging middleware via srv.Use()
//   - Adds per-handler retry-aware middleware that logs attempt info
//   - Uses context utilities (GetJobID, GetTaskType, GetAttempt, GetMaxRetry, etc.)
//
// Scenario: Multiple offline festivals, each with a unique ID.
// Each festival queries average wait time every 30 seconds (demo interval),
// starting at the festival open time and ending at the close time.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	client "github.com/FDK0901/dureq/clients/go"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"
	"github.com/bytedance/sonic"
	"github.com/rs/xid"
)

// FestivalPayload is the input for the festival wait time query handler.
type FestivalPayload struct {
	FestivalID string `json:"festival_id"`
}

// loggingMiddleware is a global middleware that logs every handler invocation.
func loggingMiddleware(logger gochainedlog.Logger) types.MiddlewareFunc {
	return func(next types.HandlerFunc) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			jobID := types.GetJobID(ctx)
			taskType := types.GetTaskType(ctx)
			attempt := types.GetAttempt(ctx)
			nodeID := types.GetNodeID(ctx)

			logger.Info().String("job_id", jobID).String("task_type", string(taskType)).Int("attempt", attempt).String("node_id", nodeID).Msg("[middleware:logging] handler started")

			err := next(ctx, payload)

			if err != nil {
				logger.Error().String("job_id", jobID).String("task_type", string(taskType)).Err(err).Msg("[middleware:logging] handler failed")
			} else {
				logger.Info().String("job_id", jobID).String("task_type", string(taskType)).Msg("[middleware:logging] handler succeeded")
			}
			return err
		}
	}
}

// retryAwareMiddleware is a per-handler middleware that logs retry context.
func retryAwareMiddleware(logger gochainedlog.Logger) types.MiddlewareFunc {
	return func(next types.HandlerFunc) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			attempt := types.GetAttempt(ctx)
			maxRetry := types.GetMaxRetry(ctx)
			taskType := types.GetTaskType(ctx)

			if attempt > 1 {
				logger.Warn().String("task_type", string(taskType)).Int("attempt", attempt).Int("max_retry", maxRetry).Msg("[middleware:retry] retrying handler")
			}
			return next(ctx, payload)
		}
	}
}

// festivalHandler is a single handler registered for the "festival.*" pattern.
func festivalHandler(logger gochainedlog.Logger) types.HandlerFunc {
	return func(ctx context.Context, payload json.RawMessage) error {
		taskType := types.GetTaskType(ctx)
		jobID := types.GetJobID(ctx)
		headers := types.GetHeaders(ctx)

		logger.Info().String("task_type", string(taskType)).String("job_id", jobID).Any("headers", headers).Msg("festival handler dispatching")

		switch taskType {
		case "festival.query_wait_time":
			var p FestivalPayload
			if err := sonic.ConfigFastest.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			logger.Info().String("festival_id", p.FestivalID).Time("time", time.Now()).Msg("querying average wait time")
			return nil

		default:
			return fmt.Errorf("unknown festival task type: %s", taskType)
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	// --- Server Side ---

	srv, err := server.New(
		server.WithRedisURL("redis://localhost:6379"),
		server.WithRedisDB(15),
		server.WithRedisPassword("your-password"),
		server.WithNodeID("festival-mux-node-2"),
		server.WithMaxConcurrency(10),
		server.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register global middleware — applies to ALL handlers.
	srv.Use(loggingMiddleware(logger))

	// Register a single pattern handler for all "festival.*" task types.
	err = srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "festival.*", // pattern matching
		Concurrency: 5,
		Timeout:     30 * time.Second,
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 5 * time.Second,
			MaxDelay:     1 * time.Minute,
			Multiplier:   2.0,
			Jitter:       0.1,
		},
		Handler: festivalHandler(logger),
		// Per-handler middleware — applied after global middleware.
		Middlewares: []types.MiddlewareFunc{
			retryAwareMiddleware(logger),
		},
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to register festival handler")
		os.Exit(1)
	}

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	// --- Client Side ---

	cli, err := client.New(
		client.WithRedisURL("redis://localhost:6379"),
		client.WithRedisPassword("your-password"),
		client.WithRedisDB(15),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	// Register multiple festivals with different schedules.
	festivals := []struct {
		ID        string
		OpenDate  time.Time
		CloseDate time.Time
	}{
		{
			ID:        xid.New().String(),
			OpenDate:  time.Now().Add(1 * time.Minute),
			CloseDate: time.Now().Add(10 * time.Minute),
		},
		{
			ID:        xid.New().String(),
			OpenDate:  time.Now().Add(2 * time.Minute),
			CloseDate: time.Now().Add(15 * time.Minute),
		},
	}

	interval := types.Duration(30 * time.Second)

	for _, fest := range festivals {
		payload, _ := sonic.ConfigFastest.Marshal(FestivalPayload{FestivalID: fest.ID})
		uniqueKey := fmt.Sprintf("festival-wait-%s", fest.ID)
		openDate := fest.OpenDate
		closeDate := fest.CloseDate

		job, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
			TaskType: "festival.query_wait_time",
			Payload:  payload,
			Schedule: types.Schedule{
				Type:     types.ScheduleDuration,
				Interval: &interval,
				StartsAt: &openDate,
				EndsAt:   &closeDate,
			},
			UniqueKey: &uniqueKey,
			Tags:      []string{"festival", fest.ID},
		})
		if err != nil {
			logger.Error().String("festival", fest.ID).Err(err).Msg("failed to enqueue festival job")
			continue
		}

		logger.Info().String("festival", fest.ID).String("job_id", job.ID).Time("starts_at", fest.OpenDate).Time("ends_at", fest.CloseDate).Msg("enqueued festival job")
	}

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
