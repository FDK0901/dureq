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

	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"
	"github.com/rs/zerolog"
)

// FestivalPayload is the input for the festival wait time query handler.
type FestivalPayload struct {
	FestivalID string `json:"festival_id"`
}

// loggingMiddleware is a global middleware that logs every handler invocation.
func loggingMiddleware(logger *zerolog.Logger) types.MiddlewareFunc {
	return func(next types.HandlerFunc) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			jobID := types.GetJobID(ctx)
			taskType := types.GetTaskType(ctx)
			attempt := types.GetAttempt(ctx)
			nodeID := types.GetNodeID(ctx)

			logger.Info().
				Str("job_id", jobID).
				Str("task_type", string(taskType)).
				Int("attempt", attempt).
				Str("node_id", nodeID).
				Msg("[middleware:logging] handler started")

			err := next(ctx, payload)

			if err != nil {
				logger.Error().
					Str("job_id", jobID).
					Str("task_type", string(taskType)).
					Err(err).
					Msg("[middleware:logging] handler failed")
			} else {
				logger.Info().
					Str("job_id", jobID).
					Str("task_type", string(taskType)).
					Msg("[middleware:logging] handler succeeded")
			}
			return err
		}
	}
}

// retryAwareMiddleware is a per-handler middleware that logs retry context.
func retryAwareMiddleware(logger *zerolog.Logger) types.MiddlewareFunc {
	return func(next types.HandlerFunc) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			attempt := types.GetAttempt(ctx)
			maxRetry := types.GetMaxRetry(ctx)
			taskType := types.GetTaskType(ctx)

			if attempt > 1 {
				logger.Warn().
					Str("task_type", string(taskType)).
					Int("attempt", attempt).
					Int("max_retry", maxRetry).
					Msg("[middleware:retry] retrying handler")
			}
			return next(ctx, payload)
		}
	}
}

// festivalHandler is a single handler registered for the "festival.*" pattern.
func festivalHandler(logger *zerolog.Logger) types.HandlerFunc {
	return func(ctx context.Context, payload json.RawMessage) error {
		taskType := types.GetTaskType(ctx)
		jobID := types.GetJobID(ctx)
		headers := types.GetHeaders(ctx)

		logger.Info().
			Str("task_type", string(taskType)).
			Str("job_id", jobID).
			Interface("headers", headers).
			Msg("festival handler dispatching")

		switch taskType {
		case "festival.query_wait_time":
			var p FestivalPayload
			if err := json.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			logger.Info().
				Str("festival_id", p.FestivalID).
				Str("time", time.Now().Format(time.RFC3339)).
				Msg("querying average wait time")

			return nil

		default:
			return fmt.Errorf("unknown festival task type: %s", taskType)
		}
	}
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	srv, err := dureq.NewServer(
		dureq.WithRedisURL("redis://localhost:6381"),
		dureq.WithRedisDB(15),
		dureq.WithRedisPassword("your-password"),
		dureq.WithNodeID("festival-mux-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register global middleware — applies to ALL handlers.
	srv.Use(loggingMiddleware(&zl))

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
		Handler: festivalHandler(&zl),
		// Per-handler middleware — applied after global middleware.
		Middlewares: []types.MiddlewareFunc{
			retryAwareMiddleware(&zl),
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

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
