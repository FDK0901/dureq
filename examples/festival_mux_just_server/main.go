// Package main demonstrates the dureq festival use case using the mux handler
// pattern with ONLY the server side (no client).
//
// This example:
//   - Registers the "festival.*" pattern handler
//   - Adds global logging middleware via srv.Use()
//   - Adds per-handler retry-aware middleware
//   - Sets up the server and waits for shutdown signal
//   - Does NOT include the client-side job enqueueing logic
//   - Useful for demonstrating server-only behavior or testing
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/bytedance/sonic"
)

// FestivalPayload is the input for the festival wait time query handler.
type FestivalPayload struct {
	FestivalID string `json:"festival_id"`
}

// loggingMiddleware is a global middleware that logs every handler invocation.
func loggingMiddleware() types.MiddlewareFunc {
	return func(next types.HandlerFunc) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			jobID := types.GetJobID(ctx)
			taskType := types.GetTaskType(ctx)
			attempt := types.GetAttempt(ctx)
			nodeID := types.GetNodeID(ctx)

			slog.Info("[middleware:logging] handler started",
				"job_id", jobID,
				"task_type", string(taskType),
				"attempt", attempt,
				"node_id", nodeID,
			)

			err := next(ctx, payload)

			if err != nil {
				slog.Error("[middleware:logging] handler failed",
					"job_id", jobID,
					"task_type", string(taskType),
					"err", err,
				)
			} else {
				slog.Info("[middleware:logging] handler succeeded",
					"job_id", jobID,
					"task_type", string(taskType),
				)
			}
			return err
		}
	}
}

// retryAwareMiddleware is a per-handler middleware that logs retry context.
func retryAwareMiddleware() types.MiddlewareFunc {
	return func(next types.HandlerFunc) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			attempt := types.GetAttempt(ctx)
			maxRetry := types.GetMaxRetry(ctx)
			taskType := types.GetTaskType(ctx)

			if attempt > 1 {
				slog.Warn("[middleware:retry] retrying handler",
					"task_type", string(taskType),
					"attempt", attempt,
					"max_retry", maxRetry,
				)
			}
			return next(ctx, payload)
		}
	}
}

// festivalHandler is a single handler registered for the "festival.*" pattern.
func festivalHandler() types.HandlerFunc {
	return func(ctx context.Context, payload json.RawMessage) error {
		taskType := types.GetTaskType(ctx)
		jobID := types.GetJobID(ctx)
		headers := types.GetHeaders(ctx)

		slog.Info("festival handler dispatching",
			"task_type", string(taskType),
			"job_id", jobID,
			"headers", headers,
		)

		switch taskType {
		case "festival.query_wait_time":
			var p FestivalPayload
			if err := sonic.ConfigFastest.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			slog.Info("querying average wait time",
				"festival_id", p.FestivalID,
				"time", time.Now().Format(time.RFC3339),
			)
			return nil

		default:
			return fmt.Errorf("unknown festival task type: %s", taskType)
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	srv, err := server.New(
		server.WithRedisURL("redis://localhost:6379"),
		server.WithRedisDB(15),
		server.WithRedisPassword("your-password"),
		server.WithNodeID("festival-mux-node-1"),
		server.WithMaxConcurrency(10),
	)
	if err != nil {
		slog.Error("failed to create server", "err", err)
		os.Exit(1)
	}

	// Register global middleware — applies to ALL handlers.
	srv.Use(loggingMiddleware())

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
		Handler: festivalHandler(),
		// Per-handler middleware — applied after global middleware.
		Middlewares: []types.MiddlewareFunc{
			retryAwareMiddleware(),
		},
	})
	if err != nil {
		slog.Error("failed to register festival handler", "err", err)
		os.Exit(1)
	}

	if err := srv.Start(ctx); err != nil {
		slog.Error("failed to start server", "err", err)
		os.Exit(1)
	}

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	slog.Info("shutting down")
	srv.Stop()
}
