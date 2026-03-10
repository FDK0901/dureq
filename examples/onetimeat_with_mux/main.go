// Package main demonstrates the dureq one-time-at scheduling use case using
// the mux handler pattern — pattern-based routing, global middleware,
// per-handler middleware, and context utilities for metadata access.
//
// Compared to the plain onetimeat example, this version:
//   - Registers a "onetimeat.*" pattern handler
//   - Adds global logging middleware via srv.Use()
//   - Adds per-handler retry-aware middleware
//   - Uses context utilities (GetJobID, GetTaskType, GetAttempt, GetMaxRetry, etc.)
//
// Scenario: Multiple one-time scheduled tasks, each fires at a specific time.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"

	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

// OnetimeAtPayload is the input for the onetime at query handler.
type OnetimeAtPayload struct {
	OnetimeID string `json:"onetime_id"`
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

// onetimeatHandler is a single handler registered for the "onetimeat.*" pattern.
func onetimeatHandler(logger *zerolog.Logger) types.HandlerFunc {
	return func(ctx context.Context, payload json.RawMessage) error {
		taskType := types.GetTaskType(ctx)
		jobID := types.GetJobID(ctx)
		runID := types.GetRunID(ctx)
		priority := types.GetPriority(ctx)
		headers := types.GetHeaders(ctx)

		logger.Info().
			Str("task_type", string(taskType)).
			Str("job_id", jobID).
			Str("run_id", runID).
			Int("priority", int(priority)).
			Interface("headers", headers).
			Msg("onetimeat handler dispatching")

		switch taskType {
		case "onetimeat.query_onetime_at":
			var p OnetimeAtPayload
			if err := json.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			logger.Info().
				Str("onetime_id", p.OnetimeID).
				Str("time", time.Now().Format(time.RFC3339)).
				Msg("querying onetime at")

			return nil

		default:
			return fmt.Errorf("unknown onetimeat task type: %s", taskType)
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
		dureq.WithNodeID("onetimeat-mux-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register global middleware — applies to ALL handlers.
	srv.Use(loggingMiddleware(&zl))

	// Register a single pattern handler for all "onetimeat.*" task types.
	err = srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "onetimeat.*", // pattern matching
		Concurrency: 5,
		Timeout:     30 * time.Second,
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 5 * time.Second,
			MaxDelay:     1 * time.Minute,
			Multiplier:   2.0,
			Jitter:       0.1,
		},
		Handler: onetimeatHandler(&zl),
		// Per-handler middleware — applied after global middleware.
		Middlewares: []types.MiddlewareFunc{
			retryAwareMiddleware(&zl),
		},
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to register onetimeat handler")
		os.Exit(1)
	}

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	// --- Client Side ---

	cli, err := dureq.NewClient(
		dureq.WithClientRedisURL("redis://localhost:6381"),
		dureq.WithClientRedisPassword("your-password"),
		dureq.WithClientRedisDB(15),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	// Register multiple one-time tasks.
	onetimes := []struct {
		ID     string
		AtTime time.Time
	}{
		{
			ID:     xid.New().String(),
			AtTime: time.Now().Add(5 * time.Second),
		},
		{
			ID:     xid.New().String(),
			AtTime: time.Now().Add(10 * time.Second),
		},
	}

	for _, onetime := range onetimes {
		payload, _ := json.Marshal(OnetimeAtPayload{OnetimeID: onetime.ID})
		uniqueKey := fmt.Sprintf("onetime-at-%s", onetime.ID)
		atTime := onetime.AtTime

		job, err := cli.EnqueueScheduled(ctx, &dureq.EnqueueRequest{
			TaskType: "onetimeat.query_onetime_at",
			Payload:  payload,
			Schedule: types.Schedule{
				Type:  types.ScheduleOneTime,
				RunAt: &atTime,
			},
			UniqueKey: &uniqueKey,
			Tags:      []string{"onetime", onetime.ID},
		})
		if err != nil {
			logger.Error().String("onetime", onetime.ID).Err(err).Msg("failed to enqueue onetime job")
			continue
		}

		logger.Info().String("onetime", onetime.ID).String("job_id", job.ID).String("at_time", onetime.AtTime.Format(time.RFC3339)).Msg("enqueued onetime job")
	}

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
