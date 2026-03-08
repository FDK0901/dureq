// Package main demonstrates heartbeat timeout and progress reporting.
//
// Scenario: A long-running data processing job reports progress (percentage)
// while running. The scheduler detects stale runs using a per-job heartbeat
// timeout (10s) and auto-recovers if the node crashes.
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
	"github.com/rs/zerolog"
)

type ProcessPayload struct {
	DatasetID string `json:"dataset_id"`
	Rows      int    `json:"rows"`
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	srv, err := dureq.NewServer(
		dureq.WithRedisURL("redis://localhost:6379"),
		dureq.WithRedisDB(15),
		dureq.WithRedisPassword("your-password"),
		dureq.WithNodeID("heartbeat-node-1"),
		dureq.WithMaxConcurrency(5),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register a long-running handler that reports progress.
	err = srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "data.process",
		Concurrency: 3,
		Timeout:     5 * time.Minute,
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 5 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			var p ProcessPayload
			if err := json.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			logger.Info().String("dataset_id", p.DatasetID).Int("rows", p.Rows).Msg("starting data processing")

			// Simulate processing rows in chunks, reporting progress.
			chunkSize := p.Rows / 10
			if chunkSize == 0 {
				chunkSize = 1
			}

			for processed := 0; processed < p.Rows; processed += chunkSize {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				// Simulate work.
				time.Sleep(2 * time.Second)

				pct := float64(processed+chunkSize) / float64(p.Rows) * 100
				if pct > 100 {
					pct = 100
				}

				// Report progress — this is persisted via heartbeat and
				// visible via GET /api/runs/{runID}/progress.
				if err := types.ReportProgress(ctx, map[string]any{
					"dataset_id":     p.DatasetID,
					"rows_processed": processed + chunkSize,
					"rows_total":     p.Rows,
					"percent":        pct,
				}); err != nil {
					logger.Warn().Err(err).Msg("failed to report progress")
				}

				logger.Info().String("dataset_id", p.DatasetID).Float64("percent", pct).Msg("processing")
			}

			logger.Info().String("dataset_id", p.DatasetID).Msg("processing complete")
			return nil
		},
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to register handler")
		os.Exit(1)
	}

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	// --- Client Side ---

	cli, err := dureq.NewClient(
		dureq.WithClientRedisURL("redis://localhost:6379"),
		dureq.WithClientRedisPassword("your-password"),
		dureq.WithClientRedisDB(15),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	// Enqueue a data processing job with a custom heartbeat timeout.
	hbTimeout := types.Duration(10 * time.Second)
	payload, _ := json.Marshal(ProcessPayload{
		DatasetID: "ds-001",
		Rows:      1000,
	})

	job, err := cli.EnqueueScheduled(ctx, &dureq.EnqueueRequest{
		TaskType: "data.process",
		Payload:  payload,
		Schedule: types.Schedule{Type: types.ScheduleImmediate},
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 5 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
		},
		HeartbeatTimeout: &hbTimeout,
		Tags:             []string{"data-processing", "demo"},
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue job")
		os.Exit(1)
	}

	logger.Info().String("job_id", job.ID).Msg("enqueued data processing job with heartbeat timeout=10s")
	logger.Info().Msg("check progress: curl http://localhost:7654/api/runs | jq")

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
