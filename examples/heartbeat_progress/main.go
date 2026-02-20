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
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	client "github.com/FDK0901/dureq/clients/go"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/bytedance/sonic"
)

type ProcessPayload struct {
	DatasetID string `json:"dataset_id"`
	Rows      int    `json:"rows"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	srv, err := server.New(
		server.WithRedisURL("redis://localhost:6379"),
		server.WithRedisDB(15),
		server.WithRedisPassword("your-password"),
		server.WithNodeID("heartbeat-node-1"),
		server.WithMaxConcurrency(5),
	)
	if err != nil {
		slog.Error("failed to create server", "err", err)
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
			if err := sonic.ConfigFastest.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			slog.Info("starting data processing", "dataset_id", p.DatasetID, "rows", p.Rows)

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
					slog.Warn("failed to report progress", "err", err)
				}

				slog.Info("processing", "dataset_id", p.DatasetID, "percent", pct)
			}

			slog.Info("processing complete", "dataset_id", p.DatasetID)
			return nil
		},
	})
	if err != nil {
		slog.Error("failed to register handler", "err", err)
		os.Exit(1)
	}

	if err := srv.Start(ctx); err != nil {
		slog.Error("failed to start server", "err", err)
		os.Exit(1)
	}

	// --- Client Side ---

	cli, err := client.New(
		client.WithRedisURL("redis://localhost:6379"),
		client.WithRedisPassword("your-password"),
		client.WithRedisDB(15),
	)
	if err != nil {
		slog.Error("failed to create client", "err", err)
		os.Exit(1)
	}
	defer cli.Close()

	// Enqueue a data processing job with a custom heartbeat timeout.
	hbTimeout := types.Duration(10 * time.Second)
	payload, _ := sonic.ConfigFastest.Marshal(ProcessPayload{
		DatasetID: "ds-001",
		Rows:      1000,
	})

	job, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
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
		slog.Error("failed to enqueue job", "err", err)
		os.Exit(1)
	}

	slog.Info("enqueued data processing job", "job_id", job.ID, "heartbeat_timeout", "10s")

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	slog.Info("shutting down")
	srv.Stop()
}
