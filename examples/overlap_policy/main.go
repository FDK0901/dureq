// Package main demonstrates schedule overlap policies.
//
// Scenario: A slow-running scheduled job (takes 8s) runs every 5s with
// different overlap policies to show SKIP and BUFFER_ONE behavior.
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

type WorkPayload struct {
	Label string `json:"label"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	srv, err := server.New(
		server.WithRedisURL("redis://localhost:6379"),
		server.WithRedisDB(15),
		server.WithRedisPassword("your-password"),
		server.WithNodeID("overlap-node-1"),
		server.WithMaxConcurrency(10),
	)
	if err != nil {
		slog.Error("failed to create server", "err", err)
		os.Exit(1)
	}

	// Register a slow handler (takes 8 seconds).
	err = srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "overlap.slow_task",
		Concurrency: 5,
		Timeout:     30 * time.Second,
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			var p WorkPayload
			if err := sonic.ConfigFastest.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			slog.Info("starting slow task (8s)", "label", p.Label)
			time.Sleep(8 * time.Second)
			slog.Info("slow task complete", "label", p.Label)
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

	interval := types.Duration(5 * time.Second)
	endsAt := time.Now().Add(1 * time.Minute)

	// Job 1: SKIP policy — if previous run is still active, skip the next firing.
	skipPayload, _ := sonic.ConfigFastest.Marshal(WorkPayload{Label: "skip-policy"})
	skipKey := "overlap-skip-demo"
	skipJob, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType: "overlap.slow_task",
		Payload:  skipPayload,
		Schedule: types.Schedule{
			Type:          types.ScheduleDuration,
			Interval:      &interval,
			EndsAt:        &endsAt,
			OverlapPolicy: types.OverlapSkip,
		},
		UniqueKey: &skipKey,
		Tags:      []string{"overlap", "skip"},
	})
	if err != nil {
		slog.Error("failed to enqueue skip job", "err", err)
	} else {
		slog.Info("enqueued SKIP overlap policy job", "job_id", skipJob.ID)
	}

	// Job 2: BUFFER_ONE policy — buffer the most recent missed firing, dispatch after completion.
	bufPayload, _ := sonic.ConfigFastest.Marshal(WorkPayload{Label: "buffer-one-policy"})
	bufKey := "overlap-buffer-demo"
	bufJob, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
		TaskType: "overlap.slow_task",
		Payload:  bufPayload,
		Schedule: types.Schedule{
			Type:          types.ScheduleDuration,
			Interval:      &interval,
			EndsAt:        &endsAt,
			OverlapPolicy: types.OverlapBufferOne,
		},
		UniqueKey: &bufKey,
		Tags:      []string{"overlap", "buffer-one"},
	})
	if err != nil {
		slog.Error("failed to enqueue buffer job", "err", err)
	} else {
		slog.Info("enqueued BUFFER_ONE overlap policy job", "job_id", bufJob.ID)
	}

	slog.Info("watch the logs: SKIP will miss firings, BUFFER_ONE will queue one and dispatch after completion")

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	slog.Info("shutting down")
	srv.Stop()
}
