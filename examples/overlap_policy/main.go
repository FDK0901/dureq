// Package main demonstrates schedule overlap policies.
//
// Scenario: A slow-running scheduled job (takes 8s) runs every 5s with
// different overlap policies to show SKIP and BUFFER_ONE behavior.
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

type WorkPayload struct {
	Label string `json:"label"`
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
		dureq.WithNodeID("overlap-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register a slow handler (takes 8 seconds).
	err = srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "overlap.slow_task",
		Concurrency: 5,
		Timeout:     30 * time.Second,
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			var p WorkPayload
			if err := json.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			logger.Info().String("label", p.Label).Msg("starting slow task (8s)")
			time.Sleep(8 * time.Second)
			logger.Info().String("label", p.Label).Msg("slow task complete")
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
		dureq.WithClientRedisURL("redis://localhost:6381"),
		dureq.WithClientRedisPassword("your-password"),
		dureq.WithClientRedisDB(15),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	interval := types.Duration(5 * time.Second)
	endsAt := time.Now().Add(1 * time.Minute)

	// Job 1: SKIP policy — if previous run is still active, skip the next firing.
	skipPayload, _ := json.Marshal(WorkPayload{Label: "skip-policy"})
	skipKey := "overlap-skip-demo"
	skipJob, err := cli.EnqueueScheduled(ctx, &dureq.EnqueueRequest{
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
		logger.Error().Err(err).Msg("failed to enqueue skip job")
	} else {
		logger.Info().String("job_id", skipJob.ID).Msg("enqueued SKIP overlap policy job (runs every 5s, task takes 8s)")
	}

	// Job 2: BUFFER_ONE policy — buffer the most recent missed firing, dispatch after completion.
	bufPayload, _ := json.Marshal(WorkPayload{Label: "buffer-one-policy"})
	bufKey := "overlap-buffer-demo"
	bufJob, err := cli.EnqueueScheduled(ctx, &dureq.EnqueueRequest{
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
		logger.Error().Err(err).Msg("failed to enqueue buffer job")
	} else {
		logger.Info().String("job_id", bufJob.ID).Msg("enqueued BUFFER_ONE overlap policy job (runs every 5s, task takes 8s)")
	}

	logger.Info().Msg("watch the logs: SKIP will miss firings, BUFFER_ONE will queue one and dispatch after completion")

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
