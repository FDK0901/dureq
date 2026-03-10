// Package main demonstrates the dureq festival use case.
//
// Scenario: Multiple offline festivals, each with a unique ID.
// Each festival queries average wait time every 30 minutes,
// starting at the festival open time and ending at the close time.
// After close, the job is automatically removed.
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

	"github.com/FDK0901/dureq/examples/shared"
	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

// OnetimeAtPayload is the input for the onetime at query handler.
type OnetimeAtPayload struct {
	OnetimeID string `json:"onetime_id"`
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	srv, err := dureq.NewServer(
		append(shared.ServerOptions(),
			dureq.WithNodeID("onetimeat-node-1"),
			dureq.WithMaxConcurrency(10),
			dureq.WithLogger(logger),
		)...,
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register the festival wait time handler.
	err = srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "onetimeat.query_onetime_at",
		Concurrency: 5,
		Timeout:     30 * time.Second,
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 5 * time.Second,
			MaxDelay:     1 * time.Minute,
			Multiplier:   2.0,
			Jitter:       0.1,
		},
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			var p OnetimeAtPayload
			if err := json.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			// Simulate querying average wait time.
			logger.Info().String("onetime_id", p.OnetimeID).String("time", time.Now().Format(time.RFC3339)).Msg("querying onetime at")

			// In production, this would:
			// 1. Query kiosk data for the festival
			// 2. Calculate average wait time
			// 3. Store the result

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

	cli, err := dureq.NewClient(shared.ClientOptions()...)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	// Register multiple festivals with different schedules.
	onetimes := []struct {
		ID     string
		AtTime time.Time
	}{
		{
			ID:     xid.New().String(),
			AtTime: time.Now().Add(1 * time.Minute), // starts in 1 minute (for demo)
		},
		{
			ID:     xid.New().String(),
			AtTime: time.Now().Add(2 * time.Minute),
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
