// Package main demonstrates the dureq festival use case.
//
// Scenario: Multiple offline festivals, each with a unique ID.
// Each festival queries average wait time every 30 seconds (demo interval),
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

	client "github.com/FDK0901/dureq/clients/go"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"
	"github.com/bytedance/sonic"
	"github.com/rs/xid"
)

// FestivalPayload is the input for the festival wait time query handler.
type FestivalPayload struct {
	FestivalID string `json:"festival_id"`
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
		server.WithNodeID("festival-node-1"),
		server.WithMaxConcurrency(10),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register the festival wait time handler.
	err = srv.RegisterHandler(types.HandlerDefinition{
		TaskType:    "festival.query_wait_time",
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
			var p FestivalPayload
			if err := sonic.ConfigFastest.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}
			logger.Info().String("festival_id", p.FestivalID).String("time", time.Now().Format(time.RFC3339)).Msg("querying average wait time")
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

		logger.Info().String("festival", fest.ID).String("job_id", job.ID).String("starts_at", fest.OpenDate.Format(time.RFC3339)).String("ends_at", fest.CloseDate.Format(time.RFC3339)).Msg("enqueued festival job")
	}

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
