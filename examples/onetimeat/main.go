// Package main demonstrates the dureq one-time-at scheduling use case.
//
// Scenario: Multiple one-time scheduled tasks, each fires at a specific time.
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
	"github.com/rs/xid"
)

// OnetimeAtPayload is the input for the onetime at query handler.
type OnetimeAtPayload struct {
	OnetimeID string `json:"onetime_id"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	srv, err := server.New(
		server.WithRedisURL("redis://localhost:6379"),
		server.WithRedisDB(15),
		server.WithRedisPassword("your-password"),
		server.WithNodeID("onetimeat-node-1"),
		server.WithMaxConcurrency(10),
	)
	if err != nil {
		slog.Error("failed to create server", "err", err)
		os.Exit(1)
	}

	// Register the onetime at handler.
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
			if err := sonic.ConfigFastest.Unmarshal(payload, &p); err != nil {
				return &types.NonRetryableError{Err: fmt.Errorf("invalid payload: %w", err)}
			}

			slog.Info("querying onetime at",
				"onetime_id", p.OnetimeID,
				"time", time.Now().Format(time.RFC3339),
			)
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

	// Register multiple one-time tasks.
	onetimes := []struct {
		ID     string
		AtTime time.Time
	}{
		{
			ID:     xid.New().String(),
			AtTime: time.Now().Add(1 * time.Minute),
		},
		{
			ID:     xid.New().String(),
			AtTime: time.Now().Add(2 * time.Minute),
		},
	}

	for _, onetime := range onetimes {
		payload, _ := sonic.ConfigFastest.Marshal(OnetimeAtPayload{OnetimeID: onetime.ID})
		uniqueKey := fmt.Sprintf("onetime-at-%s", onetime.ID)
		atTime := onetime.AtTime

		job, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{
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
			slog.Error("failed to enqueue onetime job", "onetime", onetime.ID, "err", err)
			continue
		}

		slog.Info("enqueued onetime job",
			"onetime", onetime.ID,
			"job_id", job.ID,
			"at_time", onetime.AtTime.Format(time.RFC3339),
		)
	}

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	slog.Info("shutting down")
	srv.Stop()
}
