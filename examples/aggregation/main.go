// Package main demonstrates dureq group aggregation — collecting individual
// items into groups that are automatically flushed and processed as a single
// aggregated job.
//
// Scenario: Event analytics pipeline
//
//  1. Multiple producers emit individual page-view events for different users
//  2. Events are grouped by user ID
//  3. After a grace period (no new events) or when the group reaches max size,
//     all events for that user are aggregated into a single batch payload
//  4. A handler processes the aggregated batch of events at once
//
// This reduces per-event overhead and enables efficient bulk inserts.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"

	"github.com/FDK0901/dureq/pkg/dureq"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/rs/zerolog"
)

// PageView represents a single page-view event.
type PageView struct {
	UserID    string `json:"user_id"`
	Page      string `json:"page"`
	Timestamp string `json:"timestamp"`
}

// AggregatedPageViews is the merged payload produced by the aggregator.
type AggregatedPageViews struct {
	UserID string     `json:"user_id"`
	Count  int        `json:"count"`
	Events []PageView `json:"events"`
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Server Side ---

	// Define the aggregator function: merges individual payloads into one.
	aggregator := types.GroupAggregatorFunc(
		func(group string, payloads []json.RawMessage) (json.RawMessage, error) {
			var events []PageView
			for _, p := range payloads {
				var pv PageView
				if err := json.Unmarshal(p, &pv); err != nil {
					continue
				}
				events = append(events, pv)
			}

			result := AggregatedPageViews{
				UserID: group,
				Count:  len(events),
				Events: events,
			}
			return json.Marshal(result)
		},
	)

	srv, err := dureq.NewServer(
		dureq.WithRedisURL("redis://localhost:6381"),
		dureq.WithRedisDB(15),
		dureq.WithRedisPassword("your-password"),
		dureq.WithNodeID("aggregation-demo-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
		dureq.WithGroupAggregation(types.GroupConfig{
			Aggregator:  aggregator,
			GracePeriod: 3 * time.Second,  // flush 3s after last item added
			MaxDelay:    15 * time.Second, // force flush after 15s regardless
			MaxSize:     10,               // flush immediately when 10 items in group
		}),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register the handler that processes aggregated page-view batches.
	taskType := types.TaskType("analytics.process_pageviews")

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: taskType,
		Timeout:  30 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			var batch AggregatedPageViews
			if err := json.Unmarshal(payload, &batch); err != nil {
				return fmt.Errorf("unmarshal aggregated payload: %w", err)
			}

			logger.Info().
				String("user_id", batch.UserID).
				Int("event_count", batch.Count).
				Msg("processing aggregated page views")

			for _, ev := range batch.Events {
				logger.Info().
					String("user_id", ev.UserID).
					String("page", ev.Page).
					String("timestamp", ev.Timestamp).
					Msg("  event")
			}

			// Simulate bulk insert latency.
			time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	// --- Client Side ---

	time.Sleep(2 * time.Second) // wait for leader election

	cl, err := dureq.NewClient(
		dureq.WithClientRedisURL("redis://localhost:6381"),
		dureq.WithClientRedisPassword("your-password"),
		dureq.WithClientRedisDB(15),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cl.Close()

	// Simulate two users generating page-view events.
	users := []string{"user-alice", "user-bob"}
	pages := []string{"/home", "/products", "/cart", "/checkout", "/profile", "/settings"}

	// Produce events in bursts.
	logger.Info().Msg("producing page-view events...")

	for round := 0; round < 3; round++ {
		for _, userID := range users {
			numEvents := 2 + rand.Intn(4) // 2-5 events per user per round
			for i := 0; i < numEvents; i++ {
				page := pages[rand.Intn(len(pages))]
				payload, _ := json.Marshal(PageView{
					UserID:    userID,
					Page:      page,
					Timestamp: time.Now().Format(time.RFC3339),
				})

				size, err := cl.EnqueueGroup(ctx, types.EnqueueGroupOption{
					Group:    userID,
					TaskType: taskType,
					Payload:  payload,
				})
				if err != nil {
					logger.Error().Err(err).String("user", userID).Msg("failed to enqueue group item")
					continue
				}
				logger.Info().
					String("user", userID).
					String("page", page).
					Int64("group_size", size).
					Msg("enqueued page-view event")
			}
		}

		// Pause between bursts to demonstrate grace period behavior.
		if round < 2 {
			logger.Info().Int("round", round+1).Msg("pausing between bursts...")
			time.Sleep(1 * time.Second)
		}
	}

	logger.Info().Msg("all events produced — waiting for aggregation flush and processing...")

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
