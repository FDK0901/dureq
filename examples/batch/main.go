// Package main demonstrates dureq batch processing — a shared preprocessing
// step (onetime) followed by parallel per-item processing with individual
// success/failure tracking and result collection.
//
// Scenario: Image processing pipeline
//
//  1. onetime: download a shared background template image
//  2. per-item: overlay each user's text onto the template and produce a result URL
//
// The batch orchestrator dispatches the onetime job first, waits for completion,
// then dispatches items in chunks (backpressure). Individual item failures
// don't stop the rest (continue_on_error policy).
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
		dureq.WithNodeID("batch-demo-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register the onetime handler (shared preprocessing).
	onetimeTaskType := types.TaskType("image.download_template")
	itemTaskType := types.TaskType("image.overlay_text")

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: onetimeTaskType,
		Timeout:  30 * time.Second,
		HandlerWithResult: func(_ context.Context, payload json.RawMessage) (json.RawMessage, error) {
			logger.Info().String("payload", string(payload)).Msg("downloading shared template image")
			time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)

			// Simulate returning a template URL/path.
			result := map[string]string{
				"template_url": "https://cdn.example.com/templates/background-v2.png",
				"dimensions":   "1920x1080",
			}
			data, _ := json.Marshal(result)
			return data, nil
		},
	})

	// Register the per-item handler.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: itemTaskType,
		Timeout:  15 * time.Second,
		HandlerWithResult: func(_ context.Context, payload json.RawMessage) (json.RawMessage, error) {
			var item struct {
				UserID string `json:"user_id"`
				Text   string `json:"text"`
			}
			json.Unmarshal(payload, &item)

			logger.Info().String("user_id", item.UserID).String("text", item.Text).Msg("overlaying text onto template")
			time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)

			// Simulate 10% failure rate.
			if rand.Float64() < 0.1 {
				return nil, fmt.Errorf("render failed for user %s: GPU out of memory", item.UserID)
			}

			result := map[string]string{
				"output_url": fmt.Sprintf("https://cdn.example.com/output/%s.png", item.UserID),
			}
			data, _ := json.Marshal(result)
			return data, nil
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

	// Build batch items.
	users := []struct {
		ID   string
		Text string
	}{
		{"user-001", "Happy Birthday, Alice!"},
		{"user-002", "Welcome to the Team, Bob!"},
		{"user-003", "Congratulations, Charlie!"},
		{"user-004", "Thank You, Diana!"},
		{"user-005", "Happy Anniversary, Eve!"},
		{"user-006", "Best Wishes, Frank!"},
		{"user-007", "Good Luck, Grace!"},
		{"user-008", "Season's Greetings, Hank!"},
	}

	items := make([]types.BatchItem, len(users))
	for i, u := range users {
		payload, _ := json.Marshal(map[string]string{
			"user_id": u.ID,
			"text":    u.Text,
		})
		items[i] = types.BatchItem{
			ID:      u.ID,
			Payload: payload,
		}
	}

	templatePayload, _ := json.Marshal(map[string]string{
		"template_id": "birthday-v2",
		"source":      "https://assets.example.com/templates/birthday-v2.psd",
	})

	batch, err := cl.EnqueueBatch(ctx, types.BatchDefinition{
		Name:            "birthday-cards-batch",
		OnetimeTaskType: &onetimeTaskType,
		OnetimePayload:  templatePayload,
		ItemTaskType:    itemTaskType,
		Items:           items,
		FailurePolicy:   types.BatchContinueOnError,
		ChunkSize:       5,
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue batch")
		os.Exit(1)
	}

	logger.Info().String("batch_id", batch.ID).String("name", batch.Name).Int("total_items", batch.TotalItems).Msg("batch submitted")

	// Poll for progress.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b, err := cl.GetBatch(ctx, batch.ID)
				if err != nil {
					logger.Warn().Err(err).Msg("failed to poll batch status")
					continue
				}
				logger.Info().
					String("batch_id", b.ID).
					String("status", string(b.Status)).
					Int("completed", b.CompletedItems).
					Int("failed", b.FailedItems).
					Int("running", b.RunningItems).
					Int("pending", b.PendingItems).
					Int("total", b.TotalItems).
					Msg("batch progress")

				if b.Status.IsTerminal() {
					// Fetch and display results.
					results, err := cl.GetBatchResults(ctx, batch.ID)
					if err != nil {
						logger.Error().Err(err).Msg("failed to get batch results")
						return
					}

					logger.Info().Int("count", len(results)).Msg("batch results")
					for _, r := range results {
						if r.Success {
							logger.Info().String("item_id", r.ItemID).String("output", string(r.Output)).Msg("  item succeeded")
						} else {
							errStr := ""
							if r.Error != nil {
								errStr = *r.Error
							}
							logger.Warn().String("item_id", r.ItemID).String("error", errStr).Msg("  item failed")
						}
					}
					return
				}
			}
		}
	}()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
