// Package main demonstrates dureq batch processing using the mux handler
// pattern — pattern-based routing, global middleware, per-handler middleware,
// and context utilities for metadata access.
//
// Compared to the plain batch example, this version:
//   - Registers a single "image.*" pattern handler that dispatches by task type
//   - Adds global logging middleware via srv.Use()
//   - Adds per-handler timing middleware
//   - Uses context utilities (GetJobID, GetTaskType, GetAttempt, GetHeaders, etc.)
//
// Scenario: Same image processing pipeline as the plain batch example.
//
//  1. onetime: download a shared background template image
//  2. per-item: overlay each user's text onto the template and produce a result URL
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

// loggingMiddleware is a global middleware that logs every handler invocation
// with job metadata extracted from the context.
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

// timingMiddleware is a per-handler middleware that measures execution duration.
func timingMiddleware(logger *zerolog.Logger) types.MiddlewareFunc {
	return func(next types.HandlerFunc) types.HandlerFunc {
		return func(ctx context.Context, payload json.RawMessage) error {
			start := time.Now()
			err := next(ctx, payload)
			elapsed := time.Since(start)

			taskType := types.GetTaskType(ctx)
			logger.Info().
				Str("task_type", string(taskType)).
				Dur("elapsed_ms", elapsed).
				Msg("[middleware:timing] execution time")
			return err
		}
	}
}

// imageHandler is a single handler registered for the "image.*" pattern.
// It dispatches internally based on the actual task type from the context.
func imageHandler(logger *zerolog.Logger) types.HandlerFuncWithResult {
	return func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		taskType := types.GetTaskType(ctx)
		jobID := types.GetJobID(ctx)
		headers := types.GetHeaders(ctx)

		logger.Info().
			Str("task_type", string(taskType)).
			Str("job_id", jobID).
			Interface("headers", headers).
			Msg("image handler dispatching")

		switch taskType {
		case "image.download_template":
			logger.Info().Str("payload", string(payload)).Msg("downloading shared template image")
			time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)

			result := map[string]string{
				"template_url": "https://cdn.example.com/templates/background-v2.png",
				"dimensions":   "1920x1080",
			}
			data, _ := json.Marshal(result)
			return data, nil

		case "image.overlay_text":
			var item struct {
				UserID string `json:"user_id"`
				Text   string `json:"text"`
			}
			json.Unmarshal(payload, &item)

			logger.Info().Str("user_id", item.UserID).Str("text", item.Text).Msg("overlaying text onto template")
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

		default:
			return nil, fmt.Errorf("unknown image task type: %s", taskType)
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
		dureq.WithNodeID("batch-mux-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register global middleware — applies to ALL handlers.
	srv.Use(loggingMiddleware(&zl))

	// Register a single pattern handler for all "image.*" task types.
	// The wildcard pattern matches image.download_template, image.overlay_text.
	err = srv.RegisterHandler(types.HandlerDefinition{
		TaskType:          "image.*", // pattern matching
		Timeout:           30 * time.Second,
		HandlerWithResult: imageHandler(&zl),
		// Per-handler middleware — applied after global middleware.
		Middlewares: []types.MiddlewareFunc{
			timingMiddleware(&zl),
		},
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to register image handler")
		os.Exit(1)
	}

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

	onetimeTaskType := types.TaskType("image.download_template")
	itemTaskType := types.TaskType("image.overlay_text")

	templatePayload, _ := json.Marshal(map[string]string{
		"template_id": "birthday-v2",
		"source":      "https://assets.example.com/templates/birthday-v2.psd",
	})

	batch, err := cl.EnqueueBatch(ctx, types.BatchDefinition{
		Name:            "birthday-cards-batch-mux",
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
