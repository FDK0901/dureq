// Package main demonstrates dureq retention/archival — automatic cleanup
// of completed jobs after a configurable retention period.
//
// Scenario: A server configured with a short 10-second retention period.
// Several immediate jobs are enqueued and complete quickly. After the
// retention window passes, the completed jobs are automatically cleaned up
// by the leader's lifecycle sweeper.
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
	// Configure a short retention period (10s) for demonstration.
	// In production, this would typically be hours or days.

	srv, err := dureq.NewServer(
		dureq.WithRedisURL("redis://localhost:6379"),
		dureq.WithRedisDB(15),
		dureq.WithRedisPassword("your-password"),
		dureq.WithNodeID("retention-demo-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithRetentionPeriod(10*time.Second),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	// Register a simple handler that completes quickly.
	handlers := []types.HandlerDefinition{
		{
			TaskType: "demo.task",
			Timeout:  10 * time.Second,
			Handler: func(_ context.Context, payload json.RawMessage) error {
				logger.Info().String("payload", string(payload)).Msg("processing task")
				time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
				return nil
			},
		},
	}

	for _, h := range handlers {
		if err := srv.RegisterHandler(h); err != nil {
			logger.Error().String("task_type", string(h.TaskType)).Err(err).Msg("failed to register handler")
			os.Exit(1)
		}
	}

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	// Give the server a moment to elect a leader.
	time.Sleep(3 * time.Second)

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

	// Enqueue several immediate jobs.
	jobIDs := make([]string, 0, 5)
	for i := 1; i <= 5; i++ {
		payload, _ := json.Marshal(map[string]any{
			"task_number": i,
			"message":     fmt.Sprintf("retention demo task #%d", i),
		})

		job, err := cli.Enqueue(ctx, "demo.task", payload)
		if err != nil {
			logger.Error().Int("task_number", i).Err(err).Msg("failed to enqueue job")
			continue
		}

		jobIDs = append(jobIDs, job.ID)
		logger.Info().String("job_id", job.ID).Int("task_number", i).Msg("job enqueued")
	}

	logger.Info().Int("count", len(jobIDs)).Msg("all jobs enqueued")

	// Poll to show jobs completing and then being cleaned up.
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		checkCount := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				checkCount++
				found := 0
				completed := 0
				missing := 0

				for _, id := range jobIDs {
					job, err := cli.GetJob(ctx, id)
					if err != nil {
						// Job has been cleaned up by retention.
						missing++
						continue
					}
					found++
					if job.Status == types.JobStatusCompleted {
						completed++
					}
				}

				logger.Info().Int("check", checkCount).Int("found", found).Int("completed", completed).Int("cleaned_up", missing).Int("total", len(jobIDs)).Msg("retention status")

				if missing == len(jobIDs) {
					logger.Info().Msg("all completed jobs have been cleaned up by retention policy!")
					return
				}

				if checkCount > 20 {
					logger.Info().Msg("demo timeout reached, stopping poll")
					return
				}
			}
		}
	}()

	// --- Wait for shutdown ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
