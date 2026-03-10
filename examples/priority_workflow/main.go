// Package main demonstrates workflow task priority in dureq — when multiple
// tasks become ready simultaneously, higher-priority tasks are dispatched first.
//
// Scenario: After a start task completes, three tasks become ready at once
// with different priorities. They are dispatched in priority order.
//
//	start → task_critical(priority=10) ─┐
//	      → task_normal(priority=5)    ─┼─ finish
//	      → task_low(priority=1)       ─┘
//
// With concurrency=1 on the server, the execution order is deterministic:
// critical → normal → low → finish.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
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

	// Track execution order.
	var mu sync.Mutex
	var executionOrder []string

	// --- Server Side ---

	srv, err := dureq.NewServer(
		dureq.WithRedisURL("redis://localhost:6381"),
		dureq.WithRedisDB(15),
		dureq.WithRedisPassword("your-password"),
		dureq.WithNodeID("priority-workflow-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	makeHandler := func(name string, duration time.Duration) types.HandlerFunc {
		return func(_ context.Context, payload json.RawMessage) error {
			mu.Lock()
			executionOrder = append(executionOrder, name)
			mu.Unlock()
			logger.Info().String("task", name).Msg("executing")
			time.Sleep(duration)
			return nil
		}
	}

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "prio.start",
		Timeout:  10 * time.Second,
		Handler:  makeHandler("start", 300*time.Millisecond),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "prio.critical",
		Timeout:  10 * time.Second,
		Handler:  makeHandler("critical", 500*time.Millisecond),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "prio.normal",
		Timeout:  10 * time.Second,
		Handler:  makeHandler("normal", 500*time.Millisecond),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "prio.low",
		Timeout:  10 * time.Second,
		Handler:  makeHandler("low", 500*time.Millisecond),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "prio.finish",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			mu.Lock()
			executionOrder = append(executionOrder, "finish")
			order := make([]string, len(executionOrder))
			copy(order, executionOrder)
			mu.Unlock()
			logger.Info().Msg("all tasks done")
			logger.Info().Msg(fmt.Sprintf("execution order: %v", order))
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	time.Sleep(3 * time.Second)

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

	payload, _ := json.Marshal(map[string]string{"demo": "priority"})

	wfDef := types.WorkflowDefinition{
		Name: "priority-demo",
		Tasks: []types.WorkflowTask{
			{
				Name:     "start",
				TaskType: "prio.start",
				Payload:  payload,
			},
			{
				Name:      "task_critical",
				TaskType:  "prio.critical",
				Payload:   payload,
				DependsOn: []string{"start"},
				Priority:  10, // highest priority
			},
			{
				Name:      "task_normal",
				TaskType:  "prio.normal",
				Payload:   payload,
				DependsOn: []string{"start"},
				Priority:  5,
			},
			{
				Name:      "task_low",
				TaskType:  "prio.low",
				Payload:   payload,
				DependsOn: []string{"start"},
				Priority:  1, // lowest priority
			},
			{
				Name:      "finish",
				TaskType:  "prio.finish",
				Payload:   payload,
				DependsOn: []string{"task_critical", "task_normal", "task_low"},
			},
		},
	}

	wf, err := cli.EnqueueWorkflow(ctx, wfDef, nil)
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue workflow")
		os.Exit(1)
	}

	logger.Info().String("workflow_id", wf.ID).Msg("enqueued priority workflow")

	// Poll workflow status.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				status, err := cli.GetWorkflow(ctx, wf.ID)
				if err != nil {
					continue
				}
				completed := 0
				for _, t := range status.Tasks {
					if t.Status == types.JobStatusCompleted {
						completed++
					}
				}
				logger.Info().String("status", string(status.Status)).Int("completed", completed).Int("total", len(status.Tasks)).Msg("workflow progress")

				if status.Status == types.WorkflowStatusCompleted {
					logger.Info().Msg("workflow completed!")
					return
				}
				if status.Status == types.WorkflowStatusFailed {
					logger.Error().Msg("workflow failed")
					return
				}
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println()
	logger.Info().Msg("shutting down")
	srv.Stop()
}
