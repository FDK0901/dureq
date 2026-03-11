// Package main demonstrates AllowFailure in dureq workflows — when a task
// with AllowFailure=true fails, the workflow continues instead of failing.
//
// Scenario: A data pipeline where one optional enrichment step may fail,
// but the rest of the pipeline should proceed.
//
//	fetch → enrich(AllowFailure=true) ─┐
//	      → transform                  ─┼─ aggregate
//	                                    ┘
//
// Even if enrich fails, transform + aggregate still complete.
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

	"github.com/FDK0901/dureq/examples/shared"
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

	var mu sync.Mutex
	var executionOrder []string
	track := func(name string) {
		mu.Lock()
		executionOrder = append(executionOrder, name)
		mu.Unlock()
	}

	// --- Server Side ---

	srv, err := dureq.NewServer(
		append(shared.ServerOptions(),
			dureq.WithNodeID("allow-failure-node-1"),
			dureq.WithMaxConcurrency(10),
			dureq.WithLogger(logger),
		)...,
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "af.fetch",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			track("fetch")
			logger.Info().Msg("fetching data")
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	})

	// This handler always fails — but AllowFailure=true means the workflow continues.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "af.enrich",
		Timeout:  10 * time.Second,
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts:  1,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   1.0,
		},
		Handler: func(_ context.Context, payload json.RawMessage) error {
			track("enrich")
			logger.Info().Msg("enriching data (will fail)")
			return &types.NonRetryableError{Err: fmt.Errorf("external enrichment service unavailable")}
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "af.transform",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			track("transform")
			logger.Info().Msg("transforming data")
			time.Sleep(300 * time.Millisecond)
			return nil
		},
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "af.aggregate",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			track("aggregate")
			mu.Lock()
			order := make([]string, len(executionOrder))
			copy(order, executionOrder)
			mu.Unlock()
			logger.Info().Msg(fmt.Sprintf("aggregating results — execution order: %v", order))
			return nil
		},
	})

	if err := srv.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}

	time.Sleep(3 * time.Second)

	// --- Client Side ---

	cli, err := dureq.NewClient(shared.ClientOptions()...)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create client")
		os.Exit(1)
	}
	defer cli.Close()

	payload, _ := json.Marshal(map[string]string{"demo": "allow_failure"})

	wfDef := types.WorkflowDefinition{
		Name: "allow-failure-demo",
		Tasks: []types.WorkflowTask{
			{
				Name:     "fetch",
				TaskType: "af.fetch",
				Payload:  payload,
			},
			{
				Name:         "enrich",
				TaskType:     "af.enrich",
				Payload:      payload,
				DependsOn:    []string{"fetch"},
				AllowFailure: true, // This task can fail without killing the workflow.
			},
			{
				Name:      "transform",
				TaskType:  "af.transform",
				Payload:   payload,
				DependsOn: []string{"fetch"},
			},
			{
				Name:      "aggregate",
				TaskType:  "af.aggregate",
				Payload:   payload,
				DependsOn: []string{"enrich", "transform"},
			},
		},
	}

	wf, err := cli.EnqueueWorkflow(ctx, wfDef, nil)
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue workflow")
		os.Exit(1)
	}

	logger.Info().String("workflow_id", wf.ID).Msg("enqueued allow-failure workflow")

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
				logger.Info().String("status", string(status.Status)).Msg("workflow progress")

				if status.Status == types.WorkflowStatusCompleted {
					// Show that enrich failed but workflow completed.
					enrichState := status.Tasks["enrich"]
					logger.Info().String("enrich_status", string(enrichState.Status)).Msg("enrich task result")
					logger.Info().Msg("workflow completed despite enrich failure!")
					return
				}
				if status.Status == types.WorkflowStatusFailed {
					logger.Error().Msg("workflow failed (unexpected)")
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
