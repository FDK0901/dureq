// Package main demonstrates dynamic subflow task generation in dureq workflows.
//
// Scenario: A data pipeline that fetches items, then dynamically generates
// one processing task per item at runtime.
//
//	fetch_items → generate_tasks(subflow) → [process_item_1, process_item_2, ...] → aggregate
//
// The generate_tasks handler returns a list of WorkflowTasks at runtime,
// which the orchestrator injects into the running workflow. The aggregate
// task runs after all dynamically generated tasks complete.
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

type FetchResult struct {
	Items []string `json:"items"`
}

type ItemPayload struct {
	Item string `json:"item"`
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
		dureq.WithNodeID("subflow-workflow-node-1"),
		dureq.WithMaxConcurrency(10),
		dureq.WithLogger(logger),
	)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create server")
		os.Exit(1)
	}

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "data.fetch_items",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			logger.Info().Msg("fetching items from data source")
			time.Sleep(500 * time.Millisecond)
			return nil
		},
	})

	// Subflow handler: dynamically generates processing tasks based on fetched items.
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "data.generate_tasks",
		Timeout:  10 * time.Second,
		HandlerWithResult: types.TypedSubflowHandler(func(_ context.Context, result FetchResult) ([]types.WorkflowTask, error) {
			logger.Info().Int("item_count", len(result.Items)).Msg("generating dynamic tasks")

			var tasks []types.WorkflowTask
			for _, item := range result.Items {
				itemPayload, _ := json.Marshal(ItemPayload{Item: item})
				tasks = append(tasks, types.WorkflowTask{
					Name:     fmt.Sprintf("process_%s", item),
					TaskType: "data.process_item",
					Payload:  itemPayload,
				})
			}
			return tasks, nil
		}),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "data.process_item",
		Timeout:  10 * time.Second,
		Handler: types.TypedHandler(func(_ context.Context, p ItemPayload) error {
			logger.Info().String("item", p.Item).Msg("processing item")
			time.Sleep(400 * time.Millisecond)
			return nil
		}),
	})

	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "data.aggregate",
		Timeout:  10 * time.Second,
		Handler: func(_ context.Context, payload json.RawMessage) error {
			logger.Info().Msg("aggregating results from all processed items")
			time.Sleep(300 * time.Millisecond)
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

	// The subflow generator receives the list of items to process.
	generatorPayload, _ := json.Marshal(FetchResult{
		Items: []string{"alpha", "beta", "gamma", "delta"},
	})

	wfDef := types.WorkflowDefinition{
		Name: "dynamic-subflow-pipeline",
		Tasks: []types.WorkflowTask{
			{
				Name:     "fetch_items",
				TaskType: "data.fetch_items",
				Payload:  generatorPayload,
			},
			{
				Name:      "generate_tasks",
				TaskType:  "data.generate_tasks",
				Payload:   generatorPayload,
				DependsOn: []string{"fetch_items"},
				Type:      types.WorkflowTaskSubflow,
			},
			{
				// aggregate depends on generate_tasks; dynamically generated
				// tasks also auto-depend on generate_tasks, so aggregate
				// effectively waits for all dynamic tasks to complete.
				Name:      "aggregate",
				TaskType:  "data.aggregate",
				Payload:   generatorPayload,
				DependsOn: []string{"generate_tasks"},
			},
		},
	}

	wf, err := cli.EnqueueWorkflow(ctx, wfDef, nil)
	if err != nil {
		logger.Error().Err(err).Msg("failed to enqueue workflow")
		os.Exit(1)
	}

	logger.Info().String("workflow_id", wf.ID).Msg("enqueued subflow workflow (will generate 4 dynamic tasks)")

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
					// Show dynamically injected task names.
					if gen, ok := status.Tasks["generate_tasks"]; ok && len(gen.SubflowTasks) > 0 {
						logger.Info().Int("injected_count", len(gen.SubflowTasks)).Msg("subflow injected tasks")
					}
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
