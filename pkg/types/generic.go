package types

import (
	"context"
	"encoding/json"
	"fmt"
)

// TypedHandler wraps a type-safe handler function into a HandlerFunc.
// The payload is automatically unmarshalled from JSON into T.
//
//	srv.RegisterHandler(types.HandlerDefinition{
//	    TaskType: "order.process",
//	    Handler:  types.TypedHandler(func(ctx context.Context, order OrderPayload) error {
//	        // order is already unmarshalled
//	        return nil
//	    }),
//	})
func TypedHandler[T any](fn func(ctx context.Context, payload T) error) HandlerFunc {
	return func(ctx context.Context, raw json.RawMessage) error {
		var payload T
		if err := json.Unmarshal(raw, &payload); err != nil {
			return &NonRetryableError{Err: err}
		}
		return fn(ctx, payload)
	}
}

// TypedHandlerWithResult wraps a type-safe handler that returns output data.
// The payload is unmarshalled from JSON into T, and the output R is marshalled back.
//
//	srv.RegisterHandler(types.HandlerDefinition{
//	    TaskType:          "order.process",
//	    HandlerWithResult: types.TypedHandlerWithResult(func(ctx context.Context, order OrderPayload) (OrderResult, error) {
//	        return OrderResult{Status: "ok"}, nil
//	    }),
//	})
func TypedHandlerWithResult[T any, R any](fn func(ctx context.Context, payload T) (R, error)) HandlerFuncWithResult {
	return func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var payload T
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, &NonRetryableError{Err: err}
		}
		result, err := fn(ctx, payload)
		if err != nil {
			return nil, err
		}
		out, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return nil, &NonRetryableError{Err: marshalErr}
		}
		return out, nil
	}
}

// UpstreamPayload wraps an upstream task's result alongside the original task payload.
// Used with ResultFrom to automatically pipe upstream outputs to downstream tasks.
type UpstreamPayload[T any] struct {
	Upstream json.RawMessage `json:"upstream"`
	Original T               `json:"original"`
}

// TypedHandlerWithUpstream wraps a handler that receives both the upstream task's result
// and this task's original payload. Use with WorkflowTask.ResultFrom to enable
// automatic data flow between workflow tasks.
//
//	srv.RegisterHandler(types.HandlerDefinition{
//	    TaskType: "process.order",
//	    Handler:  types.TypedHandlerWithUpstream(func(ctx context.Context, upstream ValidateResult, original OrderPayload) error {
//	        // upstream is the result from the task specified in ResultFrom
//	        // original is this task's own payload
//	        return nil
//	    }),
//	})
func TypedHandlerWithUpstream[U any, T any](fn func(ctx context.Context, upstream U, original T) error) HandlerFunc {
	return func(ctx context.Context, raw json.RawMessage) error {
		var envelope UpstreamPayload[T]
		if err := json.Unmarshal(raw, &envelope); err != nil {
			// Fallback: try to unmarshal as plain T (no upstream data).
			var payload T
			if err2 := json.Unmarshal(raw, &payload); err2 != nil {
				return &NonRetryableError{Err: err}
			}
			var zero U
			return fn(ctx, zero, payload)
		}
		var upstream U
		if len(envelope.Upstream) > 0 {
			if err := json.Unmarshal(envelope.Upstream, &upstream); err != nil {
				return &NonRetryableError{Err: err}
			}
		}
		return fn(ctx, upstream, envelope.Original)
	}
}

// TypedHandlerWithUpstreamResult wraps a handler that receives upstream result + original payload
// and returns output data. Combines ResultFrom piping with result production.
func TypedHandlerWithUpstreamResult[U any, T any, R any](fn func(ctx context.Context, upstream U, original T) (R, error)) HandlerFuncWithResult {
	return func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var envelope UpstreamPayload[T]
		if err := json.Unmarshal(raw, &envelope); err != nil {
			var payload T
			if err2 := json.Unmarshal(raw, &payload); err2 != nil {
				return nil, &NonRetryableError{Err: err}
			}
			var zero U
			result, err := fn(ctx, zero, payload)
			if err != nil {
				return nil, err
			}
			out, marshalErr := json.Marshal(result)
			if marshalErr != nil {
				return nil, &NonRetryableError{Err: marshalErr}
			}
			return out, nil
		}
		var upstream U
		if len(envelope.Upstream) > 0 {
			if err := json.Unmarshal(envelope.Upstream, &upstream); err != nil {
				return nil, &NonRetryableError{Err: err}
			}
		}
		result, err := fn(ctx, upstream, envelope.Original)
		if err != nil {
			return nil, err
		}
		out, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return nil, &NonRetryableError{Err: marshalErr}
		}
		return out, nil
	}
}

// ConditionResult wraps the route index chosen by a condition handler.
// The orchestrator reads this from the job result to determine branching.
type ConditionResult struct {
	Route uint `json:"route"`
}

// TypedConditionHandler wraps a type-safe condition handler that returns a route index.
// The route index maps to ConditionRoutes in WorkflowTask to determine the next branch.
//
//	srv.RegisterHandler(types.HandlerDefinition{
//	    TaskType:          "check.threshold",
//	    HandlerWithResult: types.TypedConditionHandler(func(ctx context.Context, p ThresholdPayload) (uint, error) {
//	        if p.Value > 100 { return 0, nil } // route 0: high
//	        return 1, nil                       // route 1: low
//	    }),
//	})
func TypedConditionHandler[T any](fn func(ctx context.Context, payload T) (uint, error)) HandlerFuncWithResult {
	return func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var payload T
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, &NonRetryableError{Err: err}
		}
		route, err := fn(ctx, payload)
		if err != nil {
			return nil, err
		}
		out, marshalErr := json.Marshal(ConditionResult{Route: route})
		if marshalErr != nil {
			return nil, &NonRetryableError{Err: marshalErr}
		}
		return out, nil
	}
}

// SubflowResult wraps dynamically generated tasks from a subflow handler.
type SubflowResult struct {
	Tasks []WorkflowTask `json:"tasks"`
}

// TypedSubflowHandler wraps a handler that dynamically generates workflow tasks at runtime.
// The returned tasks are injected into the workflow instance and dispatched by the orchestrator.
//
//	srv.RegisterHandler(types.HandlerDefinition{
//	    TaskType:          "generate.subtasks",
//	    HandlerWithResult: types.TypedSubflowHandler(func(ctx context.Context, p Input) ([]types.WorkflowTask, error) {
//	        return []types.WorkflowTask{...}, nil
//	    }),
//	})
func TypedSubflowHandler[T any](fn func(ctx context.Context, payload T) ([]WorkflowTask, error)) HandlerFuncWithResult {
	return func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var payload T
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, &NonRetryableError{Err: err}
		}
		tasks, err := fn(ctx, payload)
		if err != nil {
			return nil, err
		}
		// Validate that generated tasks have names and no collisions.
		names := make(map[string]struct{}, len(tasks))
		for _, t := range tasks {
			if t.Name == "" {
				return nil, &NonRetryableError{Err: fmt.Errorf("subflow task has empty name")}
			}
			if _, dup := names[t.Name]; dup {
				return nil, &NonRetryableError{Err: fmt.Errorf("subflow has duplicate task name %q", t.Name)}
			}
			names[t.Name] = struct{}{}
		}
		out, marshalErr := json.Marshal(SubflowResult{Tasks: tasks})
		if marshalErr != nil {
			return nil, &NonRetryableError{Err: marshalErr}
		}
		return out, nil
	}
}
