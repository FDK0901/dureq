// Package sideeffect provides at-most-once execution helpers for external
// side effects within dureq handlers.
//
// When a handler calls an external API (payment, email, webhook), a crash
// after the API call but before job completion causes the handler to re-run,
// potentially executing the side effect twice. Step() prevents this by
// persisting the result in Redis before returning it to the handler.
//
// Usage within a handler:
//
//	func handlePayment(ctx context.Context, order OrderPayload) error {
//	    result, err := sideeffect.Step(ctx, "charge", func(ctx context.Context) (string, error) {
//	        return paymentGateway.Charge(order.ID, order.Amount)
//	    })
//	    if err != nil {
//	        return err
//	    }
//	    // result is the charge reference — cached on retry
//	    ...
//	}
package sideeffect

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/FDK0901/dureq/pkg/types"
)

const defaultTTLSeconds = 86400 // 24 hours

// Step executes fn at most once per (runID, stepKey) pair.
//
// On first call: claims the step, executes fn, stores the result, returns it.
// On retry (same runID): returns the cached result without re-executing fn.
//
// The stepKey must be unique within the handler for a given run. Use
// descriptive names like "charge-payment", "send-email", "update-inventory".
//
// Step requires that the context was created by the dureq worker (contains
// RunID and SideEffectStore). It will return an error if called outside a
// handler execution context.
func Step[T any](ctx context.Context, stepKey string, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T

	runID := types.GetRunID(ctx)
	if runID == "" {
		return zero, fmt.Errorf("sideeffect.Step: no RunID in context (must be called from a dureq handler)")
	}

	store := types.GetSideEffectStore(ctx)
	if store == nil {
		return zero, fmt.Errorf("sideeffect.Step: no SideEffectStore in context (must be called from a dureq handler)")
	}

	// Try to claim or get cached result.
	ttl := types.GetSideEffectTTL(ctx)
	if ttl <= 0 {
		ttl = defaultTTLSeconds
	}
	cached, done, err := store.ClaimSideEffect(ctx, runID, stepKey, ttl)
	if err != nil {
		return zero, fmt.Errorf("sideeffect.Step(%s): %w", stepKey, err)
	}

	if done {
		// Step was already completed — unmarshal cached result.
		var result T
		if err := json.Unmarshal([]byte(cached), &result); err != nil {
			return zero, fmt.Errorf("sideeffect.Step(%s): unmarshal cached result: %w", stepKey, err)
		}
		return result, nil
	}

	// We claimed the step — execute the function.
	result, err := fn(ctx)
	if err != nil {
		// Execution failed — do NOT mark as done so it can be retried.
		return zero, err
	}

	// Store the result.
	data, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return zero, fmt.Errorf("sideeffect.Step(%s): marshal result: %w", stepKey, marshalErr)
	}
	if completeErr := store.CompleteSideEffect(ctx, runID, stepKey, string(data)); completeErr != nil {
		// Non-fatal: the step executed successfully, so return the result.
		// The next retry will re-execute (at-least-once, not at-most-once).
		// This is a best-effort cache.
		_ = completeErr
	}

	return result, nil
}

// StepOnce executes fn at most once per (runID, stepKey) pair.
// This is a convenience wrapper for fire-and-forget side effects
// that don't return a meaningful result.
func StepOnce(ctx context.Context, stepKey string, fn func(ctx context.Context) error) error {
	_, err := Step(ctx, stepKey, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}

// ExternalCall wraps an external API call with a deterministic idempotency key.
// The callback receives the idempotency key to pass to the external service.
//
// Usage:
//
//	err := sideeffect.ExternalCall(ctx, "stripe-charge", func(ctx context.Context, idempotencyKey string) error {
//	    _, err := stripe.Charges.Create(&stripe.ChargeParams{
//	        IdempotencyKey: &idempotencyKey,
//	        ...
//	    })
//	    return err
//	})
func ExternalCall(ctx context.Context, stepKey string, fn func(ctx context.Context, idempotencyKey string) error) error {
	runID := types.GetRunID(ctx)
	idempotencyKey := fmt.Sprintf("dureq:%s:%s", runID, stepKey)

	return StepOnce(ctx, stepKey, func(ctx context.Context) error {
		return fn(ctx, idempotencyKey)
	})
}
