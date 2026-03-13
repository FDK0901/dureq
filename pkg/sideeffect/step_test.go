package sideeffect

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/FDK0901/dureq/pkg/types"
)

// mockSideEffectStore implements types.SideEffectStore in memory for testing.
type mockSideEffectStore struct {
	mu    sync.Mutex
	steps map[string]string // key = "runID:stepKey", value = "" (pending) or result (done)
}

func newMockStore() *mockSideEffectStore {
	return &mockSideEffectStore{steps: make(map[string]string)}
}

func (m *mockSideEffectStore) key(runID, stepKey string) string {
	return runID + ":" + stepKey
}

func (m *mockSideEffectStore) ClaimSideEffect(_ context.Context, runID, stepKey string, _ int) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.key(runID, stepKey)
	if result, exists := m.steps[k]; exists {
		if result == "" {
			return "", false, fmt.Errorf("side effect step %q is already being executed", stepKey)
		}
		return result, true, nil
	}
	m.steps[k] = "" // pending
	return "", false, nil
}

func (m *mockSideEffectStore) CompleteSideEffect(_ context.Context, runID, stepKey string, result string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.steps[m.key(runID, stepKey)] = result
	return nil
}

func testCtx(runID string, store types.SideEffectStore) context.Context {
	ctx := context.Background()
	ctx = types.WithRunID(ctx, runID)
	ctx = types.WithSideEffectStore(ctx, store)
	return ctx
}

func TestStep_FirstCallExecutes(t *testing.T) {
	store := newMockStore()
	ctx := testCtx("run-1", store)

	var calls int
	result, err := Step(ctx, "payment", func(ctx context.Context) (string, error) {
		calls++
		return "charge-ref-123", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "charge-ref-123" {
		t.Errorf("expected charge-ref-123, got %s", result)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestStep_SecondCallReturnsCached(t *testing.T) {
	store := newMockStore()
	ctx := testCtx("run-2", store)

	var calls int
	fn := func(ctx context.Context) (string, error) {
		calls++
		return "ref-abc", nil
	}

	// First call.
	_, err := Step(ctx, "email", fn)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Second call — should return cached.
	result, err := Step(ctx, "email", fn)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if result != "ref-abc" {
		t.Errorf("expected ref-abc, got %s", result)
	}
	if calls != 1 {
		t.Errorf("expected 1 call (cached on second), got %d", calls)
	}
}

func TestStep_DifferentKeysExecuteSeparately(t *testing.T) {
	store := newMockStore()
	ctx := testCtx("run-3", store)

	var calls int
	fn := func(ctx context.Context) (int, error) {
		calls++
		return calls, nil
	}

	r1, _ := Step(ctx, "step-a", fn)
	r2, _ := Step(ctx, "step-b", fn)

	if r1 != 1 {
		t.Errorf("step-a: expected 1, got %d", r1)
	}
	if r2 != 2 {
		t.Errorf("step-b: expected 2, got %d", r2)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

func TestStep_FailedExecutionNotCached(t *testing.T) {
	store := newMockStore()
	ctx := testCtx("run-4", store)

	// Reset the store state so the step can be re-claimed after failure.
	var calls int
	fn := func(ctx context.Context) (string, error) {
		calls++
		if calls == 1 {
			return "", fmt.Errorf("transient error")
		}
		return "success", nil
	}

	// First call fails.
	_, err := Step(ctx, "webhook", fn)
	if err == nil {
		t.Fatal("expected error on first call")
	}

	// Reset pending state to simulate a new execution attempt.
	store.mu.Lock()
	delete(store.steps, "run-4:webhook")
	store.mu.Unlock()

	// Second call succeeds.
	result, err := Step(ctx, "webhook", fn)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if result != "success" {
		t.Errorf("expected success, got %s", result)
	}
}

func TestStep_NoRunID_ReturnsError(t *testing.T) {
	ctx := context.Background()
	_, err := Step(ctx, "step", func(ctx context.Context) (string, error) {
		return "x", nil
	})
	if err == nil {
		t.Fatal("expected error when no RunID in context")
	}
}

func TestStep_NoStore_ReturnsError(t *testing.T) {
	ctx := types.WithRunID(context.Background(), "run-x")
	_, err := Step(ctx, "step", func(ctx context.Context) (string, error) {
		return "x", nil
	})
	if err == nil {
		t.Fatal("expected error when no SideEffectStore in context")
	}
}

func TestStepOnce_Works(t *testing.T) {
	store := newMockStore()
	ctx := testCtx("run-5", store)

	var calls int
	err := StepOnce(ctx, "fire-and-forget", func(ctx context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second call should not execute.
	err = StepOnce(ctx, "fire-and-forget", func(ctx context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error on retry: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestExternalCall_ProvidesIdempotencyKey(t *testing.T) {
	store := newMockStore()
	ctx := testCtx("run-6", store)

	var receivedKey string
	err := ExternalCall(ctx, "stripe-charge", func(ctx context.Context, idempotencyKey string) error {
		receivedKey = idempotencyKey
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "dureq:run-6:stripe-charge"
	if receivedKey != expected {
		t.Errorf("expected key %q, got %q", expected, receivedKey)
	}
}
