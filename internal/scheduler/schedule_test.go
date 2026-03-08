package scheduler

import (
	"testing"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

func durationPtr(d time.Duration) *types.Duration {
	v := types.Duration(d)
	return &v
}

func TestNextRunTime_Duration_NoJitter(t *testing.T) {
	after := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	interval := 10 * time.Minute

	s := types.Schedule{
		Type:     types.ScheduleDuration,
		Interval: durationPtr(interval),
	}

	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatalf("NextRunTime() error: %v", err)
	}

	expected := after.Add(interval)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

func TestNextRunTime_Duration_WithJitter(t *testing.T) {
	after := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	interval := 10 * time.Minute
	jitterMax := 30 * time.Second

	s := types.Schedule{
		Type:     types.ScheduleDuration,
		Interval: durationPtr(interval),
		Jitter:   durationPtr(jitterMax),
	}

	baseExpected := after.Add(interval)

	// Run multiple iterations to verify the result is always within [base, base+jitter).
	for i := 0; i < 100; i++ {
		next, err := NextRunTime(s, after)
		if err != nil {
			t.Fatalf("NextRunTime() error on iteration %d: %v", i, err)
		}

		if next.Before(baseExpected) {
			t.Fatalf("iteration %d: next %v is before base %v", i, next, baseExpected)
		}
		upperBound := baseExpected.Add(jitterMax)
		if !next.Before(upperBound) {
			t.Fatalf("iteration %d: next %v is at or after upper bound %v", i, next, upperBound)
		}
	}
}

func TestNextRunTime_Duration_JitterProducesVariation(t *testing.T) {
	after := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	interval := 5 * time.Minute
	jitterMax := 10 * time.Second

	s := types.Schedule{
		Type:     types.ScheduleDuration,
		Interval: durationPtr(interval),
		Jitter:   durationPtr(jitterMax),
	}

	// Collect multiple results and check that not all are identical,
	// confirming jitter introduces randomness.
	results := make(map[int64]struct{})
	for i := 0; i < 50; i++ {
		next, err := NextRunTime(s, after)
		if err != nil {
			t.Fatalf("NextRunTime() error: %v", err)
		}
		results[next.UnixNano()] = struct{}{}
	}

	if len(results) < 2 {
		t.Fatal("expected jitter to produce at least 2 distinct values over 50 iterations, got 1")
	}
}

func TestNextRunTime_Duration_ZeroJitter(t *testing.T) {
	after := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	interval := 10 * time.Minute

	// Jitter of 0 should behave the same as no jitter.
	s := types.Schedule{
		Type:     types.ScheduleDuration,
		Interval: durationPtr(interval),
		Jitter:   durationPtr(0),
	}

	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatalf("NextRunTime() error: %v", err)
	}

	expected := after.Add(interval)
	if !next.Equal(expected) {
		t.Fatalf("expected %v with zero jitter, got %v", expected, next)
	}
}
