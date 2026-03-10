package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/redis/rueidis"
)

func newTestClient(t *testing.T) rueidis.Client {
	t.Helper()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"localhost:6381"},
		SelectDB:    15,
		Password:    "your-password",
	})
	if err != nil {
		t.Fatalf("failed to connect to Redis: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestLimiter_Allow_ConsumesTokens(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	// Clean up any leftover key from a previous run.
	prefix := "dureq_test_rl_" + time.Now().Format("20060102150405")
	limiter := New(rdb, prefix)

	cfg := Config{
		MaxTokens:  5,
		RefillRate: 0.0, // no refill so we can exhaust the bucket
	}

	// Should allow exactly MaxTokens requests of 1 token each.
	for i := 0; i < 5; i++ {
		allowed, err := limiter.Allow(ctx, "tier1", 1, cfg)
		if err != nil {
			t.Fatalf("Allow() returned error on request %d: %v", i+1, err)
		}
		if !allowed {
			t.Fatalf("Allow() denied request %d, expected allowed", i+1)
		}
	}

	// The 6th request should be denied.
	allowed, err := limiter.Allow(ctx, "tier1", 1, cfg)
	if err != nil {
		t.Fatalf("Allow() returned error: %v", err)
	}
	if allowed {
		t.Fatal("Allow() should have denied the 6th request, bucket should be empty")
	}
}

func TestLimiter_Allow_BurstConsumption(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	prefix := "dureq_test_rl_burst_" + time.Now().Format("20060102150405")
	limiter := New(rdb, prefix)

	cfg := Config{
		MaxTokens:  10,
		RefillRate: 0.0,
	}

	// Consume 7 tokens at once.
	allowed, err := limiter.Allow(ctx, "burst", 7, cfg)
	if err != nil {
		t.Fatalf("Allow() error: %v", err)
	}
	if !allowed {
		t.Fatal("Allow() denied burst of 7, expected allowed")
	}

	// Only 3 tokens remain, requesting 5 should be denied.
	allowed, err = limiter.Allow(ctx, "burst", 5, cfg)
	if err != nil {
		t.Fatalf("Allow() error: %v", err)
	}
	if allowed {
		t.Fatal("Allow() should have denied request for 5 tokens when only 3 remain")
	}

	// But requesting 3 should succeed.
	allowed, err = limiter.Allow(ctx, "burst", 3, cfg)
	if err != nil {
		t.Fatalf("Allow() error: %v", err)
	}
	if !allowed {
		t.Fatal("Allow() denied request for 3 tokens, expected allowed")
	}
}

func TestLimiter_Allow_Refill(t *testing.T) {
	rdb := newTestClient(t)
	ctx := context.Background()

	prefix := "dureq_test_rl_refill_" + time.Now().Format("20060102150405")
	limiter := New(rdb, prefix)

	cfg := Config{
		MaxTokens:  2,
		RefillRate: 100.0, // 100 tokens/sec — should refill quickly
	}

	// Drain the bucket.
	for i := 0; i < 2; i++ {
		_, _ = limiter.Allow(ctx, "refill", 1, cfg)
	}

	// Bucket should be empty now.
	allowed, err := limiter.Allow(ctx, "refill", 1, cfg)
	if err != nil {
		t.Fatalf("Allow() error: %v", err)
	}
	if allowed {
		t.Fatal("Allow() should be denied immediately after draining")
	}

	// Wait for tokens to refill.
	time.Sleep(50 * time.Millisecond)

	// Should be allowed again after refill.
	allowed, err = limiter.Allow(ctx, "refill", 1, cfg)
	if err != nil {
		t.Fatalf("Allow() error: %v", err)
	}
	if !allowed {
		t.Fatal("Allow() should be allowed after refill period")
	}
}
