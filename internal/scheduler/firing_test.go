package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// newTestStore connects to the standalone Redis at REDIS_URL (e.g.
// redis://localhost:6381, password REDIS_PASSWORD) under a unique key prefix.
// Skips if REDIS_URL is unset or Redis is unreachable.
func newTestStore(t *testing.T) *store.RedisStore {
	t.Helper()

	addr := strings.TrimPrefix(os.Getenv("REDIS_URL"), "redis://")
	if addr == "" {
		t.Skip("REDIS_URL not set")
	}
	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		password = "your-password"
	}
	rdb, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, Password: password})
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(rdb.Close)

	st, err := store.NewRedisStore(rdb, store.RedisStoreConfig{
		KeyPrefix: "test_" + xid.New().String(),
		Tiers:     []store.TierConfig{{Name: "normal", Weight: 1, FetchBatch: 10}},
	}, nil)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	if err := st.EnsureStreams(context.Background()); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}
	return st
}

// createScheduledJob stores a DURATION job whose firing is due at scheduledAt.
func createScheduledJob(t *testing.T, st *store.RedisStore, headers map[string]string, scheduledAt time.Time) *types.ScheduleEntry {
	t.Helper()
	ctx := context.Background()

	interval := types.Duration(time.Minute)
	job := &types.Job{
		ID:       xid.New().String(),
		TaskType: "test.firing",
		Payload:  json.RawMessage(`{}`),
		Schedule: types.Schedule{Type: types.ScheduleDuration, Interval: &interval},
		Status:   types.JobStatusScheduled,
		Headers:  headers,
	}
	if _, err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	entry := &types.ScheduleEntry{JobID: job.ID, NextRunAt: scheduledAt, Schedule: job.Schedule}
	if _, err := st.SaveSchedule(ctx, entry); err != nil {
		t.Fatalf("save schedule: %v", err)
	}
	return entry
}

func workEntries(t *testing.T, st *store.RedisStore) []rueidis.XRangeEntry {
	t.Helper()
	rdb := st.Client()
	entries, err := rdb.Do(context.Background(), rdb.B().Xrange().Key(store.WorkStreamKey(st.Prefix(), "normal")).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatalf("XRANGE: %v", err)
	}
	return entries
}

// A leader that takes over before the previous leader advanced the schedule
// processes the same due entry on a later tick. Both must derive the same
// FiringID, so the firing is dispatched exactly once.
func TestProcessScheduleEntry_SameFiringOnLaterTick_DispatchedOnce(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})

	scheduledAt := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	entry := createScheduledJob(t, st, nil, scheduledAt)
	staleEntry := *entry // what the next leader loads before the advance is visible

	firstTick := scheduledAt.Add(500 * time.Millisecond)
	s.processScheduleEntry(ctx, entry, firstTick)
	s.processScheduleEntry(ctx, &staleEntry, scheduledAt.Add(2500*time.Millisecond))

	entries := workEntries(t, st)
	if len(entries) != 1 {
		t.Fatalf("expected the firing to be dispatched once, got %d stream entries", len(entries))
	}
	wantFiringID := fmt.Sprintf("%s:%d", entry.JobID, scheduledAt.UnixMilli())
	if got := entries[0].FieldValues["firing_id"]; got != wantFiringID {
		t.Fatalf("firing_id = %q, want %q (scheduled time, not tick time)", got, wantFiringID)
	}

	// The skipped duplicate must leave the job as the first dispatch left it.
	job, _, err := st.GetJob(ctx, entry.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != types.JobStatusPending {
		t.Fatalf("job status = %s, want %s", job.Status, types.JobStatusPending)
	}
	if job.LastRunAt == nil || !job.LastRunAt.Equal(firstTick) {
		t.Fatalf("job LastRunAt = %v, want %v (first dispatch)", job.LastRunAt, firstTick)
	}
}

// Firing headers must not be persisted on the job: a stale FiringID would be
// carried into non-firing dispatches (retry, resume, orphan redispatch) and
// deduplicated away.
func TestProcessScheduleEntry_DoesNotPersistFiringHeaders(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})

	scheduledAt := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	// A job stored by an older version still carries the previous firing's headers.
	entry := createScheduledJob(t, st, map[string]string{
		"x-dureq-firing-id": "stale:1",
		"x-dureq-backfill":  "true",
		"user-header":       "kept",
	}, scheduledAt)

	s.processScheduleEntry(ctx, entry, scheduledAt.Add(500*time.Millisecond))

	entries := workEntries(t, st)
	if len(entries) != 1 {
		t.Fatalf("expected 1 stream entry, got %d", len(entries))
	}
	if got, want := entries[0].FieldValues["firing_id"], fmt.Sprintf("%s:%d", entry.JobID, scheduledAt.UnixMilli()); got != want {
		t.Fatalf("firing_id = %q, want %q", got, want)
	}
	var msgHeaders map[string]string
	if err := json.Unmarshal([]byte(entries[0].FieldValues["headers"]), &msgHeaders); err != nil {
		t.Fatalf("decode message headers: %v", err)
	}
	if msgHeaders["x-dureq-backfill"] != "" || msgHeaders["user-header"] != "kept" {
		t.Fatalf("unexpected message headers: %v", msgHeaders)
	}

	job, _, err := st.GetJob(ctx, entry.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	for _, h := range []string{"x-dureq-firing-id", "x-dureq-backfill", "x-dureq-scheduled-at"} {
		if v, ok := job.Headers[h]; ok {
			t.Fatalf("stored job still has %s=%q", h, v)
		}
	}
	if job.Headers["user-header"] != "kept" {
		t.Fatalf("user header lost: %v", job.Headers)
	}
}
