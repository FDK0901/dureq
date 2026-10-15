// Package integration — regression tests for the 2026-10-16 audit fixes.
//
//   - R04: A failed dispatch must not consume the FiringID dedup mark.
//   - R06: A worker that fails to take the per-run lock must leave the stream
//     entry pending, and a running handler must keep its entry from being
//     reclaimed.
package integration

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// ============================================================
// R04: FiringID dedup is set only after a successful XADD
// ============================================================

func TestR04_FailedDispatch_DoesNotConsumeFiringID(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()
	st := cli.Store()

	streamKey := store.WorkStreamKey(st.Prefix(), "normal")
	firingID := "r04-job:1000"
	newMsg := func(runID string) *types.WorkMessage {
		return &types.WorkMessage{
			RunID:        runID,
			JobID:        "r04-job",
			TaskType:     "test-task",
			Payload:      json.RawMessage(`{}`),
			Deadline:     time.Now().Add(time.Hour),
			DispatchedAt: time.Now(),
			FiringID:     firingID,
			OperationKey: firingID,
		}
	}

	// Make the XADD fail: the stream key holds a string, so XADD returns WRONGTYPE.
	rdb := st.Client()
	if err := rdb.Do(ctx, rdb.B().Set().Key(streamKey).Value("not-a-stream").Build()).Error(); err != nil {
		t.Fatalf("SET stream key: %v", err)
	}
	if _, err := st.DispatchWork(ctx, "normal", newMsg("r04-run-1")); err == nil {
		t.Fatal("expected dispatch to fail while the stream key has the wrong type")
	}
	if err := rdb.Do(ctx, rdb.B().Del().Key(streamKey).Build()).Error(); err != nil {
		t.Fatalf("DEL stream key: %v", err)
	}

	// Retry of the same firing (scheduler retry gets a new RunID) must dispatch.
	msgID, err := st.DispatchWork(ctx, "normal", newMsg("r04-run-2"))
	if err != nil {
		t.Fatalf("retry dispatch: %v", err)
	}
	if msgID == "" {
		t.Fatal("retry dispatch was deduplicated: failed attempt consumed the FiringID")
	}

	// Once dispatched, the same firing is still deduplicated.
	msgID, err = st.DispatchWork(ctx, "normal", newMsg("r04-run-3"))
	if err != nil {
		t.Fatalf("duplicate dispatch: %v", err)
	}
	if msgID != "" {
		t.Fatalf("expected duplicate firing to be deduplicated, got msgID %q", msgID)
	}

	if entries := xrangeAll(t, st, streamKey); len(entries) != 1 {
		t.Fatalf("expected exactly 1 stream entry, got %d", len(entries))
	}
}

// ============================================================
// R06: reclaim must not ACK an entry that is still being executed
// ============================================================

func TestR06_RefreshMessageClaim_PreventsReclaim(t *testing.T) {
	flushDB(t)
	ctx := context.Background()

	cli := newClient(t)
	defer cli.Close()
	st := cli.Store()
	if err := st.EnsureStreams(ctx); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	if _, err := st.DispatchWork(ctx, "normal", &types.WorkMessage{
		RunID:        "r06-claim-run",
		JobID:        "r06-claim-job",
		TaskType:     "test-task",
		Payload:      json.RawMessage(`{}`),
		Deadline:     time.Now().Add(time.Hour),
		DispatchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	rdb := st.Client()
	streamKey := store.WorkStreamKey(st.Prefix(), "normal")
	xs, err := rdb.Do(ctx, rdb.B().Xreadgroup().Group(store.ConsumerGroup, "w1").Count(1).Streams().Key(streamKey).Id(">").Build()).AsXRead()
	if err != nil || len(xs[streamKey]) != 1 {
		t.Fatalf("XREADGROUP as w1: entries=%v err=%v", xs, err)
	}
	msgID := xs[streamKey][0].ID

	const minIdle = 250 * time.Millisecond

	// w1 is still running and refreshes its claim: w2 must not reclaim it.
	time.Sleep(2 * minIdle)
	if err := st.RefreshMessageClaim(ctx, "normal", "w1", msgID); err != nil {
		t.Fatalf("refresh claim: %v", err)
	}
	claimed, err := st.ClaimStaleMessages(ctx, "normal", "w2", minIdle, 10)
	if err != nil {
		t.Fatalf("claim stale: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("expected no reclaim right after refresh, got %d", len(claimed))
	}

	// w1 stops refreshing (crashed): w2 reclaims it.
	time.Sleep(2 * minIdle)
	claimed, err = st.ClaimStaleMessages(ctx, "normal", "w2", minIdle, 10)
	if err != nil {
		t.Fatalf("claim stale: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != msgID {
		t.Fatalf("expected %s to be reclaimed after refreshes stop, got %v", msgID, claimed)
	}
}

func TestR06_RunLockHeld_LeavesMessagePending(t *testing.T) {
	flushDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := newServer(t, "r06-node", 2)
	var executed atomic.Int32
	srv.RegisterHandler(types.HandlerDefinition{
		TaskType: "test.r06",
		Handler: func(ctx context.Context, payload json.RawMessage) error {
			executed.Add(1)
			return nil
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	cli := newClient(t)
	defer cli.Close()
	st := cli.Store()
	rdb := st.Client()

	// Another worker holds the run lock (its handler is still running).
	runID := "r06-locked-run"
	lockKey := store.LockKey(st.Prefix(), "run:"+runID)
	if err := rdb.Do(ctx, rdb.B().Set().Key(lockKey).Value("other-node:1").Ex(time.Minute).Build()).Error(); err != nil {
		t.Fatalf("SET lock: %v", err)
	}

	msgID, err := st.DispatchWork(ctx, "normal", &types.WorkMessage{
		RunID:        runID,
		JobID:        "r06-locked-job",
		TaskType:     "test.r06",
		Payload:      json.RawMessage(`{}`),
		Deadline:     time.Now().Add(time.Hour),
		DispatchedAt: time.Now(),
		OperationKey: "r06-locked-job",
	})
	if err != nil || msgID == "" {
		t.Fatalf("dispatch: msgID=%q err=%v", msgID, err)
	}

	// Wait until the worker has read the entry.
	streamKey := store.WorkStreamKey(st.Prefix(), "normal")
	waitFor(t, 10*time.Second, "worker reads the entry", func() bool {
		groups, err := rdb.Do(ctx, rdb.B().XinfoGroups().Key(streamKey).Build()).ToArray()
		if err != nil {
			return false
		}
		for _, g := range groups {
			m, err := g.AsStrMap()
			if err == nil && m["name"] == store.ConsumerGroup && m["last-delivered-id"] == msgID {
				return true
			}
		}
		return false
	})

	// Give the worker time to finish processing the entry, then check it was not ACK'd.
	time.Sleep(time.Second)
	pending, err := rdb.Do(ctx, rdb.B().Xpending().Key(streamKey).Group(store.ConsumerGroup).Start("-").End("+").Count(10).Build()).ToArray()
	if err != nil {
		t.Fatalf("XPENDING: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected the entry to stay pending while the lock is held, got %d pending", len(pending))
	}
	if n := executed.Load(); n != 0 {
		t.Fatalf("handler must not run while another worker holds the run lock, ran %d times", n)
	}
}
