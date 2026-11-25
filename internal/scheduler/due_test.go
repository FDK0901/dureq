package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/pkg/types"
)

// slowDispatcher takes a while for each firing.
type slowDispatcher struct {
	*dispatcher.Dispatcher
	takes time.Duration
}

func (d slowDispatcher) DispatchFiring(ctx context.Context, job *types.Job, attempt int) (bool, error) {
	time.Sleep(d.takes)
	return d.Dispatcher.DispatchFiring(ctx, job, attempt)
}

// Schedules that are due at once do not wait for each other, and each fires once.
func TestTick_ManyDue(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	const n, takes = 200, 10 * time.Millisecond
	s := New(Config{Store: st, Dispatcher: slowDispatcher{dispatcher.New(st, nil), takes}})
	ids := make(map[string]bool)
	for range n {
		ids[createScheduledJob(t, st, nil, time.Now().Add(-time.Second)).JobID] = true
	}

	start := time.Now()
	s.tick(ctx)
	if took := time.Since(start); took > n*takes/2 {
		t.Errorf("%d firings of %v took %v", n, takes, took)
	}
	s.tick(ctx) // none is due any more
	for _, e := range workEntries(t, st) {
		id := e.FieldValues["job_id"]
		if !ids[id] {
			t.Errorf("job %s was dispatched again, or is unknown", id)
		}
		delete(ids, id)
	}
	if len(ids) > 0 {
		t.Errorf("%d jobs were not dispatched", len(ids))
	}
	due, err := st.ListDueSchedules(ctx, time.Now())
	if err != nil || len(due) > 0 {
		t.Errorf("%d schedules are still due (%v)", len(due), err)
	}
}

// A leader that is stopped leaves the rest to the next one.
func TestProcessDue_Stopped(t *testing.T) {
	st := newTestStore(t)
	s := New(Config{Store: st, Dispatcher: dispatcher.New(st, nil)})
	var entries []*types.ScheduleEntry
	for range 5 {
		entries = append(entries, createScheduledJob(t, st, nil, time.Now().Add(-time.Second)))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.processDue(ctx, entries, time.Now())
	if n := len(workEntries(t, st)); n > 0 {
		t.Errorf("%d firings after the leader was stopped", n)
	}
	s.processDue(context.Background(), nil, time.Now())
}
