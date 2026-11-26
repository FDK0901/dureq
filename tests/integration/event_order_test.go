// Package integration — events come after what they stand for.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
)

// Whoever reads a job, or its runs, on an event finds them as they are by then, and the event
// says the same.
func TestEvents_ComeAfterTheirEffects(t *testing.T) {
	const n = 100
	_, cli := startTiered(t, 20, nil,
		types.HandlerDefinition{TaskType: "works", Handler: func(context.Context, json.RawMessage) error { return nil }},
		types.HandlerDefinition{TaskType: "fails", Handler: func(context.Context, json.RawMessage) error { return errors.New("no") }},
		types.HandlerDefinition{TaskType: "pauses", Handler: func(context.Context, json.RawMessage) error { return errors.New("no") }},
		types.HandlerDefinition{TaskType: "asks to be paused", Handler: func(context.Context, json.RawMessage) error { return &types.PauseError{Reason: "no"} }})
	st := cli.Store()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	events := make(chan types.JobEvent, 10*n)
	var seen atomic.Int64
	listen := func() error {
		return st.Client().Receive(ctx, st.Client().B().Subscribe().Channel(store.EventsPubSubChannel(st.Prefix())).Build(), func(msg rueidis.PubSubMessage) {
			var e types.JobEvent
			if json.Unmarshal([]byte(msg.Message), &e) == nil {
				events <- e
			}
		})
	}
	go listen()
	time.Sleep(500 * time.Millisecond)

	check := func(e types.JobEvent) error {
		job, _, err := st.GetJob(ctx, e.JobID)
		if err != nil {
			return err
		}
		if e.Status != job.Status || e.RunCount != job.RunCount || job.RunCount == 0 {
			return fmt.Errorf("the event says %s after %d runs, the job %s after %d", e.Status, e.RunCount, job.Status, job.RunCount)
		}
		runs, _, err := st.ListJobRuns(ctx, store.RunFilter{JobID: &e.JobID, Limit: 5})
		if err != nil {
			return err
		}
		if len(runs) != 1 || runs[0].ID != e.RunID {
			return fmt.Errorf("%d runs are in the history", len(runs))
		}
		if active, err := st.HasActiveRunForJob(ctx, e.JobID); err != nil || active {
			return fmt.Errorf("the run still counts as active (%v)", err)
		}
		want := map[types.EventType]types.JobStatus{types.EventJobCompleted: types.JobStatusCompleted, types.EventJobFailed: types.JobStatusDead, types.EventJobPaused: types.JobStatusPaused}[e.Type]
		if job.Status != want {
			return fmt.Errorf("the job is %s", job.Status)
		}
		return nil
	}
	errs := make(chan error, 10*n)
	for range 10 {
		go func() {
			for e := range events {
				switch e.Type {
				case types.EventJobCompleted, types.EventJobFailed, types.EventJobPaused:
					if err := check(e); err != nil {
						errs <- fmt.Errorf("%s of %s: %w", e.Type, e.TaskType, err)
					}
					seen.Add(1)
				}
			}
		}()
	}

	for range n {
		for taskType, policy := range map[types.TaskType]*types.RetryPolicy{
			"works":             nil,
			"asks to be paused": nil,
			"fails":             {MaxAttempts: 1, InitialDelay: time.Second, MaxDelay: time.Second, Multiplier: 1},
			"pauses":            {MaxAttempts: 5, InitialDelay: time.Second, MaxDelay: time.Second, Multiplier: 1, PauseAfterErrCount: 1},
		} {
			if _, err := cli.EnqueueScheduled(ctx, &client.EnqueueRequest{TaskType: taskType, Payload: json.RawMessage(`{}`), RetryPolicy: policy, Schedule: types.Schedule{Type: types.ScheduleImmediate}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	waitFor(t, 20*time.Second, "events", func() bool { return seen.Load() >= 4*n })
	close(errs)
	count := make(map[string]int)
	for err := range errs {
		count[err.Error()]++
	}
	for msg, n := range count {
		t.Errorf("%d x %s", n, msg)
	}
}
