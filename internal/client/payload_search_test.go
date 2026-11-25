package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// newSearchClient returns a test client, or skips if the server has no RedisJSON.
func newSearchClient(t *testing.T) *Client {
	t.Helper()
	cli := newTestClient(t)
	ctx := context.Background()
	key := cli.Store().Prefix() + ":probe"
	if err := cli.rdb.Do(ctx, cli.rdb.B().JsonSet().Key(key).Path("$").Value("{}").Build()).Error(); err != nil {
		t.Skipf("no RedisJSON: %v", err)
	}
	cli.rdb.Do(ctx, cli.rdb.B().Del().Key(key).Build())
	return cli
}

// storeJob stores a job created age ago.
func storeJob(t *testing.T, cli *Client, id string, taskType types.TaskType, payload string, status types.JobStatus, age time.Duration) {
	t.Helper()
	if _, err := cli.Store().CreateJob(context.Background(), &types.Job{
		ID:        id,
		TaskType:  taskType,
		Payload:   json.RawMessage(payload),
		Status:    status,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		CreatedAt: time.Now().Add(-age),
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
}

func search(t *testing.T, cli *Client, path string, value any, q store.PayloadQuery) []string {
	t.Helper()
	jobs, err := cli.GetJobsByParamJSONPath(context.Background(), path, value, q)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	ids := []string{}
	for _, job := range jobs {
		ids = append(ids, job.ID)
	}
	return ids
}

func TestGetJobsByParamJSONPath_Selects(t *testing.T) {
	cli := newSearchClient(t)
	const event = `{"eventId":"e1"}`
	for i, status := range []types.JobStatus{
		types.JobStatusCancelled, types.JobStatusScheduled, types.JobStatusDead, types.JobStatusFailed,
		types.JobStatusCompleted, types.JobStatusRunning, types.JobStatusPaused, types.JobStatusRetrying, types.JobStatusPending,
	} {
		storeJob(t, cli, string(status), "update", event, status, time.Duration(i)*time.Minute)
	}
	storeJob(t, cli, "other-type", "remind", event, types.JobStatusScheduled, time.Hour)
	storeJob(t, cli, "other-event", "update", `{"eventId":"e2"}`, types.JobStatusScheduled, 0)
	storeJob(t, cli, "no-event", "update", `{}`, types.JobStatusScheduled, 0)

	for _, tc := range []struct {
		name string
		q    store.PayloadQuery
		want []string // newest first
	}{
		{"everything", store.PayloadQuery{}, []string{"cancelled", "scheduled", "dead", "failed", "completed", "running", "paused", "retrying", "pending", "other-type"}},
		{"of a task type", store.PayloadQuery{TaskType: "remind"}, []string{"other-type"}},
		{"live", store.PayloadQuery{LiveOnly: true, TaskType: "update"}, []string{"scheduled", "failed", "running", "paused", "retrying", "pending"}},
		{"statuses", store.PayloadQuery{Statuses: []types.JobStatus{types.JobStatusDead, types.JobStatusPaused}}, []string{"dead", "paused"}},
		{"live and statuses", store.PayloadQuery{LiveOnly: true, Statuses: []types.JobStatus{types.JobStatusDead, types.JobStatusPaused}}, []string{"paused"}},
		{"limit counts selected jobs", store.PayloadQuery{LiveOnly: true, Limit: 2}, []string{"scheduled", "failed"}},
		{"nothing", store.PayloadQuery{TaskType: "unknown"}, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := search(t, cli, "eventId", "e1", tc.q); !slices.Equal(got, tc.want) {
				t.Fatalf("found %v, want %v", got, tc.want)
			}
		})
	}

	// The single search is unchanged: the newest, whatever its status.
	job, err := cli.GetJobByParamJSONPathWithTaskType(context.Background(), "eventId", "e1", "update")
	if err != nil || job.ID != "cancelled" {
		t.Fatalf("single search found %v, %v; want the cancelled job", job, err)
	}
}

// Every matching job is found once, over many pages, also if they share a timestamp.
func TestGetJobsByParamJSONPath_ManyJobs(t *testing.T) {
	for _, sameTime := range []bool{false, true} {
		t.Run(fmt.Sprintf("same timestamp %v", sameTime), func(t *testing.T) {
			cli := newSearchClient(t)
			ctx := context.Background()
			now := time.Now()
			want := 0
			for i := range 350 {
				job := &types.Job{
					ID: fmt.Sprintf("job-%03d", i), TaskType: "update", Status: types.JobStatusScheduled,
					Payload: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i%7)), CreatedAt: now,
				}
				if !sameTime {
					job.CreatedAt = now.Add(time.Duration(i) * time.Millisecond)
				}
				if i%7 == 3 {
					want++
				}
				if _, err := cli.Store().CreateJob(ctx, job); err != nil {
					t.Fatalf("create job: %v", err)
				}
			}
			got := search(t, cli, "n", 3, store.PayloadQuery{TaskType: "update"})
			unique := slices.Compact(slices.Sorted(slices.Values(got)))
			if len(got) != want || len(unique) != want {
				t.Fatalf("found %d jobs, %d of them different; want %d", len(got), len(unique), want)
			}
		})
	}
}

// Numbers are found as they are written, however large.
func TestGetJobByParamJSONPath_Numbers(t *testing.T) {
	cli := newSearchClient(t)
	ctx := context.Background()
	for _, literal := range []string{"42", "1234567", "1000000", "9007199254740993", "-5000000", "1.5"} {
		storeJob(t, cli, literal, "update", `{"id":`+literal+`}`, types.JobStatusScheduled, 0)
	}
	for _, tc := range []struct {
		value any
		want  string
	}{
		{42, "42"}, {"42", "42"}, {1234567, "1234567"}, {int64(9007199254740993), "9007199254740993"},
		{-5000000, "-5000000"}, {1.5, "1.5"},
		{float64(1234567), "1234567"}, // printed as 1.234567e+06, which has always matched
		{"1e+06", "1000000"},          // likewise
	} {
		job, err := cli.GetJobByParamJSONPath(ctx, "id", tc.value)
		if err != nil || job.ID != tc.want {
			t.Fatalf("%T %v: found %v, %v; want job %s", tc.value, tc.value, job, err, tc.want)
		}
	}
	if _, err := cli.GetJobByParamJSONPath(ctx, "id", 9007199254740992); !errors.Is(err, types.ErrJobNotFound) {
		t.Fatalf("a neighbouring number was found: %v", err)
	}
}

// A job that is gone but still indexed does not hide the next match.
func TestPayloadSearch_SkipsVanishedJob(t *testing.T) {
	cli := newSearchClient(t)
	ctx := context.Background()
	storeJob(t, cli, "kept", "update", `{"eventId":"e1"}`, types.JobStatusScheduled, time.Minute)
	storeJob(t, cli, "vanished", "update", `{"eventId":"e1"}`, types.JobStatusScheduled, 0)
	if err := cli.rdb.Do(ctx, cli.rdb.B().Del().Key(store.JobKey(cli.Store().Prefix(), "vanished")).Build()).Error(); err != nil {
		t.Fatal(err)
	}

	job, err := cli.GetJobByParamJSONPath(ctx, "eventId", "e1")
	if err != nil || job.ID != "kept" {
		t.Fatalf("found %v, %v; want the job that is left", job, err)
	}
	if got := search(t, cli, "eventId", "e1", store.PayloadQuery{}); !slices.Equal(got, []string{"kept"}) {
		t.Fatalf("found %v", got)
	}
}

// A search that fails says so instead of finding nothing: the caller might create the
// job a second time.
func TestPayloadSearch_ReportsErrors(t *testing.T) {
	cli := newSearchClient(t)
	storeJob(t, cli, "j", "update", `{"eventId":"e1"}`, types.JobStatusScheduled, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := cli.GetJobByParamJSONPath(ctx, "eventId", "e1"); err == nil || errors.Is(err, types.ErrJobNotFound) {
		t.Fatalf("single search err = %v, want the failure", err)
	}
	if _, err := cli.GetJobsByParamJSONPath(ctx, "eventId", "e1", store.PayloadQuery{}); err == nil {
		t.Fatal("no error")
	}
}
