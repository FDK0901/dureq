package client

import (
	"context"
	"encoding/json"
	"errors"
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
}
