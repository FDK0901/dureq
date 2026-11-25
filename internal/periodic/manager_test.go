package periodic

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

type configs []*types.PeriodicTaskConfig

func (c *configs) GetConfigs() ([]*types.PeriodicTaskConfig, error) { return *c, nil }

// An updated task takes the place of the one before instead of running next to it.
func TestSync_UpdatedTask_ReplacesJob(t *testing.T) {
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
	defer rdb.Close()
	cli, err := client.New(client.WithRedisClient(rdb), client.WithKeyPrefix("test_"+xid.New().String()))
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	ctx := context.Background()

	provider := &configs{{TaskType: "report", CronExpr: "0 0 * * *", UniqueKey: "report"}}
	m := NewManager(ManagerConfig{Provider: provider, Client: cli})
	m.sync(ctx)
	first, _, _ := cli.Store().CheckUniqueKey(ctx, "report")

	*provider = configs{{TaskType: "report", CronExpr: "0 1 * * *", UniqueKey: "report", Payload: json.RawMessage(`{"v":2}`)}}
	m.sync(ctx)
	second, _, _ := cli.Store().CheckUniqueKey(ctx, "report")
	if first == "" || second == "" || first == second {
		t.Fatalf("key held by %q, then by %q", first, second)
	}

	jobs, _, err := cli.Store().ListJobsPaginated(ctx, store.JobFilter{Limit: 100})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	for _, job := range jobs {
		if live := !job.Status.IsTerminal(); live != (job.ID == second) {
			t.Fatalf("job %s is %s; only %s should be live", job.ID, job.Status, second)
		}
	}
	if _, _, err := cli.Store().GetSchedule(ctx, first); err != types.ErrScheduleNotFound {
		t.Fatalf("the replaced task is still scheduled: %v", err)
	}
}
