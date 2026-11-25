package archival

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// newTestStore connects to the standalone Redis at REDIS_URL under a unique key prefix.
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
	st, err := store.NewRedisStore(rdb, store.RedisStoreConfig{KeyPrefix: "test_" + xid.New().String()}, nil)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	return st
}

// Archiving an old job must not free a unique key that another job holds by
// now.
func TestCleanup_UniqueKeyHeldByAnotherJob_IsNotReleased(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	key := "k"
	old := &types.Job{
		ID:        xid.New().String(),
		TaskType:  "test.archival",
		Payload:   json.RawMessage(`{}`),
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		Status:    types.JobStatusDead,
		UniqueKey: &key,
		UpdatedAt: time.Now().Add(-48 * time.Hour),
	}
	if _, err := st.CreateJob(ctx, old); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := st.SetUniqueKey(ctx, key, "current-job"); err != nil {
		t.Fatalf("set unique key: %v", err)
	}

	New(Config{Store: st, RetentionPeriod: 24 * time.Hour}).cleanup(ctx)

	if _, _, err := st.GetJob(ctx, old.ID); err != types.ErrJobNotFound {
		t.Fatalf("old job err = %v, want not found", err)
	}
	if holder, _, _ := st.CheckUniqueKey(ctx, key); holder != "current-job" {
		t.Fatalf("key holder = %q, want current-job", holder)
	}
}
