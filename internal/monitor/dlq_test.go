package monitor

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/redis/rueidis"
)

// countingRedis counts the entries that were asked for with XREVRANGE.
type countingRedis struct {
	rueidis.Client
	asked int
}

func (c *countingRedis) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	if a := cmd.Commands(); a[0] == "XREVRANGE" {
		n, _ := strconv.Atoi(a[len(a)-1])
		c.asked += n
	}
	return c.Client.Do(ctx, cmd)
}

// All of the dead letter queue can be seen, and a page does not read all of it.
func TestDLQ_Pages(t *testing.T) {
	base, srv := newTestAPI(t, nil)
	ctx := context.Background()
	const n = 2300
	cmds := make(rueidis.Commands, 0, n)
	for i := range n {
		cmds = append(cmds, base.Client().B().Xadd().Key(store.DLQStreamKey(base.Prefix())).Id("*").FieldValue().FieldValue("job_id", fmt.Sprint(i)).Build())
	}
	base.Client().DoMulti(ctx, cmds...)
	for _, tc := range []struct{ limit, offset, first, count int }{
		{10, 0, 2299, 10}, {10, 1500, 799, 10}, {100, 2250, 49, 50}, {10, 2300, 0, 0}, {10, 5000, 0, 0},
	} {
		var page listPage[types.WorkMessage]
		get(t, srv, fmt.Sprintf("/api/dlq?limit=%d&offset=%d", tc.limit, tc.offset), &page)
		if page.Total != n || len(page.Data) != tc.count {
			t.Errorf("limit %d, offset %d: %d of %d, want %d of %d", tc.limit, tc.offset, len(page.Data), page.Total, tc.count, n)
			continue
		}
		for i, m := range page.Data {
			if m.JobID != fmt.Sprint(tc.first-i) {
				t.Errorf("limit %d, offset %d: message %d is %s", tc.limit, tc.offset, i, m.JobID)
				break
			}
		}
	}
	api := NewAPIService(base, nil, chainedslog.NewSlog(chainedslog.NewSlogBase()))
	defer api.Shutdown()
	if msgs, total, err := api.ListDLQ(ctx, 0, 0); err != nil || len(msgs) != maxDLQMessages || total != n {
		t.Errorf("without a limit: %d of %d (%v)", len(msgs), total, err)
	}
	counting := &countingRedis{Client: base.Client()}
	st, err := store.NewRedisStore(counting, base.Config(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ListDLQPage(ctx, 1, 0); err != nil || counting.asked != 1 {
		t.Errorf("a page of one message reads %d (%v)", counting.asked, err)
	}
}
