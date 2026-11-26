package monitor

import (
	"context"
	"fmt"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

type listPage[T any] struct {
	Data  []T `json:"data"`
	Total int `json:"total"`
}

// A name is looked for among all, not among those of one page, and the total is how many match.
func TestNameFilter(t *testing.T) {
	st, srv := newTestAPI(t, nil)
	ctx := context.Background()
	now := time.Now()
	for i := range 130 {
		name := "other"
		if i < 5 { // the oldest
			name = "wanted"
		}
		at := now.Add(time.Duration(i) * time.Second)
		id := fmt.Sprintf("%03d", i)
		if _, err := st.SaveWorkflow(ctx, &types.WorkflowInstance{ID: id, WorkflowName: name, Status: types.WorkflowStatusRunning, CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.SaveBatch(ctx, &types.BatchInstance{ID: id, Name: name, Status: types.WorkflowStatusRunning, CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"workflows", "batches"} {
		for _, tc := range []struct {
			query string
			want  []string
		}{
			{"limit=3", []string{"004", "003", "002"}},
			{"limit=3&offset=3", []string{"001", "000"}},
			{"limit=2&sort=oldest", []string{"000", "001"}},
			{"limit=2&status=running", []string{"004", "003"}},
		} {
			var page listPage[struct {
				ID string `json:"id"`
			}]
			get(t, srv, "/api/"+kind+"?name=wanted&"+tc.query, &page)
			var ids []string
			for _, v := range page.Data {
				ids = append(ids, v.ID)
			}
			if !slices.Equal(ids, tc.want) || page.Total != 5 {
				t.Errorf("%s?%s: %v of %d, want %v of 5", kind, tc.query, ids, page.Total, tc.want)
			}
		}
		var page listPage[any]
		if get(t, srv, "/api/"+kind+"?name=none", &page); page.Total != 0 || len(page.Data) != 0 {
			t.Errorf("%s: %d of %d have a name that none has", kind, len(page.Data), page.Total)
		}
	}
}

// Past what a name filter looks at, what matches is not found: the total says that it is a lower
// bound. With total=false, a page says only whether there are more.
func TestNameFilter_SaysWhatItHasNotLookedAt(t *testing.T) {
	addr, password := testRedis(t)
	rdb, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, Password: password})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rdb.Close)
	st, err := store.NewRedisStore(rdb, store.RedisStoreConfig{
		KeyPrefix:   "test_" + xid.New().String(),
		Tiers:       []store.TierConfig{{Name: "normal", Weight: 1, FetchBatch: 10}},
		MaxNameScan: 50,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	api := NewAPI(st, nil, nil)
	t.Cleanup(api.Shutdown)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)

	ctx := context.Background()
	now := time.Now()
	for i := range 120 {
		name := "other"
		if i%10 == 0 { // 12 of them, 5 among the 50 newest
			name = "wanted"
		}
		at := now.Add(time.Duration(i) * time.Second)
		id := fmt.Sprintf("%03d", i)
		if _, err := st.SaveWorkflow(ctx, &types.WorkflowInstance{ID: id, WorkflowName: name, Status: types.WorkflowStatusRunning, CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.SaveBatch(ctx, &types.BatchInstance{ID: id, Name: name, Status: types.WorkflowStatusRunning, CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	type page struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
		Total      *int  `json:"total"`
		LowerBound bool  `json:"total_is_lower_bound"`
		HasMore    *bool `json:"has_more"`
	}
	for _, kind := range []string{"workflows", "batches"} {
		for _, tc := range []struct {
			query      string
			count      int
			total      int // -1: none
			lowerBound bool
			more       int // -1: none, 0: false, 1: true
		}{
			{"name=wanted&limit=3", 3, 5, true, -1},
			{"name=other&limit=3", 3, 45, true, -1},
			{"name=wanted&limit=3&sort=oldest", 3, 5, true, -1},
			{"limit=3", 3, 120, false, -1},
			{"name=wanted&limit=3&total=false", 3, -1, false, 1},
			{"name=wanted&limit=5&total=false", 5, -1, false, 1},  // not all were looked at
			{"name=wanted&limit=10&total=false", 5, -1, false, 1}, // the same
			{"limit=100&offset=20&total=false", 100, -1, false, 0},
			{"limit=100&offset=19&total=false", 100, -1, false, 1},
		} {
			var p page
			get(t, srv, "/api/"+kind+"?"+tc.query, &p)
			total := -1
			if p.Total != nil {
				total = *p.Total
			}
			more := -1
			if p.HasMore != nil {
				more = map[bool]int{false: 0, true: 1}[*p.HasMore]
			}
			if len(p.Data) != tc.count || total != tc.total || p.LowerBound != tc.lowerBound || more != tc.more {
				t.Errorf("%s?%s: %d, total %d (lower bound %v), more %d; want %d, %d (%v), %d", kind, tc.query, len(p.Data), total, p.LowerBound, more, tc.count, tc.total, tc.lowerBound, tc.more)
			}
		}
	}
}
