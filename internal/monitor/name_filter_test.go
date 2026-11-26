package monitor

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
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
