// Package integration — how workflows and batches end, and what a retry goes on with.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/pkg/types"
)

func tallyTask(name string, deps ...string) types.WorkflowTask {
	return types.WorkflowTask{Name: name, TaskType: "tally.wf", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, name)), DependsOn: deps}
}

func waitWorkflow(t *testing.T, cli *client.Client, id string, want types.WorkflowStatus) *types.WorkflowInstance {
	t.Helper()
	var wf *types.WorkflowInstance
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got, _, err := cli.Store().GetWorkflow(context.Background(), id)
		if err == nil {
			wf = got
			if got.Status == want {
				return got
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	for name, st := range wf.Tasks {
		t.Logf("  task %s: %s skipped=%v child=%q job=%q", name, st.Status, st.Skipped, st.ChildWorkflowID, st.JobID)
	}
	t.Fatalf("workflow is %s, want %s", wf.Status, want)
	return nil
}

// A root task that is a workflow of its own is started.
func TestWorkflow_ChildWorkflowAtRoot(t *testing.T) {
	for name, extra := range map[string][]types.WorkflowTask{"only root": nil, "with a plain root": {tallyTask("plain")}} {
		t.Run(name, func(t *testing.T) {
			var c tally
			cli := startTally(t, "tally.wf", &c, nil)
			child := types.WorkflowTask{Name: "child", ChildWorkflowDef: &types.WorkflowDefinition{Name: "inner", Tasks: []types.WorkflowTask{tallyTask("c1"), tallyTask("c2", "c1")}}}
			wf, err := cli.EnqueueWorkflow(context.Background(), types.WorkflowDefinition{Name: "outer", Tasks: append([]types.WorkflowTask{child, tallyTask("after", "child")}, extra...)}, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
			ids := []string{"c1", "c2", "after"}
			for _, e := range extra {
				ids = append(ids, e.Name)
			}
			if off := c.not(1, ids); len(off) > 0 {
				t.Errorf("did not run once: %v", off)
			}
		})
	}
}
