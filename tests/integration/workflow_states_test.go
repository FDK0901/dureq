// Package integration — suspended workflows, child workflows that end in other ways than by
// completing or failing, and deadlines of retries.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/monitor"
	"github.com/FDK0901/dureq/pkg/types"
)

func newAPI(t *testing.T, cli *client.Client) *monitor.APIService {
	t.Helper()
	api := monitor.NewAPIService(cli.Store(), dispatcher.New(cli.Store(), nil), nil)
	t.Cleanup(api.Shutdown)
	return api
}

func tallyHook(id string) *types.WorkflowHookDef {
	return &types.WorkflowHookDef{TaskType: "tally.wf", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))}
}

// kindOf returns the last part of an ID. The parts before it tell the cases apart that run
// side by side.
func kindOf(id string) string { return id[strings.LastIndex(id, "-")+1:] }

func childTask(name string, inner types.WorkflowDefinition, deps ...string) types.WorkflowTask {
	inner.Name = "inner"
	return types.WorkflowTask{Name: name, DependsOn: deps, ChildWorkflowDef: &inner}
}

// A suspended workflow stays so when what is under way ends, and goes on once it is resumed.
func TestWorkflow_StaysSuspended(t *testing.T) {
	var c tally
	cli := startTally(t, "tally.wf", &c, func(id string) error {
		first := len(c.not(1, []string{id})) == 0
		switch kindOf(id) {
		case "next":
			return nil
		case "bad":
			time.Sleep(1500 * time.Millisecond)
			return &types.NonRetryableError{Err: errBoom}
		case "flaky":
			time.Sleep(1500 * time.Millisecond)
			if first {
				return &types.NonRetryableError{Err: errBoom}
			}
		case "slow":
			time.Sleep(1500 * time.Millisecond)
		}
		return nil
	})
	api := newAPI(t, cli)
	ctx := context.Background()
	for name, def := range map[string]types.WorkflowDefinition{
		"completes": {Tasks: []types.WorkflowTask{tallyTask("completes-slow"), tallyTask("completes-next", "completes-slow")}},
		"allowed": {Tasks: []types.WorkflowTask{
			func() types.WorkflowTask { task := tallyTask("allowed-bad"); task.AllowFailure = true; return task }(),
			tallyTask("allowed-next", "allowed-bad")}},
		"child": {Tasks: []types.WorkflowTask{
			childTask("child", types.WorkflowDefinition{Tasks: []types.WorkflowTask{tallyTask("child-slow")}}), tallyTask("child-next", "child")}},
		"retried": {RetryPolicy: &types.RetryPolicy{MaxAttempts: 2}, Tasks: []types.WorkflowTask{tallyTask("retried-flaky"), tallyTask("retried-next", "retried-flaky")}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			def.Name = "w"
			wf, err := cli.EnqueueWorkflow(ctx, def, nil)
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(700 * time.Millisecond)
			if err := api.SuspendWorkflow(ctx, wf.ID); err != nil {
				t.Fatalf("suspend: %v", err)
			}
			time.Sleep(3500 * time.Millisecond)
			next := []string{name + "-next"}
			if off := c.not(0, next); len(off) > 0 {
				t.Errorf("ran while suspended: %v", off)
			}
			if off := c.not(1, []string{"retried-flaky"}); name == "retried" && len(off) > 0 {
				t.Errorf("retried while suspended: %v", off)
			}
			got, _, err := cli.Store().GetWorkflow(ctx, wf.ID)
			if err != nil || got.Status != types.WorkflowStatusSuspended {
				t.Fatalf("workflow is %s (%v), want suspended", got.Status, err)
			}
			if err := api.ResumeWorkflow(ctx, wf.ID); err != nil {
				t.Fatalf("resume: %v", err)
			}
			waitWorkflow(t, cli, wf.ID, types.WorkflowStatusCompleted)
			if off := c.not(1, next); len(off) > 0 {
				t.Errorf("did not run once: %v", off)
			}
		})
	}
}
