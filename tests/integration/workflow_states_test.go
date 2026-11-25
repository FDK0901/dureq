// Package integration — suspended workflows, child workflows that end in other ways than by
// completing or failing, and deadlines of retries.
package integration

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

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
