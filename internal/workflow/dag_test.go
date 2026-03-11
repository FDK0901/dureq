package workflow

import (
	"encoding/json"
	"testing"

	"github.com/FDK0901/dureq/pkg/types"
)

func TestValidateDAG_SimpleLinearChain(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "linear",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a"},
			{Name: "b", TaskType: "handler_b", DependsOn: []string{"a"}},
			{Name: "c", TaskType: "handler_c", DependsOn: []string{"b"}},
		},
	}
	if err := ValidateDAG(def); err != nil {
		t.Fatalf("expected valid DAG, got error: %v", err)
	}
}

func TestValidateDAG_DuplicateTaskName(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "dup",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a"},
			{Name: "a", TaskType: "handler_b"},
		},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for duplicate task names, got nil")
	}
}

func TestValidateDAG_CyclicDependency(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "cycle",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a", DependsOn: []string{"c"}},
			{Name: "b", TaskType: "handler_b", DependsOn: []string{"a"}},
			{Name: "c", TaskType: "handler_c", DependsOn: []string{"b"}},
		},
	}
	err := ValidateDAG(def)
	if err == nil {
		t.Fatal("expected error for cyclic dependency, got nil")
	}
}

func TestValidateDAG_UnknownDependency(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "unknown_dep",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a", DependsOn: []string{"nonexistent"}},
		},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for unknown dependency, got nil")
	}
}

func TestValidateDAG_EmptyTasks(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name:  "empty",
		Tasks: []types.WorkflowTask{},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for empty tasks, got nil")
	}
}

func TestValidateDAG_SelfDependency(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "self_dep",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a", DependsOn: []string{"a"}},
		},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for self dependency, got nil")
	}
}

func TestValidateDAG_ChildWorkflowDef(t *testing.T) {
	// A task with ChildWorkflowDef set and no TaskType should be valid.
	childDef := &types.WorkflowDefinition{
		Name: "child_workflow",
		Tasks: []types.WorkflowTask{
			{Name: "child_step1", TaskType: "child_handler_1"},
			{Name: "child_step2", TaskType: "child_handler_2", DependsOn: []string{"child_step1"}},
		},
	}

	def := &types.WorkflowDefinition{
		Name: "parent",
		Tasks: []types.WorkflowTask{
			{Name: "setup", TaskType: "setup_handler"},
			{
				Name:             "run_child",
				ChildWorkflowDef: childDef,
				DependsOn:        []string{"setup"},
			},
			{Name: "teardown", TaskType: "teardown_handler", DependsOn: []string{"run_child"}},
		},
	}

	if err := ValidateDAG(def); err != nil {
		t.Fatalf("expected valid DAG with ChildWorkflowDef task, got error: %v", err)
	}
}

func TestValidateDAG_ChildWorkflowDef_WithPayload(t *testing.T) {
	childDef := &types.WorkflowDefinition{
		Name: "child",
		Tasks: []types.WorkflowTask{
			{Name: "step1", TaskType: "handler", Payload: json.RawMessage(`{"key":"value"}`)},
		},
	}

	def := &types.WorkflowDefinition{
		Name: "parent_with_payload",
		Tasks: []types.WorkflowTask{
			{
				Name:             "child_task",
				ChildWorkflowDef: childDef,
				Payload:          json.RawMessage(`{"parent_key":"parent_value"}`),
			},
		},
	}

	if err := ValidateDAG(def); err != nil {
		t.Fatalf("expected valid DAG, got error: %v", err)
	}
}

func TestValidateDAG_DiamondShape(t *testing.T) {
	//     a
	//    / \
	//   b   c
	//    \ /
	//     d
	def := &types.WorkflowDefinition{
		Name: "diamond",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a"},
			{Name: "b", TaskType: "handler_b", DependsOn: []string{"a"}},
			{Name: "c", TaskType: "handler_c", DependsOn: []string{"a"}},
			{Name: "d", TaskType: "handler_d", DependsOn: []string{"b", "c"}},
		},
	}
	if err := ValidateDAG(def); err != nil {
		t.Fatalf("expected valid diamond DAG, got error: %v", err)
	}
}

// --- ResultFrom validation ---

func TestValidateDAG_ResultFrom_Valid(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "result_from_valid",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a"},
			{Name: "b", TaskType: "handler_b", DependsOn: []string{"a"}, ResultFrom: "a"},
		},
	}
	if err := ValidateDAG(def); err != nil {
		t.Fatalf("expected valid DAG, got error: %v", err)
	}
}

func TestValidateDAG_ResultFrom_UnknownTask(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "result_from_unknown",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a"},
			{Name: "b", TaskType: "handler_b", DependsOn: []string{"a"}, ResultFrom: "nonexistent"},
		},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for unknown ResultFrom target, got nil")
	}
}

func TestValidateDAG_ResultFrom_NotInDependsOn(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "result_from_not_dep",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a"},
			{Name: "b", TaskType: "handler_b"},
			{Name: "c", TaskType: "handler_c", DependsOn: []string{"a"}, ResultFrom: "b"},
		},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for ResultFrom not in DependsOn, got nil")
	}
}

// --- Subflow/Condition cross-validation ---

func TestValidateDAG_SubflowWithConditionRoutes(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "subflow_cond_routes",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a"},
			{
				Name: "b", TaskType: "handler_b", DependsOn: []string{"a"},
				Type:            types.WorkflowTaskSubflow,
				ConditionRoutes: map[uint]string{0: "a"},
			},
		},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for subflow with ConditionRoutes, got nil")
	}
}

func TestValidateDAG_MaxIterationsOnNonCondition(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "max_iter_non_cond",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a", MaxIterations: 5},
		},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for MaxIterations on non-condition task, got nil")
	}
}

// --- Recursive child workflow validation ---

func TestValidateDAG_ChildWorkflow_InvalidChild(t *testing.T) {
	childDef := &types.WorkflowDefinition{
		Name: "invalid_child",
		Tasks: []types.WorkflowTask{
			{Name: "x", TaskType: "handler_x", DependsOn: []string{"y"}},
			{Name: "y", TaskType: "handler_y", DependsOn: []string{"x"}},
		},
	}
	def := &types.WorkflowDefinition{
		Name: "parent_invalid_child",
		Tasks: []types.WorkflowTask{
			{Name: "run_child", ChildWorkflowDef: childDef},
		},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for invalid child workflow, got nil")
	}
}

// --- AllowFailure + AllTasksCompleted / AnyTaskFatallyFailed ---

func TestAllTasksCompleted_WithAllowFailure(t *testing.T) {
	instance := &types.WorkflowInstance{
		Definition: types.WorkflowDefinition{
			Tasks: []types.WorkflowTask{
				{Name: "a", TaskType: "h", AllowFailure: true},
				{Name: "b", TaskType: "h"},
			},
		},
		Tasks: map[string]types.WorkflowTaskState{
			"a": {Status: types.JobStatusFailed},
			"b": {Status: types.JobStatusCompleted},
		},
	}
	if !AllTasksCompleted(instance) {
		t.Fatal("expected AllTasksCompleted=true when failed task has AllowFailure")
	}
}

func TestAllTasksCompleted_WithoutAllowFailure(t *testing.T) {
	instance := &types.WorkflowInstance{
		Definition: types.WorkflowDefinition{
			Tasks: []types.WorkflowTask{
				{Name: "a", TaskType: "h"},
				{Name: "b", TaskType: "h"},
			},
		},
		Tasks: map[string]types.WorkflowTaskState{
			"a": {Status: types.JobStatusFailed},
			"b": {Status: types.JobStatusCompleted},
		},
	}
	if AllTasksCompleted(instance) {
		t.Fatal("expected AllTasksCompleted=false when failed task does NOT have AllowFailure")
	}
}

func TestAnyTaskFatallyFailed_AllowFailure(t *testing.T) {
	instance := &types.WorkflowInstance{
		Definition: types.WorkflowDefinition{
			Tasks: []types.WorkflowTask{
				{Name: "a", TaskType: "h", AllowFailure: true},
				{Name: "b", TaskType: "h"},
			},
		},
		Tasks: map[string]types.WorkflowTaskState{
			"a": {Status: types.JobStatusFailed},
			"b": {Status: types.JobStatusCompleted},
		},
	}
	if AnyTaskFatallyFailed(instance) {
		t.Fatal("expected AnyTaskFatallyFailed=false when only AllowFailure tasks failed")
	}
}

func TestAnyTaskFatallyFailed_RealFailure(t *testing.T) {
	instance := &types.WorkflowInstance{
		Definition: types.WorkflowDefinition{
			Tasks: []types.WorkflowTask{
				{Name: "a", TaskType: "h"},
				{Name: "b", TaskType: "h"},
			},
		},
		Tasks: map[string]types.WorkflowTaskState{
			"a": {Status: types.JobStatusFailed},
			"b": {Status: types.JobStatusCompleted},
		},
	}
	if !AnyTaskFatallyFailed(instance) {
		t.Fatal("expected AnyTaskFatallyFailed=true when non-AllowFailure task failed")
	}
}

// --- Precondition validation ---

func TestValidateDAG_PreconditionValid(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "precond_valid",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a"},
			{Name: "b", TaskType: "handler_b", DependsOn: []string{"a"}, Preconditions: []types.Precondition{
				{Type: "upstream_output", Task: "a", Path: "$.status", Expected: "ok"},
			}},
		},
	}
	if err := ValidateDAG(def); err != nil {
		t.Fatalf("expected valid DAG with precondition, got error: %v", err)
	}
}

func TestValidateDAG_PreconditionUnknownTask(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "precond_unknown",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a"},
			{Name: "b", TaskType: "handler_b", DependsOn: []string{"a"}, Preconditions: []types.Precondition{
				{Type: "upstream_output", Task: "nonexistent", Path: "$.status", Expected: "ok"},
			}},
		},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for precondition referencing unknown task, got nil")
	}
}

func TestValidateDAG_PreconditionNotInDependsOn(t *testing.T) {
	def := &types.WorkflowDefinition{
		Name: "precond_not_dep",
		Tasks: []types.WorkflowTask{
			{Name: "a", TaskType: "handler_a"},
			{Name: "b", TaskType: "handler_b"},
			{Name: "c", TaskType: "handler_c", DependsOn: []string{"a"}, Preconditions: []types.Precondition{
				{Type: "upstream_output", Task: "b", Path: "$.status", Expected: "ok"},
			}},
		},
	}
	if err := ValidateDAG(def); err == nil {
		t.Fatal("expected error for precondition task not in DependsOn, got nil")
	}
}

// --- extractSimpleJSONPath ---

func TestExtractSimpleJSONPath(t *testing.T) {
	data := json.RawMessage(`{"status": "ok", "result": {"code": 42, "msg": "hello"}, "flag": true}`)

	tests := []struct {
		path     string
		expected string
	}{
		{"$.status", "ok"},
		{"status", "ok"},
		{"$.result.code", "42"},
		{"result.msg", "hello"},
		{"$.flag", "true"},
		{"nonexistent", ""},
		{"result.nonexistent", ""},
	}

	for _, tt := range tests {
		actual := extractSimpleJSONPath(data, tt.path)
		if actual != tt.expected {
			t.Errorf("extractSimpleJSONPath(%q) = %q, want %q", tt.path, actual, tt.expected)
		}
	}
}
