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
