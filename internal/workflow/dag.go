// Package workflow implements workflow DAG validation, topological ordering,
// and the orchestrator that advances workflow state on task completion.
package workflow

import (
	"fmt"

	"github.com/FDK0901/dureq/pkg/types"
)

// ValidateDAG checks that a workflow definition forms a valid DAG:
// - All task names are unique
// - All DependsOn references exist
// - No circular dependencies (Kahn's algorithm)
func ValidateDAG(def *types.WorkflowDefinition) error {
	if len(def.Tasks) == 0 {
		return fmt.Errorf("workflow %q has no tasks", def.Name)
	}

	names := make(map[string]struct{}, len(def.Tasks))
	for _, t := range def.Tasks {
		if t.Name == "" {
			return fmt.Errorf("workflow %q: task has empty name", def.Name)
		}
		if _, dup := names[t.Name]; dup {
			return fmt.Errorf("workflow %q: duplicate task name %q", def.Name, t.Name)
		}
		names[t.Name] = struct{}{}
	}

	// Check all DependsOn references exist.
	for _, t := range def.Tasks {
		for _, dep := range t.DependsOn {
			if _, ok := names[dep]; !ok {
				return fmt.Errorf("workflow %q: task %q depends on unknown task %q", def.Name, t.Name, dep)
			}
			if dep == t.Name {
				return fmt.Errorf("workflow %q: task %q depends on itself", def.Name, t.Name)
			}
		}
	}

	// Kahn's algorithm for cycle detection.
	inDegree := make(map[string]int, len(def.Tasks))
	dependents := make(map[string][]string, len(def.Tasks))
	for _, t := range def.Tasks {
		if _, ok := inDegree[t.Name]; !ok {
			inDegree[t.Name] = 0
		}
		for _, dep := range t.DependsOn {
			inDegree[t.Name]++
			dependents[dep] = append(dependents[dep], t.Name)
		}
	}

	queue := make([]string, 0)
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++

		for _, dep := range dependents[node] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}

	if visited != len(def.Tasks) {
		return fmt.Errorf("%w: %s", types.ErrCyclicDependency, def.Name)
	}

	return nil
}

// TopologicalOrder returns the tasks in a valid execution order.
func TopologicalOrder(def *types.WorkflowDefinition) ([]string, error) {
	if err := ValidateDAG(def); err != nil {
		return nil, err
	}

	inDegree := make(map[string]int, len(def.Tasks))
	dependents := make(map[string][]string, len(def.Tasks))
	for _, t := range def.Tasks {
		if _, ok := inDegree[t.Name]; !ok {
			inDegree[t.Name] = 0
		}
		for _, dep := range t.DependsOn {
			inDegree[t.Name]++
			dependents[dep] = append(dependents[dep], t.Name)
		}
	}

	queue := make([]string, 0)
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	order := make([]string, 0, len(def.Tasks))
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)

		for _, dep := range dependents[node] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}

	return order, nil
}

// ReadyTasks returns the names of tasks in a workflow instance that are
// ready to be dispatched: all their dependencies are completed and the
// task itself is still pending.
func ReadyTasks(instance *types.WorkflowInstance) []string {
	// Build a lookup of task definitions by name.
	taskDefs := make(map[string]types.WorkflowTask, len(instance.Definition.Tasks))
	for _, t := range instance.Definition.Tasks {
		taskDefs[t.Name] = t
	}

	var ready []string
	for name, state := range instance.Tasks {
		if state.Status != types.JobStatusPending {
			continue
		}
		def, ok := taskDefs[name]
		if !ok {
			continue
		}
		allDepsDone := true
		for _, dep := range def.DependsOn {
			depState, exists := instance.Tasks[dep]
			if !exists || depState.Status != types.JobStatusCompleted {
				allDepsDone = false
				break
			}
		}
		if allDepsDone {
			ready = append(ready, name)
		}
	}
	return ready
}

// RootTasks returns the names of tasks with no dependencies.
func RootTasks(def *types.WorkflowDefinition) []string {
	var roots []string
	for _, t := range def.Tasks {
		if len(t.DependsOn) == 0 {
			roots = append(roots, t.Name)
		}
	}
	return roots
}

// AllTasksCompleted returns true if every task in the workflow has completed.
func AllTasksCompleted(instance *types.WorkflowInstance) bool {
	for _, state := range instance.Tasks {
		if state.Status != types.JobStatusCompleted {
			return false
		}
	}
	return true
}

// AnyTaskFailed returns true if any task in the workflow has failed.
func AnyTaskFailed(instance *types.WorkflowInstance) bool {
	for _, state := range instance.Tasks {
		if state.Status == types.JobStatusFailed || state.Status == types.JobStatusDead {
			return true
		}
	}
	return false
}
