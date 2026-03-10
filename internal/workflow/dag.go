// Package workflow implements workflow DAG validation, topological ordering,
// and the orchestrator that advances workflow state on task completion.
package workflow

import (
	"fmt"
	"sort"

	"github.com/FDK0901/dureq/pkg/types"
)

// DefaultMaxConditionIterations is the system-wide limit for condition node re-evaluation.
const DefaultMaxConditionIterations = 100

// ValidateDAG checks that a workflow definition forms a valid graph:
//   - All task names are unique
//   - All DependsOn references exist
//   - No circular dependencies in purely static tasks (Kahn's algorithm)
//   - Condition nodes may create back-edges (cycles are allowed through condition nodes
//     and are bounded by MaxIterations at runtime)
//   - ConditionRoutes targets exist as task names
func ValidateDAG(def *types.WorkflowDefinition) error {
	if len(def.Tasks) == 0 {
		return fmt.Errorf("workflow %q has no tasks", def.Name)
	}

	names := make(map[string]struct{}, len(def.Tasks))
	tasksByName := make(map[string]*types.WorkflowTask, len(def.Tasks))
	for i := range def.Tasks {
		t := &def.Tasks[i]
		if t.Name == "" {
			return fmt.Errorf("workflow %q: task has empty name", def.Name)
		}
		if _, dup := names[t.Name]; dup {
			return fmt.Errorf("workflow %q: duplicate task name %q", def.Name, t.Name)
		}
		names[t.Name] = struct{}{}
		tasksByName[t.Name] = t
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

	// Validate condition routes reference existing tasks.
	for _, t := range def.Tasks {
		if t.Type == types.WorkflowTaskCondition {
			if len(t.ConditionRoutes) == 0 {
				return fmt.Errorf("workflow %q: condition task %q has no routes", def.Name, t.Name)
			}
			for route, target := range t.ConditionRoutes {
				if _, ok := names[target]; !ok {
					return fmt.Errorf("workflow %q: condition task %q route %d references unknown task %q", def.Name, t.Name, route, target)
				}
			}
		}
	}

	// Validate ResultFrom references.
	for _, t := range def.Tasks {
		if t.ResultFrom != "" {
			if _, ok := names[t.ResultFrom]; !ok {
				return fmt.Errorf("workflow %q: task %q ResultFrom references unknown task %q", def.Name, t.Name, t.ResultFrom)
			}
			// ResultFrom target must be in DependsOn.
			found := false
			for _, dep := range t.DependsOn {
				if dep == t.ResultFrom {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("workflow %q: task %q ResultFrom %q must also be in DependsOn", def.Name, t.Name, t.ResultFrom)
			}
		}
	}

	// Validate preconditions.
	for _, t := range def.Tasks {
		for _, pc := range t.Preconditions {
			if pc.Type == "upstream_output" {
				if pc.Task == "" {
					return fmt.Errorf("workflow %q: task %q precondition references empty task name", def.Name, t.Name)
				}
				if _, ok := names[pc.Task]; !ok {
					return fmt.Errorf("workflow %q: task %q precondition references unknown task %q", def.Name, t.Name, pc.Task)
				}
				// Precondition task must be in DependsOn.
				found := false
				for _, dep := range t.DependsOn {
					if dep == pc.Task {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("workflow %q: task %q precondition task %q must be in DependsOn", def.Name, t.Name, pc.Task)
				}
			}
		}
	}

	// Validate subflow tasks don't have condition-specific fields.
	for _, t := range def.Tasks {
		if t.Type == types.WorkflowTaskSubflow && len(t.ConditionRoutes) > 0 {
			return fmt.Errorf("workflow %q: subflow task %q must not have ConditionRoutes", def.Name, t.Name)
		}
		if t.Type != types.WorkflowTaskCondition && t.MaxIterations > 0 {
			return fmt.Errorf("workflow %q: task %q has MaxIterations but is not a condition node", def.Name, t.Name)
		}
	}

	// Recursively validate child workflow definitions.
	for _, t := range def.Tasks {
		if t.ChildWorkflowDef != nil {
			if err := ValidateDAG(t.ChildWorkflowDef); err != nil {
				return fmt.Errorf("workflow %q: child workflow in task %q: %w", def.Name, t.Name, err)
			}
		}
	}

	// Kahn's algorithm for cycle detection.
	// Condition nodes introduce intentional cycles (back-edges), so we exclude
	// edges FROM condition routes when checking for cycles. The condition node
	// itself still participates in the normal DAG (via DependsOn).
	conditionTargets := make(map[string]bool)
	for _, t := range def.Tasks {
		if t.Type == types.WorkflowTaskCondition {
			for _, target := range t.ConditionRoutes {
				// Mark edges from condition to route targets as allowed back-edges.
				conditionTargets[t.Name+"->"+target] = true
			}
		}
	}

	inDegree := make(map[string]int, len(def.Tasks))
	dependents := make(map[string][]string, len(def.Tasks))
	for _, t := range def.Tasks {
		if _, ok := inDegree[t.Name]; !ok {
			inDegree[t.Name] = 0
		}
		for _, dep := range t.DependsOn {
			// Skip back-edges created by condition routing.
			if conditionTargets[dep+"->"+t.Name] {
				continue
			}
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

// InitPendingDeps initializes the PendingDeps map for a workflow instance.
// Call this once at workflow creation to set up reference counting.
func InitPendingDeps(instance *types.WorkflowInstance) {
	instance.PendingDeps = make(map[string]int, len(instance.Definition.Tasks))
	for _, t := range instance.Definition.Tasks {
		instance.PendingDeps[t.Name] = len(t.DependsOn)
	}
}

// DecrementPendingDeps decrements the pending dep count for all successors
// of the completed task. For condition nodes, only the selected route's target
// is decremented (not all successors).
func DecrementPendingDeps(instance *types.WorkflowInstance, completedTask string) {
	if instance.PendingDeps == nil {
		return
	}

	// Find the completed task definition.
	var completedDef *types.WorkflowTask
	for i := range instance.Definition.Tasks {
		if instance.Definition.Tasks[i].Name == completedTask {
			completedDef = &instance.Definition.Tasks[i]
			break
		}
	}

	if completedDef != nil && completedDef.Type == types.WorkflowTaskCondition {
		// For condition nodes, only decrement the selected route target.
		state := instance.Tasks[completedTask]
		if state.ConditionRoute != nil {
			if target, ok := completedDef.ConditionRoutes[*state.ConditionRoute]; ok {
				if count, exists := instance.PendingDeps[target]; exists && count > 0 {
					instance.PendingDeps[target] = count - 1
				}
			}
		}
		return
	}

	// For regular tasks, decrement all dependents.
	for _, t := range instance.Definition.Tasks {
		for _, dep := range t.DependsOn {
			if dep == completedTask {
				if count, exists := instance.PendingDeps[t.Name]; exists && count > 0 {
					instance.PendingDeps[t.Name] = count - 1
				}
			}
		}
	}
}

// ReadyTasks returns the names of tasks in a workflow instance that are
// ready to be dispatched. Uses reference counting (PendingDeps) when available
// for O(1) per-task checking, falling back to full scan for backwards compatibility.
// Results are sorted by priority (highest first).
func ReadyTasks(instance *types.WorkflowInstance) []string {
	// Build a lookup of task definitions by name.
	taskDefs := make(map[string]types.WorkflowTask, len(instance.Definition.Tasks))
	for _, t := range instance.Definition.Tasks {
		taskDefs[t.Name] = t
	}

	var ready []string

	if instance.PendingDeps != nil {
		// Fast path: use reference counting.
		for name, state := range instance.Tasks {
			if state.Status != types.JobStatusPending {
				continue
			}
			if count, ok := instance.PendingDeps[name]; ok && count == 0 {
				ready = append(ready, name)
			}
		}
	} else {
		// Legacy path: full dependency scan.
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
	}

	// Sort by priority (highest first) for deterministic dispatch order.
	if len(ready) > 1 {
		sort.Slice(ready, func(i, j int) bool {
			pi := taskDefs[ready[i]].Priority
			pj := taskDefs[ready[j]].Priority
			if pi != pj {
				return pi > pj
			}
			return ready[i] < ready[j] // stable sort by name
		})
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

// AllTasksCompleted returns true if every task in the workflow has reached
// a terminal state. Tasks with AllowFailure=true count as "done" even if
// they failed.
func AllTasksCompleted(instance *types.WorkflowInstance) bool {
	allowFailure := buildAllowFailureSet(instance)
	for name, state := range instance.Tasks {
		if state.Status == types.JobStatusCompleted {
			continue
		}
		if allowFailure[name] && (state.Status == types.JobStatusFailed || state.Status == types.JobStatusDead) {
			continue
		}
		return false
	}
	return true
}

// AnyTaskFatallyFailed returns true if any task has failed and is NOT marked
// AllowFailure. Used to detect stuck workflows that can never complete.
func AnyTaskFatallyFailed(instance *types.WorkflowInstance) bool {
	allowFailure := buildAllowFailureSet(instance)
	for name, state := range instance.Tasks {
		if (state.Status == types.JobStatusFailed || state.Status == types.JobStatusDead) && !allowFailure[name] {
			return true
		}
	}
	return false
}

// AnyTaskFailed returns true if any task in the workflow has failed (including AllowFailure tasks).
func AnyTaskFailed(instance *types.WorkflowInstance) bool {
	for _, state := range instance.Tasks {
		if state.Status == types.JobStatusFailed || state.Status == types.JobStatusDead {
			return true
		}
	}
	return false
}

// buildAllowFailureSet returns a set of task names that have AllowFailure=true.
func buildAllowFailureSet(instance *types.WorkflowInstance) map[string]bool {
	af := make(map[string]bool)
	for _, t := range instance.Definition.Tasks {
		if t.AllowFailure {
			af[t.Name] = true
		}
	}
	return af
}

// isBackEdge returns true if routeTarget is reachable from condName via the
// dependency graph (i.e., routeTarget → ... → condName forms a path).
// This means the route creates a cycle (back-edge / loop).
func isBackEdge(def *types.WorkflowDefinition, condName, routeTarget string) bool {
	successors := make(map[string][]string)
	for _, t := range def.Tasks {
		for _, dep := range t.DependsOn {
			successors[dep] = append(successors[dep], t.Name)
		}
	}

	visited := make(map[string]bool)
	queue := []string{routeTarget}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if visited[node] {
			continue
		}
		visited[node] = true
		for _, succ := range successors[node] {
			if succ == condName {
				return true
			}
			queue = append(queue, succ)
		}
	}
	return false
}

// resetLoopBody resets all tasks in the dependency path from startTask to endTask
// (exclusive of both) back to pending status. This is used when a condition node
// loops back to re-execute a portion of the DAG.
func resetLoopBody(instance *types.WorkflowInstance, startTask, endTask string) {
	// Build successors map: task → tasks that depend on it.
	successors := make(map[string][]string)
	for _, t := range instance.Definition.Tasks {
		for _, dep := range t.DependsOn {
			successors[dep] = append(successors[dep], t.Name)
		}
	}

	// BFS from startTask to find all reachable tasks before endTask.
	visited := make(map[string]bool)
	queue := []string{startTask}
	visited[startTask] = true

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		for _, succ := range successors[node] {
			if visited[succ] || succ == startTask || succ == endTask {
				continue
			}
			visited[succ] = true
			queue = append(queue, succ)

			// Reset intermediate task to pending.
			if state, exists := instance.Tasks[succ]; exists && state.Status == types.JobStatusCompleted {
				state.Status = types.JobStatusPending
				state.JobID = ""
				state.Error = nil
				state.StartedAt = nil
				state.FinishedAt = nil
				instance.Tasks[succ] = state

				// Re-initialize PendingDeps from definition.
				if instance.PendingDeps != nil {
					for _, t := range instance.Definition.Tasks {
						if t.Name == succ {
							instance.PendingDeps[succ] = len(t.DependsOn)
							break
						}
					}
				}
			}
		}
	}
}

// MaxIterationsForTask returns the max iterations for a condition task,
// using the task's configured value or the system default.
func MaxIterationsForTask(task *types.WorkflowTask) int {
	if task.MaxIterations > 0 {
		return task.MaxIterations
	}
	return DefaultMaxConditionIterations
}
