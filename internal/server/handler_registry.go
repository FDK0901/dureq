package server

import (
	"fmt"
	"strings"
	"sync"

	"github.com/FDK0901/dureq/pkg/types"
)

// HandlerRegistry stores registered task type handlers.
// It supports exact matches and pattern-based routing (e.g., "email:*").
// Longest matching pattern wins, similar to net/http.ServeMux.
// It is safe for concurrent use.
type HandlerRegistry struct {
	mu       sync.RWMutex
	handlers map[types.TaskType]*types.HandlerDefinition
	patterns []patternEntry // sorted by specificity (longest prefix first)
}

// patternEntry stores a wildcard pattern registration.
type patternEntry struct {
	prefix string // pattern without trailing "*"
	def    *types.HandlerDefinition
}

// NewHandlerRegistry creates a new empty handler registry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[types.TaskType]*types.HandlerDefinition),
	}
}

// Register adds a handler definition to the registry.
// TaskType can be an exact name ("email.send") or a pattern ("email:*", "email.*").
// Patterns ending with "*" match any task type sharing that prefix.
func (r *HandlerRegistry) Register(def types.HandlerDefinition) error {
	if def.TaskType == "" {
		return fmt.Errorf("task type is required")
	}
	if def.Handler == nil && def.HandlerWithResult == nil {
		return fmt.Errorf("handler function is required for task type %q", def.TaskType)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	tt := string(def.TaskType)

	// Pattern registration (e.g., "email:*", "billing.*").
	if strings.HasSuffix(tt, "*") {
		prefix := tt[:len(tt)-1]
		for _, p := range r.patterns {
			if p.prefix == prefix {
				return fmt.Errorf("pattern already registered: %q", def.TaskType)
			}
		}
		defCopy := def
		r.patterns = append(r.patterns, patternEntry{prefix: prefix, def: &defCopy})
		// Keep sorted by longest prefix first for correct matching order.
		sortPatterns(r.patterns)
		return nil
	}

	// Exact registration.
	if _, exists := r.handlers[def.TaskType]; exists {
		return fmt.Errorf("handler already registered for task type %q", def.TaskType)
	}

	r.handlers[def.TaskType] = &def
	return nil
}

// Get returns the handler definition for a task type.
// It first checks exact matches, then falls back to pattern matching
// (longest prefix wins).
func (r *HandlerRegistry) Get(taskType types.TaskType) (*types.HandlerDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Exact match first.
	if def, ok := r.handlers[taskType]; ok {
		return def, true
	}

	// Pattern match — longest prefix wins.
	tt := string(taskType)
	for _, p := range r.patterns {
		if strings.HasPrefix(tt, p.prefix) {
			return p.def, true
		}
	}

	return nil, false
}

// TaskTypes returns all registered task type names (exact + pattern).
func (r *HandlerRegistry) TaskTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]string, 0, len(r.handlers)+len(r.patterns))
	for tt := range r.handlers {
		result = append(result, string(tt))
	}
	for _, p := range r.patterns {
		result = append(result, p.prefix+"*")
	}
	return result
}

// Len returns the number of registered handlers (exact + pattern).
func (r *HandlerRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers) + len(r.patterns)
}

// HandlerVersions returns a map of task type → handler version for all
// registered handlers that have a non-empty Version.
func (r *HandlerRegistry) HandlerVersions() map[types.TaskType]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions := make(map[types.TaskType]string)
	for tt, def := range r.handlers {
		if def.Version != "" {
			versions[tt] = def.Version
		}
	}
	return versions
}

// sortPatterns sorts pattern entries by prefix length descending (longest first).
func sortPatterns(patterns []patternEntry) {
	// Simple insertion sort — pattern lists are typically very small.
	for i := 1; i < len(patterns); i++ {
		j := i
		for j > 0 && len(patterns[j].prefix) > len(patterns[j-1].prefix) {
			patterns[j], patterns[j-1] = patterns[j-1], patterns[j]
			j--
		}
	}
}
