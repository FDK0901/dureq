// Package handlers provides a static handler registry for dureqd.
//
// To add a new built-in handler, create a file in this package (e.g., myjob.go)
// and append to the builtinHandlers slice via an init() function.
//
// Handlers can be selectively enabled/disabled at startup via RegisterFiltered.
package handlers

import (
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/pkg/types"
)

// builtinHandlers is the global registry of statically-linked handlers.
// Add handlers via init() functions in separate files within this package.
var builtinHandlers []types.HandlerDefinition

// Register appends a handler definition to the builtin registry.
// Call this from init() in handler files.
func Register(h types.HandlerDefinition) {
	builtinHandlers = append(builtinHandlers, h)
}

// RegisterAll registers all built-in handlers with the given server.
// Returns the number of successfully registered handlers and the number of errors.
func RegisterAll(srv *server.Server) (registered, errors int) {
	for _, h := range builtinHandlers {
		if err := srv.RegisterHandler(h); err != nil {
			errors++
			continue
		}
		registered++
	}
	return
}

// RegisterFiltered registers only handlers whose task type is in the enabled set.
// If enabled is nil or empty, all handlers are registered (same as RegisterAll).
func RegisterFiltered(srv *server.Server, enabled map[string]bool) (registered, skipped int) {
	for _, h := range builtinHandlers {
		if len(enabled) > 0 && !enabled[string(h.TaskType)] {
			skipped++
			continue
		}
		if err := srv.RegisterHandler(h); err != nil {
			skipped++
			continue
		}
		registered++
	}
	return
}

// List returns the task types of all registered built-in handlers.
func List() []string {
	names := make([]string, len(builtinHandlers))
	for i, h := range builtinHandlers {
		names[i] = string(h.TaskType)
	}
	return names
}

// Count returns the number of registered built-in handlers.
func Count() int {
	return len(builtinHandlers)
}
