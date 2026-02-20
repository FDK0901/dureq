package types

import "encoding/json"

// PeriodicTaskConfig describes a single periodic task to be managed.
type PeriodicTaskConfig struct {
	// CronExpr is the cron expression (e.g., "*/5 * * * *").
	CronExpr string `json:"cron_expr"`

	// TaskType is the handler to invoke.
	TaskType TaskType `json:"task_type"`

	// Payload is the task payload.
	Payload json.RawMessage `json:"payload,omitempty"`

	// UniqueKey prevents duplicate schedules for the same logical task.
	UniqueKey string `json:"unique_key,omitempty"`
}

// PeriodicTaskConfigProvider is an interface that returns the current set of
// periodic tasks. The PeriodicTaskManager calls GetConfigs periodically and
// syncs the returned tasks with the scheduler — adding new ones, removing
// stale ones, and updating changed ones.
type PeriodicTaskConfigProvider interface {
	GetConfigs() ([]*PeriodicTaskConfig, error)
}

// PeriodicTaskConfigProviderFunc is a convenience adapter.
type PeriodicTaskConfigProviderFunc func() ([]*PeriodicTaskConfig, error)

func (f PeriodicTaskConfigProviderFunc) GetConfigs() ([]*PeriodicTaskConfig, error) {
	return f()
}
