package periodic

import (
	"context"
	"encoding/json"
	"time"

	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/pkg/types"
)

// ManagerConfig configures the PeriodicTaskManager.
type ManagerConfig struct {
	// Provider returns the current set of periodic task configs.
	Provider types.PeriodicTaskConfigProvider

	// Client is the dureq client used to enqueue/update schedules.
	Client *client.Client

	// SyncInterval is how often to re-sync with the provider. Default: 1m.
	SyncInterval time.Duration

	Logger gochainedlog.Logger
}

// Manager syncs periodic tasks from an external config source into the
// dureq scheduler. It adds new tasks, updates changed tasks, and removes
// tasks that no longer appear in the provider output.
type Manager struct {
	provider     types.PeriodicTaskConfigProvider
	client       *client.Client
	syncInterval time.Duration
	logger       gochainedlog.Logger
	cancel       context.CancelFunc

	// Tracks which unique keys we've created so we can detect removals.
	tracked map[string]*types.PeriodicTaskConfig
}

// NewManager creates a new periodic task manager.
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.Logger == nil {
		cfg.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = 1 * time.Minute
	}
	return &Manager{
		provider:     cfg.Provider,
		client:       cfg.Client,
		syncInterval: cfg.SyncInterval,
		logger:       cfg.Logger,
		tracked:      make(map[string]*types.PeriodicTaskConfig),
	}
}

// Start begins the sync loop.
func (m *Manager) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	go m.loop(ctx)
}

// Stop halts the sync loop.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
}

func (m *Manager) loop(ctx context.Context) {
	// Sync immediately on start.
	m.sync(ctx)

	ticker := time.NewTicker(m.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sync(ctx)
		}
	}
}

func (m *Manager) sync(ctx context.Context) {
	configs, err := m.provider.GetConfigs()
	if err != nil {
		m.logger.Warn().Err(err).Msg("periodic: failed to get configs from provider")
		return
	}

	// Build a set of current unique keys.
	currentKeys := make(map[string]*types.PeriodicTaskConfig, len(configs))
	for _, cfg := range configs {
		key := cfg.UniqueKey
		if key == "" {
			key = string(cfg.TaskType) + ":" + cfg.CronExpr
		}
		currentKeys[key] = cfg
	}

	// Add or update tasks.
	for key, cfg := range currentKeys {
		existing, tracked := m.tracked[key]
		if tracked && configEqual(existing, cfg) {
			continue // No change.
		}

		uniqueKey := key
		cronExpr := cfg.CronExpr

		var payload json.RawMessage
		if cfg.Payload != nil {
			payload = cfg.Payload
		} else {
			payload = json.RawMessage("{}")
		}

		// If updating an existing task, release the old unique key first so the
		// NX-guarded SetUniqueKey inside EnqueueScheduled can succeed.
		if tracked {
			if err := m.client.Store().DeleteUniqueKey(ctx, key); err != nil {
				m.logger.Warn().String("key", key).Err(err).Msg("periodic: failed to release unique key for update")
			}
		}

		_, err := m.client.EnqueueScheduled(ctx, &client.EnqueueRequest{
			TaskType: cfg.TaskType,
			Payload:  payload,
			Schedule: types.Schedule{
				Type:     types.ScheduleCron,
				CronExpr: &cronExpr,
			},
			UniqueKey: &uniqueKey,
		})
		if err != nil {
			m.logger.Warn().String("key", key).String("task_type", string(cfg.TaskType)).Err(err).Msg("periodic: failed to enqueue/update task")
			continue
		}

		m.tracked[key] = cfg
		if tracked {
			m.logger.Info().String("key", key).Msg("periodic: updated task")
		} else {
			m.logger.Info().String("key", key).Msg("periodic: added task")
		}
	}

	// Remove tasks no longer in provider output.
	for key := range m.tracked {
		if _, exists := currentKeys[key]; !exists {
			// Cancel the job by unique key.
			if err := m.client.CancelByUniqueKey(ctx, key); err != nil {
				m.logger.Warn().String("key", key).Err(err).Msg("periodic: failed to cancel removed task")
			} else {
				m.logger.Info().String("key", key).Msg("periodic: removed task")
			}
			delete(m.tracked, key)
		}
	}
}

func configEqual(a, b *types.PeriodicTaskConfig) bool {
	if a.CronExpr != b.CronExpr {
		return false
	}
	if a.TaskType != b.TaskType {
		return false
	}
	if string(a.Payload) != string(b.Payload) {
		return false
	}
	return true
}
