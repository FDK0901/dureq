package dynconfig

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/redis/rueidis"
)

// Key patterns for dynamic configuration.
func globalConfigKey(prefix string) string             { return prefix + ":config:global" }
func handlerConfigKey(prefix, taskType string) string  { return prefix + ":config:handler:" + taskType }

// Snapshot holds a point-in-time view of dynamic configuration.
type Snapshot struct {
	// Global settings.
	MaxConcurrency        int           `json:"max_concurrency,omitempty"`
	SchedulerTickInterval time.Duration `json:"scheduler_tick_interval,omitempty"`

	// Per-handler overrides (keyed by task type).
	Handlers map[string]HandlerOverride `json:"handlers,omitempty"`
}

// HandlerOverride holds per-handler dynamic settings.
type HandlerOverride struct {
	Concurrency int           `json:"concurrency,omitempty"`
	Timeout     time.Duration `json:"timeout,omitempty"`
	MaxAttempts int           `json:"max_attempts,omitempty"`
}

// Manager polls Redis for configuration changes and maintains an atomic snapshot.
type Manager struct {
	rdb      rueidis.Client
	prefix   string
	logger   gochainedlog.Logger
	interval time.Duration

	snapshot atomic.Pointer[Snapshot]
	cancel   context.CancelFunc
}

// Config holds manager configuration.
type Config struct {
	RDB          rueidis.Client
	Prefix       string
	PollInterval time.Duration // Default: 10s.
	Logger       gochainedlog.Logger
}

// NewManager creates a new dynamic configuration manager.
func NewManager(cfg Config) *Manager {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	m := &Manager{
		rdb:      cfg.RDB,
		prefix:   cfg.Prefix,
		logger:   cfg.Logger,
		interval: cfg.PollInterval,
	}
	m.snapshot.Store(&Snapshot{})
	return m
}

// Start begins the polling loop. Non-blocking.
func (m *Manager) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	// Do an initial load.
	m.reload(ctx)
	go m.pollLoop(ctx)
}

// Stop halts the polling loop.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
}

// Get returns the current configuration snapshot.
func (m *Manager) Get() *Snapshot {
	return m.snapshot.Load()
}

// GetHandler returns the handler override for a task type, if any.
func (m *Manager) GetHandler(taskType string) (HandlerOverride, bool) {
	snap := m.snapshot.Load()
	if snap.Handlers == nil {
		return HandlerOverride{}, false
	}
	h, ok := snap.Handlers[taskType]
	return h, ok
}

func (m *Manager) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reload(ctx)
		}
	}
}

func (m *Manager) reload(ctx context.Context) {
	snap := &Snapshot{
		Handlers: make(map[string]HandlerOverride),
	}

	// Load global config.
	vals, err := m.rdb.Do(ctx, m.rdb.B().Hgetall().Key(globalConfigKey(m.prefix)).Build()).AsStrMap()
	if err == nil {
		if v, ok := vals["max_concurrency"]; ok {
			if n, err := strconv.Atoi(v); err == nil {
				snap.MaxConcurrency = n
			}
		}
		if v, ok := vals["scheduler_tick_interval"]; ok {
			if d, err := time.ParseDuration(v); err == nil {
				snap.SchedulerTickInterval = d
			}
		}
	}

	// Load per-handler configs by scanning matching keys.
	prefix := handlerConfigKey(m.prefix, "")
	var cursor uint64
	for {
		cmd := m.rdb.B().Scan().Cursor(cursor).Match(prefix + "*").Count(100).Build()
		result, err := m.rdb.Do(ctx, cmd).AsScanEntry()
		if err != nil {
			break
		}
		for _, key := range result.Elements {
			taskType := key[len(prefix):]
			hvals, err := m.rdb.Do(ctx, m.rdb.B().Hgetall().Key(key).Build()).AsStrMap()
			if err != nil {
				continue
			}
			ho := HandlerOverride{}
			if v, ok := hvals["concurrency"]; ok {
				if n, err := strconv.Atoi(v); err == nil {
					ho.Concurrency = n
				}
			}
			if v, ok := hvals["timeout"]; ok {
				if d, err := time.ParseDuration(v); err == nil {
					ho.Timeout = d
				}
			}
			if v, ok := hvals["max_attempts"]; ok {
				if n, err := strconv.Atoi(v); err == nil {
					ho.MaxAttempts = n
				}
			}
			snap.Handlers[taskType] = ho
		}
		cursor = result.Cursor
		if cursor == 0 {
			break
		}
	}

	m.snapshot.Store(snap)
}

// --- Write API (for admin/client use) ---

// SetGlobal sets a global config field.
func (m *Manager) SetGlobal(ctx context.Context, field, value string) error {
	return m.rdb.Do(ctx, m.rdb.B().Hset().Key(globalConfigKey(m.prefix)).FieldValue().FieldValue(field, value).Build()).Error()
}

// SetHandler sets a handler config field for a specific task type.
func (m *Manager) SetHandler(ctx context.Context, taskType, field, value string) error {
	return m.rdb.Do(ctx, m.rdb.B().Hset().Key(handlerConfigKey(m.prefix, taskType)).FieldValue().FieldValue(field, value).Build()).Error()
}

// DeleteGlobalField deletes a global config field.
func (m *Manager) DeleteGlobalField(ctx context.Context, field string) error {
	return m.rdb.Do(ctx, m.rdb.B().Hdel().Key(globalConfigKey(m.prefix)).Field(field).Build()).Error()
}

// DeleteHandler deletes all overrides for a task type.
func (m *Manager) DeleteHandler(ctx context.Context, taskType string) error {
	return m.rdb.Do(ctx, m.rdb.B().Del().Key(handlerConfigKey(m.prefix, taskType)).Build()).Error()
}

// GetAllGlobal returns all global config fields.
func (m *Manager) GetAllGlobal(ctx context.Context) (map[string]string, error) {
	return m.rdb.Do(ctx, m.rdb.B().Hgetall().Key(globalConfigKey(m.prefix)).Build()).AsStrMap()
}

// GetAllHandler returns all config fields for a task type.
func (m *Manager) GetAllHandler(ctx context.Context, taskType string) (map[string]string, error) {
	return m.rdb.Do(ctx, m.rdb.B().Hgetall().Key(handlerConfigKey(m.prefix, taskType)).Build()).AsStrMap()
}
