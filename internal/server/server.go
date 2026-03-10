package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"time"

	"github.com/FDK0901/dureq/internal/aggregation"
	"github.com/FDK0901/dureq/internal/archival"
	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/dynconfig"
	"github.com/FDK0901/dureq/internal/election"
	"github.com/FDK0901/dureq/internal/lock"
	"github.com/FDK0901/dureq/internal/scheduler"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/internal/worker"
	"github.com/FDK0901/dureq/internal/workflow"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// Server is the main dureq server node. All nodes are workers;
// the elected leader additionally runs the scheduler and orchestrator.
type Server struct {
	cfg         Config
	logger      gochainedlog.Logger
	rdb         rueidis.Client
	store       *store.RedisStore
	registry    *HandlerRegistry
	middlewares []types.MiddlewareFunc
	hooks       types.Hooks
	elector     *election.Elector
	locker      *lock.Locker
	sched       *scheduler.Scheduler
	disp        *dispatcher.Dispatcher
	work        *worker.Worker
	orch        *workflow.Orchestrator
	aggProc     *aggregation.Processor
	archCleaner *archival.Cleaner
	dynCfg      *dynconfig.Manager

	startedAt time.Time
	ctx       context.Context
	cancel    context.CancelFunc
}

// New creates a new server with the given options.
// Call RegisterHandler to add task types, then Start to begin.
func New(opts ...Option) (*Server, error) {
	cfg := Config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.defaults()

	if cfg.RedisOptions.URL == "" && cfg.RedisOptions.SentinelConfig == nil && len(cfg.RedisOptions.ClusterAddrs) == 0 {
		return nil, fmt.Errorf("Redis URL, Sentinel config, or Cluster addresses required")
	}

	s := &Server{
		cfg:      cfg,
		logger:   cfg.Logger,
		registry: NewHandlerRegistry(),
	}

	if cfg.NodeID == "" {
		s.cfg.NodeID = election.GenerateNodeID()
	}

	return s, nil
}

// Use adds global middleware that wraps every handler. Must be called before Start.
// Middlewares are applied in order: first added = outermost.
func (s *Server) Use(mw ...types.MiddlewareFunc) {
	s.middlewares = append(s.middlewares, mw...)
}

// Middlewares returns the registered global middleware chain.
func (s *Server) Middlewares() []types.MiddlewareFunc {
	return s.middlewares
}

// RegisterHandler registers a task type handler on this server.
// Must be called before Start.
func (s *Server) RegisterHandler(def types.HandlerDefinition) error {
	return s.registry.Register(def)
}

// Hooks returns the lifecycle hooks for direct manipulation.
func (s *Server) Hooks() *types.Hooks { return &s.hooks }

// OnJobCompleted registers a hook called when any job completes successfully.
func (s *Server) OnJobCompleted(fn types.HookFunc) { s.hooks.OnJobCompleted = append(s.hooks.OnJobCompleted, fn) }

// OnJobFailed registers a hook called when a job fails (may be retried).
func (s *Server) OnJobFailed(fn types.HookFunc) { s.hooks.OnJobFailed = append(s.hooks.OnJobFailed, fn) }

// OnJobDead registers a hook called when a job exhausts all retries.
func (s *Server) OnJobDead(fn types.HookFunc) { s.hooks.OnJobDead = append(s.hooks.OnJobDead, fn) }

// OnJobPaused registers a hook called when a job is paused.
func (s *Server) OnJobPaused(fn types.HookFunc) { s.hooks.OnJobPaused = append(s.hooks.OnJobPaused, fn) }

// OnJobResumed registers a hook called when a paused job is resumed.
func (s *Server) OnJobResumed(fn types.HookFunc) { s.hooks.OnJobResumed = append(s.hooks.OnJobResumed, fn) }

// Store returns the Redis store. Available after Start().
func (s *Server) Store() *store.RedisStore { return s.store }

// Worker returns the worker instance. Available after Start().
func (s *Server) Worker() *worker.Worker { return s.work }

// RedisClient returns the Redis client. Available after Start().
func (s *Server) RedisClient() rueidis.Client { return s.rdb }

// Dispatcher returns the dispatcher. Available after Start().
func (s *Server) Dispatcher() *dispatcher.Dispatcher { return s.disp }

// DynConfig returns the dynamic configuration manager. Available after Start().
func (s *Server) DynConfig() *dynconfig.Manager { return s.dynCfg }

// Start connects to Redis, initializes all subsystems, and begins operation.
func (s *Server) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.startedAt = time.Now()

	// Create Redis client.
	rdb, err := s.createRedisClient()
	if err != nil {
		return fmt.Errorf("redis connect: %w", err)
	}
	s.rdb = rdb

	// Ping to verify connectivity.
	if err := s.rdb.Do(s.ctx, s.rdb.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	s.logger.Info().String("url", s.cfg.RedisOptions.URL).String("node_id", s.cfg.NodeID).Msg("connected to Redis")

	// Initialize Redis store.
	s.store, err = store.NewRedisStore(s.rdb, s.cfg.StoreConfig, s.logger)
	if err != nil {
		return fmt.Errorf("redis store: %w", err)
	}

	// Ensure Redis Streams and consumer groups exist.
	if err := s.store.EnsureStreams(s.ctx); err != nil {
		return fmt.Errorf("ensure streams: %w", err)
	}

	// Start delayed retry poller.
	go store.NewDelayedPoller(s.store, s.logger).Start(s.ctx)

	// Initialize dynamic configuration manager.
	s.dynCfg = dynconfig.NewManager(dynconfig.Config{
		RDB:    s.rdb,
		Prefix: s.store.Prefix(),
		Logger: s.logger,
	})
	s.dynCfg.Start(s.ctx)

	// Initialize dispatcher.
	s.disp = dispatcher.New(s.store, s.logger)
	s.disp.SetHooks(&s.hooks)

	// Initialize locker.
	s.locker = lock.NewLocker(lock.LockerConfig{
		RDB:                s.rdb,
		Prefix:             s.store.Prefix(),
		LockTTL:            s.cfg.LockTTL,
		AutoExtendInterval: s.cfg.LockAutoExtend,
		Logger:             s.logger,
	})

	// Initialize scheduler (runs only on leader).
	s.sched = scheduler.New(scheduler.Config{
		Store:        s.store,
		Dispatcher:   s.disp,
		TickInterval: s.cfg.SchedulerTickInterval,
		Logger:       s.logger,
	})

	// Initialize worker.
	s.work, err = worker.New(worker.Config{
		NodeID:            s.cfg.NodeID,
		Store:             s.store,
		Registry:          s.registry,
		Disp:              s.disp,
		Locker:            s.locker,
		MaxConcurrency:    s.cfg.MaxConcurrency,
		ShutdownTimeout:   s.cfg.ShutdownTimeout,
		GlobalMiddlewares: s.middlewares,
		Logger:            s.logger,
	})
	if err != nil {
		return fmt.Errorf("worker: %w", err)
	}

	// Initialize aggregation processor (runs on every node with GroupConfig, not just leader).
	// FlushGroup uses an atomic Lua script, so concurrent flushers are safe.
	if s.cfg.GroupConfig != nil {
		s.aggProc = aggregation.New(aggregation.Config{
			Store:      s.store,
			Dispatcher: s.disp,
			Group:      *s.cfg.GroupConfig,
			Logger:     s.logger,
		})
	}

	// Initialize archival cleaner (runs only on leader).
	{
		archCfg := archival.Config{
			Store:  s.store,
			Logger: s.logger,
		}
		if s.cfg.RetentionPeriod != nil {
			archCfg.RetentionPeriod = *s.cfg.RetentionPeriod
		}
		s.archCleaner = archival.New(archCfg)
	}

	// Initialize workflow orchestrator (runs only on leader).
	s.orch = workflow.NewOrchestrator(workflow.OrchestratorConfig{
		Store:      s.store,
		Dispatcher: s.disp,
		Logger:     s.logger,
	})

	// Initialize elector.
	s.elector = election.NewElector(election.ElectorConfig{
		RDB:    s.rdb,
		Prefix: s.store.Prefix(),
		NodeID: s.cfg.NodeID,
		TTL:    s.cfg.ElectionTTL,
		Logger: s.logger,
	})
	s.elector.OnElected = s.onElected
	s.elector.OnDemoted = s.onDemoted

	// Start subsystems.
	if err := s.elector.Start(s.ctx); err != nil {
		return fmt.Errorf("elector start: %w", err)
	}

	// Only start the worker if at least one handler is registered.
	// A handler-less node (e.g., dureqd coordinator) should not consume work.
	if len(s.registry.TaskTypes()) > 0 {
		if err := s.work.Start(s.ctx); err != nil {
			return fmt.Errorf("worker start: %w", err)
		}
	}

	// Start aggregation processor on this node (leader-independent).
	if s.aggProc != nil {
		s.aggProc.Start(s.ctx)
	}

	// Start heartbeat.
	go s.heartbeatLoop()

	s.logger.Info().String("node_id", s.cfg.NodeID).Strs("task_types", s.registry.TaskTypes()).Int("max_concurrency", s.cfg.MaxConcurrency).Msg("server started")

	return nil
}

// Stop gracefully shuts down the server.
// The worker is stopped first to allow graceful drain and requeue of in-flight tasks.
func (s *Server) Stop() error {
	s.logger.Info().String("node_id", s.cfg.NodeID).Msg("server stopping")

	s.work.Stop()
	s.cancel()

	if s.aggProc != nil {
		s.aggProc.Stop()
	}
	s.archCleaner.Stop()
	s.dynCfg.Stop()
	s.orch.Stop()
	s.sched.Stop()
	s.elector.Stop()

	if s.rdb != nil {
		s.rdb.Close()
	}

	s.logger.Info().String("node_id", s.cfg.NodeID).Msg("server stopped")
	return nil
}

// onElected is called when this node wins the leader election.
func (s *Server) onElected() {
	s.logger.Info().Msg("this node is now the leader — starting scheduler, orchestrator, and archival")
	s.sched.Start(s.ctx)
	s.orch.Start(s.ctx)
	s.archCleaner.Start(s.ctx)
}

// onDemoted is called when this node loses leadership.
func (s *Server) onDemoted() {
	s.logger.Info().Msg("this node lost leadership — stopping scheduler, orchestrator, and archival")
	s.sched.Stop()
	s.orch.Stop()
	s.archCleaner.Stop()
}

// heartbeatLoop registers this node and sends periodic heartbeats.
func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		info := &types.NodeInfo{
			NodeID:        s.cfg.NodeID,
			TaskTypes:     s.registry.TaskTypes(),
			StartedAt:     s.startedAt,
			LastHeartbeat: time.Now(),
		}
		if s.work != nil {
			info.PoolStats = s.work.Stats()
			info.ActiveRunIDs = s.work.ActiveRunIDs()
		}
		s.store.SaveNode(s.ctx, info)

		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ClusterTaskTypes returns a set of all task types registered across
// the cluster (from node heartbeats) plus this node's local registry.
func (s *Server) ClusterTaskTypes() map[string]bool {
	tt := make(map[string]bool)

	// Local registry (always available, even before first heartbeat).
	for _, t := range s.registry.TaskTypes() {
		tt[t] = true
	}

	// Other nodes in the cluster via heartbeat data.
	nodes, err := s.store.ListNodes(s.ctx)
	if err != nil {
		return tt
	}
	for _, n := range nodes {
		for _, t := range n.TaskTypes {
			tt[t] = true
		}
	}

	return tt
}

// createRedisClient constructs a rueidis client from the server config.
// Supports standalone, Sentinel failover, and Cluster modes.
func (s *Server) createRedisClient() (rueidis.Client, error) {
	ropts := s.cfg.RedisOptions

	// Sentinel failover mode.
	if ropts.SentinelConfig != nil {
		sc := ropts.SentinelConfig
		opt := rueidis.ClientOption{
			InitAddress: sc.SentinelAddrs,
			Password:    ropts.Password,
			SelectDB:    ropts.DB,
			Sentinel: rueidis.SentinelOption{
				MasterSet: sc.MasterName,
				Password:  sc.SentinelPassword,
			},
			// BlockingPoolSize for XREADGROUP BLOCK commands.
			BlockingPoolSize: max(ropts.PoolSize, 10),
		}
		if ropts.TLSConfig != nil {
			opt.TLSConfig = ropts.TLSConfig
		}
		return rueidis.NewClient(opt)
	}

	// Cluster mode.
	if len(ropts.ClusterAddrs) > 0 {
		opt := rueidis.ClientOption{
			InitAddress:      ropts.ClusterAddrs,
			Password:         ropts.Password,
			BlockingPoolSize: max(ropts.PoolSize, 10),
		}
		if ropts.TLSConfig != nil {
			opt.TLSConfig = ropts.TLSConfig
		}
		return rueidis.NewClient(opt)
	}

	// Standard standalone mode — parse URL.
	u, err := url.Parse(ropts.URL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}
	addr := u.Host
	if addr == "" {
		addr = "localhost:6379"
	}

	opt := rueidis.ClientOption{
		InitAddress:      []string{addr},
		SelectDB:         ropts.DB,
		BlockingPoolSize: max(ropts.PoolSize, 10),
	}

	// Password from URL or option.
	if ropts.Password != "" {
		opt.Password = ropts.Password
	} else if u.User != nil {
		if p, ok := u.User.Password(); ok {
			opt.Password = p
		}
	}

	// TLS: if scheme is "rediss" or TLSConfig is provided.
	if u.Scheme == "rediss" || ropts.TLSConfig != nil {
		if ropts.TLSConfig != nil {
			opt.TLSConfig = ropts.TLSConfig
		} else {
			opt.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
	}

	return rueidis.NewClient(opt)
}

func generateJobID() string {
	return xid.New().String()
}
