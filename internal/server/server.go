package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq"
	"github.com/FDK0901/dureq/internal/actors"
	"github.com/FDK0901/dureq/internal/lock"
	"github.com/FDK0901/dureq/internal/messages"
	"github.com/FDK0901/dureq/internal/monitor"
	"github.com/FDK0901/dureq/internal/provider"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/internal/handler"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/anthdm/hollywood/actor"
	"github.com/anthdm/hollywood/cluster"
	"github.com/anthdm/hollywood/remote"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// Server is the dureq server node built on the Hollywood actor framework.
// It replaces the v1 pull/polling model with a push-based actor model.
type Server struct {
	cfg      Config
	rdb      rueidis.Client
	store    *store.RedisStore
	locker   *lock.Locker
	disp     *actorDispatcher
	registry *handler.Registry

	engine  *actor.Engine
	rem     *remote.Remote
	cluster *cluster.Cluster

	// Local actor PIDs (per node).
	workerSupervisorPID *actor.PID
	heartbeatPID        *actor.PID
	eventBridgePID      *actor.PID
	notifierPID         *actor.PID
	clusterGuardPID     *actor.PID

	// Singleton PIDs (cluster-wide, resolved after activation).
	schedulerPID    *actor.PID
	orchestratorPID *actor.PID
	dispatcherPID   *actor.PID

	middlewares []types.MiddlewareFunc

	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a new dureq server with the given options.
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

	if cfg.NodeID == "" {
		cfg.NodeID = xid.New().String()
	}
	if cfg.Logger == nil {
		cfg.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}

	return &Server{
		cfg:      cfg,
		registry: handler.NewRegistry(),
	}, nil
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

// Store returns the Redis store. Available after Start().
func (s *Server) Store() *store.RedisStore { return s.store }

// RedisClient returns the Redis client. Available after Start().
func (s *Server) RedisClient() rueidis.Client { return s.rdb }

// Dispatcher returns the actor-model Dispatcher that satisfies monitor.Dispatcher.
// Available after Start().
func (s *Server) Dispatcher() monitor.Dispatcher { return s.disp }

// Start connects to Redis, initializes the actor system, and begins operation.
func (s *Server) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// 1. Create Redis client.
	rdb, err := s.createRedisClient()
	if err != nil {
		return fmt.Errorf("redis connect: %w", err)
	}
	s.rdb = rdb

	if err := s.rdb.Do(s.ctx, s.rdb.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}

	// 2. Initialize Redis store.
	s.store, err = store.NewRedisStore(s.rdb, s.cfg.StoreConfig, nil)
	if err != nil {
		return fmt.Errorf("redis store: %w", err)
	}

	if err := s.store.EnsureStreams(s.ctx); err != nil {
		return fmt.Errorf("ensure streams: %w", err)
	}

	// 2b. Create per-run locker.
	s.locker = lock.NewLocker(lock.LockerConfig{
		RDB:                s.rdb,
		Prefix:             s.store.Prefix(),
		LockTTL:            s.cfg.LockTTL,
		AutoExtendInterval: s.cfg.LockAutoExtend,
		Logger:             s.cfg.Logger,
	})

	// 2c. Create actor dispatcher (satisfies monitor.Dispatcher).
	s.disp = &actorDispatcher{store: s.store, logger: s.cfg.Logger}

	// 3. Set up Hollywood Remote + Engine.
	listenAddr := s.cfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:0"
	}
	s.rem = remote.New(listenAddr, remote.NewConfig())
	s.engine, err = actor.NewEngine(actor.NewEngineConfig().WithRemote(s.rem))
	if err != nil {
		return fmt.Errorf("actor engine: %w", err)
	}

	// 4. Create cluster with Redis provider.
	clusterConfig := cluster.NewConfig().
		WithEngine(s.engine).
		WithID(s.cfg.NodeID).
		WithRegion(s.cfg.ClusterRegion).
		WithProvider(provider.NewRedisProvider(provider.RedisProviderConfig{
			RedisClient: s.rdb,
			KeyPrefix:   s.store.Prefix(),
		}))

	s.cluster, err = cluster.New(clusterConfig)
	if err != nil {
		return fmt.Errorf("cluster: %w", err)
	}

	// 5. Register singleton kinds (must be before cluster.Start).
	s.cluster.RegisterKind(actors.KindScheduler,
		actors.NewSchedulerActor(s.store, s.cluster, s.cfg.Logger),
		cluster.NewKindConfig())

	s.cluster.RegisterKind(actors.KindOrchestrator,
		actors.NewOrchestratorActor(s.store, s.cluster, s.cfg.Logger),
		cluster.NewKindConfig())

	s.cluster.RegisterKind(actors.KindDispatcher,
		actors.NewDispatcherActor(s.store, s.cfg.Logger),
		cluster.NewKindConfig())

	// 6. Start the cluster.
	s.cluster.Start()

	// 7. Spawn local actors.
	s.spawnLocalActors()

	// 7b. Start cancel bridge: Redis Pub/Sub → actor CancelRunMsg.
	go s.cancelSubscribeLoop()

	// 7c. Start delayed retry poller (actor path — re-notifies instead of XADD).
	delayedPoller := store.NewDelayedNotifyPoller(s.store, s.cfg.Logger)
	go delayedPoller.Start(s.ctx)

	// 8. Activate singletons via ClusterGuard.
	s.clusterGuardPID = s.engine.Spawn(
		actors.NewClusterGuardActor(s.cluster, s.onSingletonsReady, s.cfg.Logger),
		"cluster_guard",
		actor.WithID(s.cfg.NodeID),
	)

	s.cfg.Logger.Info().String("node_id", s.cfg.NodeID).String("listen_addr", s.engine.Address()).Int("max_concurrency", s.cfg.MaxConcurrency).Strs("task_types", s.registry.TaskTypes()).Msg("dureq server started")

	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() error {
	s.cfg.Logger.Info().String("node_id", s.cfg.NodeID).Msg("dureq server stopping")

	// 1. Stop accepting new jobs via notifier.
	if s.notifierPID != nil {
		<-s.engine.Poison(s.notifierPID).Done()
	}

	// 2. Drain the worker supervisor.
	if s.workerSupervisorPID != nil {
		s.engine.Send(s.workerSupervisorPID, messages.DrainMsg{})

		// Give in-flight workers a grace period, then poison.
		done := s.engine.Poison(s.workerSupervisorPID).Done()
		select {
		case <-done:
		case <-time.After(s.cfg.ShutdownTimeout):
			s.cfg.Logger.Warn().Msg("shutdown timeout waiting for worker supervisor drain")
		}
	}

	// 3. Stop remaining local actors.
	if s.heartbeatPID != nil {
		<-s.engine.Poison(s.heartbeatPID).Done()
	}
	if s.eventBridgePID != nil {
		<-s.engine.Poison(s.eventBridgePID).Done()
	}
	if s.clusterGuardPID != nil {
		<-s.engine.Poison(s.clusterGuardPID).Done()
	}

	// 4. Stop cluster (triggers singleton migration).
	if s.cluster != nil {
		s.cluster.Stop()
	}

	// 5. Cancel context and close Redis.
	s.cancel()
	if s.rdb != nil {
		s.rdb.Close()
	}

	s.cfg.Logger.Info().String("node_id", s.cfg.NodeID).Msg("dureq server stopped")
	return nil
}

// ---------------------------------------------------------------------------
// Internal setup
// ---------------------------------------------------------------------------

func (s *Server) spawnLocalActors() {
	// WorkerSupervisorActor.
	wsProducer := actors.NewWorkerSupervisorActor(
		s.cfg.NodeID, s.store, s.registry, s.cfg.MaxConcurrency,
		s.locker, s.cfg.Logger,
	)
	s.workerSupervisorPID = s.engine.Spawn(wsProducer, "worker_supervisor",
		actor.WithID(s.cfg.NodeID))

	// HeartbeatActor.
	s.heartbeatPID = s.engine.Spawn(
		actors.NewHeartbeatActor(s.cfg.NodeID, s.store, s.registry.TaskTypes(), s.cfg.Logger),
		"heartbeat",
		actor.WithID(s.cfg.NodeID),
	)

	// EventBridgeActor.
	ebProducer := actors.NewEventBridgeActor(s.cfg.NodeID, s.store, s.cfg.Logger)
	s.eventBridgePID = s.engine.Spawn(ebProducer, "event_bridge",
		actor.WithID(s.cfg.NodeID))

	// NotifierActor.
	notifyChannel := store.JobNotifyChannel(s.store.Prefix())
	notifierProducer := actors.NewNotifierActor(s.rdb, notifyChannel, s.cfg.Logger)
	s.notifierPID = s.engine.Spawn(notifierProducer, "notifier",
		actor.WithID(s.cfg.NodeID))

	// Wire EventBridge PID to WorkerSupervisor (both are local, available now).
	s.engine.Send(s.workerSupervisorPID, messages.WireEventBridgePIDMsg{PID: s.eventBridgePID})
}

// onSingletonsReady is called by the ClusterGuardActor after singleton
// activation. It wires the singleton PIDs into local actors.
func (s *Server) onSingletonsReady(scheduler, orchestrator, dispatcher *actor.PID) {
	s.schedulerPID = scheduler
	s.orchestratorPID = orchestrator
	s.dispatcherPID = dispatcher

	// Wire dispatcher PID to local actors via messages.
	if s.notifierPID != nil && dispatcher != nil {
		s.engine.Send(s.notifierPID, messages.WireDispatcherPIDMsg{PID: dispatcher})
	}
	if s.workerSupervisorPID != nil && dispatcher != nil {
		s.engine.Send(s.workerSupervisorPID, messages.WireDispatcherPIDMsg{PID: dispatcher})
	}
	if s.eventBridgePID != nil && orchestrator != nil {
		s.engine.Send(s.eventBridgePID, messages.WireOrchestratorPIDMsg{PID: orchestrator})
	}

	// Wire orchestrator PID to actorDispatcher so API-triggered events
	// (batch retry, workflow retry) reach the orchestrator singleton.
	if orchestrator != nil {
		s.disp.SetOrchestratorPID(s.engine, orchestrator)
	}

	s.cfg.Logger.Info().String("scheduler", scheduler.String()).String("orchestrator", orchestrator.String()).String("dispatcher", dispatcher.String()).Msg("singletons ready")
}

// ---------------------------------------------------------------------------
// Redis client creation
// ---------------------------------------------------------------------------

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

// cancelSubscribeLoop bridges Redis Pub/Sub cancel signals to actor CancelRunMsg.
// The monitor API publishes run IDs to the cancel channel; this goroutine
// forwards them to the WorkerSupervisorActor as CancelRunMsg.
func (s *Server) cancelSubscribeLoop() {
	channel := store.CancelChannelKey(s.store.Prefix())
	err := s.rdb.Receive(s.ctx, s.rdb.B().Subscribe().Channel(channel).Build(),
		func(msg rueidis.PubSubMessage) {
			runID := msg.Message
			if s.workerSupervisorPID != nil {
				s.engine.Send(s.workerSupervisorPID, &pb.CancelRunMsg{RunId: runID})
			}
		})
	if err != nil && s.ctx.Err() == nil {
		s.cfg.Logger.Warn().Err(err).Msg("cancel subscribe loop ended with error")
	}
}
