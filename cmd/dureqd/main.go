package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/FDK0901/dureq/cmd/dureqd/config"
	"github.com/FDK0901/dureq/cmd/dureqd/handlers"
	"github.com/FDK0901/dureq/internal/monitor"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/go-chainedlog/impl/chainedzerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	zl := chainedzerolog.NewZerologBase()
	logger := chainedzerolog.NewZerolog(zl)

	cfg, err := config.GetConfig()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to get config")
	}

	opts := []server.Option{
		server.WithLogger(logger),
	}
	if len(cfg.RedisConfig.ClusterAddrs) > 0 {
		opts = append(opts, server.WithRedisCluster(cfg.RedisConfig.ClusterAddrs))
	} else if cfg.RedisConfig.URL != "" {
		opts = append(opts, server.WithRedisURL(cfg.RedisConfig.URL))
		if cfg.RedisConfig.DB != 0 {
			opts = append(opts, server.WithRedisDB(cfg.RedisConfig.DB))
		}
	}
	if cfg.RedisConfig.Username != "" {
		opts = append(opts, server.WithRedisUsername(cfg.RedisConfig.Username))
	}
	if cfg.RedisConfig.Password != "" {
		opts = append(opts, server.WithRedisPassword(cfg.RedisConfig.Password))
	}
	if cfg.RedisConfig.PoolSize > 0 {
		opts = append(opts, server.WithRedisPoolSize(cfg.RedisConfig.PoolSize))
	}

	if cfg.DureqdConfig.NodeID != "" {
		opts = append(opts, server.WithNodeID(cfg.DureqdConfig.NodeID))
	}
	if cfg.DureqdConfig.Concurrency != 0 {
		opts = append(opts, server.WithMaxConcurrency(cfg.DureqdConfig.Concurrency))
	}
	if cfg.DureqdConfig.Prefix != "" {
		opts = append(opts, server.WithKeyPrefix(cfg.DureqdConfig.Prefix))
	}
	if m := parseMode(cfg.DureqdConfig.Mode); m != 0 {
		opts = append(opts, server.WithMode(m))
	}
	apiAddr := ":8080"
	if cfg.DureqdConfig.ApiAddress != "" {
		apiAddr = cfg.DureqdConfig.ApiAddress
	}
	grpcAddr := ":9090"
	if cfg.DureqdConfig.GrpcAddress != "" {
		grpcAddr = cfg.DureqdConfig.GrpcAddress
	}

	srv, err := server.New(opts...)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create server")
	}

	// Register built-in handlers from the static registry.
	var enabledHandlers map[string]bool
	if len(cfg.DureqdConfig.Handlers) > 0 {
		enabledHandlers = make(map[string]bool, len(cfg.DureqdConfig.Handlers))
		for _, h := range cfg.DureqdConfig.Handlers {
			enabledHandlers[h] = true
		}
	}
	registered, skipped := handlers.RegisterFiltered(srv, enabledHandlers)
	if skipped > 0 {
		logger.Info().Int("skipped", skipped).Int("registered", registered).Msg("some handlers skipped (filtered or failed)")
	}
	logger.Info().Int("count", registered).Strs("available", handlers.List()).Msg("registered built-in handlers")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("failed to start server")
	}

	// Track readiness — set after server + APIs are fully initialized.
	var ready atomic.Bool

	// Start monitoring API — HTTP (wired to server's store and dispatcher).
	api := monitor.NewAPI(srv.Store(), srv.Dispatcher(), logger)
	if w := srv.Worker(); w != nil {
		api.SetSyncRetryStatsFunc(func() any { return w.SyncerStats() })
	}

	mux := http.NewServeMux()

	// Health check: returns 200 if Redis is reachable.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, pingCancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer pingCancel()

		if err := srv.Store().Ping(pingCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	// Readiness check: returns 200 when server is started, handlers registered, and Redis reachable.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "not ready"})
			return
		}
		// Verify Redis is still reachable (lightweight ping).
		pingCtx, pingCancel := context.WithTimeout(r.Context(), 1*time.Second)
		defer pingCancel()
		if err := srv.Store().Ping(pingCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "reason": "redis unreachable"})
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"status":   "ready",
			"handlers": registered,
		})
	})

	// All other routes go to the monitoring API.
	mux.Handle("/", api.Handler())

	httpSrv := &http.Server{Addr: apiAddr, Handler: mux}
	go func() {
		logger.Info().String("addr", apiAddr).Msg("monitoring HTTP API listening")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("HTTP API server error")
		}
	}()

	// Start monitoring API — gRPC. Listen synchronously before spawning Serve
	// goroutine so that readiness is not set until the port is bound.
	grpcSrv := grpc.NewServer()
	grpcMonitor := monitor.NewGRPCServer(api.Service())
	grpcMonitor.RegisterServer(grpcSrv)
	reflection.Register(grpcSrv)

	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to listen for gRPC")
	}
	go func() {
		logger.Info().String("addr", grpcAddr).Msg("monitoring gRPC API listening")
		if err := grpcSrv.Serve(grpcLis); err != nil {
			logger.Fatal().Err(err).Msg("gRPC server error")
		}
	}()

	// Mark as ready after all components are up and listening.
	ready.Store(true)
	logger.Info().Msg("dureqd ready")

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	fmt.Println() // newline after ^C
	logger.Info().String("signal", sig.String()).Msg("received signal, shutting down")

	// Mark not ready immediately to fail readiness probes during drain.
	ready.Store(false)

	// Graceful drain: allow in-flight jobs to complete within the timeout.
	drainTimeout := 30 * time.Second
	if cfg.DureqdConfig.DrainTimeoutMs > 0 {
		drainTimeout = time.Duration(cfg.DureqdConfig.DrainTimeoutMs) * time.Millisecond
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
	defer drainCancel()

	logger.Info().Int("timeout_ms", int(drainTimeout.Milliseconds())).Msg("draining in-flight jobs")

	// Stop accepting new work first, then drain.
	grpcSrv.GracefulStop()
	httpSrv.Shutdown(drainCtx)
	srv.Stop()

	logger.Info().Msg("dureqd shutdown complete")
}

// parseMode converts a config string like "queue,scheduler" into a server.Mode bitmask.
// Returns 0 (caller uses default ModeFull) for empty or unrecognized input.
func parseMode(s string) server.Mode {
	if s == "" {
		return 0
	}

	modeMap := map[string]server.Mode{
		"full":      server.ModeFull,
		"queue":     server.ModeQueue,
		"scheduler": server.ModeScheduler,
		"workflow":  server.ModeWorkflow,
		"monitor":   server.ModeMonitor,
	}

	if m, ok := modeMap[strings.ToLower(strings.TrimSpace(s))]; ok {
		return m
	}

	// Comma-separated combination: "queue,scheduler"
	var combined server.Mode
	for _, part := range strings.Split(s, ",") {
		if m, ok := modeMap[strings.ToLower(strings.TrimSpace(part))]; ok {
			combined |= m
		}
	}
	return combined
}
