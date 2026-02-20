package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/FDK0901/dureq/cmd/dureqd/config"
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
	if cfg.RedisConfig.URL != "" {
		opts = append(opts, server.WithRedisURL(cfg.RedisConfig.URL))
	}
	if cfg.RedisConfig.Password != "" {
		opts = append(opts, server.WithRedisPassword(cfg.RedisConfig.Password))
	}
	if cfg.RedisConfig.DB != 0 {
		opts = append(opts, server.WithRedisDB(cfg.RedisConfig.DB))
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

	// TODO: Register handlers from a plugin/config system.
	// For now, this is a skeleton. Users would import this and add:
	//   srv.RegisterHandler(types.HandlerDefinition{...})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("failed to start server")
	}

	// Start monitoring API — HTTP (wired to server's store and dispatcher).
	api := monitor.NewAPI(srv.Store(), srv.Dispatcher(), logger)
	if w := srv.Worker(); w != nil {
		api.SetSyncRetryStatsFunc(func() any { return w.SyncerStats() })
	}
	httpSrv := &http.Server{Addr: apiAddr, Handler: api.Handler()}
	go func() {
		logger.Info().String("addr", apiAddr).Msg("monitoring HTTP API listening")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("HTTP API server error")
		}
	}()

	// Start monitoring API — gRPC.
	grpcSrv := grpc.NewServer()
	grpcMonitor := monitor.NewGRPCServer(srv.Store(), srv.Dispatcher(), logger)
	if w := srv.Worker(); w != nil {
		grpcMonitor.SetSyncRetryStatsFunc(func() any { return w.SyncerStats() })
	}
	grpcMonitor.RegisterServer(grpcSrv)
	reflection.Register(grpcSrv)
	go func() {
		lis, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to listen for gRPC")
		}
		logger.Info().String("addr", grpcAddr).Msg("monitoring gRPC API listening")
		if err := grpcSrv.Serve(lis); err != nil {
			logger.Fatal().Err(err).Msg("gRPC server error")
		}
	}()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	fmt.Println() // newline after ^C
	logger.Info().String("signal", sig.String()).Msg("received signal, shutting down")

	grpcSrv.GracefulStop()
	api.Shutdown()
	httpSrv.Close()
	srv.Stop()
}
