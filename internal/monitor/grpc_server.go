package monitor

import (
	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
	"github.com/FDK0901/dureq/internal/store"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"google.golang.org/grpc"
)

// GRPCServer implements the MonitorService gRPC service.
type GRPCServer struct {
	pb.UnimplementedMonitorServiceServer
	store      *store.RedisStore
	dispatcher Dispatcher
	logger     gochainedlog.Logger

	syncRetryStats SyncRetryStatsFunc
}

// NewGRPCServer creates a new gRPC monitor server backed by Redis.
func NewGRPCServer(s *store.RedisStore, disp Dispatcher, logger gochainedlog.Logger) *GRPCServer {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return &GRPCServer{store: s, dispatcher: disp, logger: logger}
}

// SetSyncRetryStatsFunc sets the function used to retrieve SyncRetrier stats.
func (g *GRPCServer) SetSyncRetryStatsFunc(fn SyncRetryStatsFunc) {
	g.syncRetryStats = fn
}

// RegisterServer registers the MonitorService implementation with a gRPC server.
func (g *GRPCServer) RegisterServer(s *grpc.Server) {
	pb.RegisterMonitorServiceServer(s, g)
}
