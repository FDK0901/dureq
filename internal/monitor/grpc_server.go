package monitor

import (
	"net/http"

	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer implements the MonitorService gRPC service.
type GRPCServer struct {
	pb.UnimplementedMonitorServiceServer
	svc    *APIService
	logger gochainedlog.Logger
}

// NewGRPCServer creates a new gRPC monitor server backed by an APIService.
func NewGRPCServer(svc *APIService) *GRPCServer {
	return &GRPCServer{svc: svc, logger: svc.Logger()}
}

// RegisterServer registers the MonitorService implementation with a gRPC server.
func (g *GRPCServer) RegisterServer(s *grpc.Server) {
	pb.RegisterMonitorServiceServer(s, g)
}

// toGRPCError converts an ApiError (or generic error) to a gRPC status error.
func toGRPCError(err error) error {
	if apiErr, ok := err.(*ApiError); ok {
		code := codes.Internal
		switch apiErr.StatusCode {
		case http.StatusNotFound:
			code = codes.NotFound
		case http.StatusBadRequest:
			code = codes.InvalidArgument
		case http.StatusConflict:
			code = codes.AlreadyExists
		}
		return status.Error(code, apiErr.Msg)
	}
	return status.Error(codes.Internal, err.Error())
}
