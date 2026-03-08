package monitor

import (
	"context"

	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
)

// --- Bulk Job Operations ---

// BulkCancelJobs cancels multiple jobs by ID or status filter.
func (g *GRPCServer) BulkCancelJobs(ctx context.Context, req *pb.BulkCancelJobsRequest) (*pb.BulkCancelJobsResponse, error) {
	result, err := g.svc.BulkCancelJobs(ctx, BulkRequest{IDs: req.GetIds(), Status: req.GetStatus()})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.BulkCancelJobsResponse{Affected: int32(result.Affected)}, nil
}

// BulkRetryJobs retries multiple jobs by ID or status filter.
func (g *GRPCServer) BulkRetryJobs(ctx context.Context, req *pb.BulkRetryJobsRequest) (*pb.BulkRetryJobsResponse, error) {
	result, err := g.svc.BulkRetryJobs(ctx, BulkRequest{IDs: req.GetIds(), Status: req.GetStatus()})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.BulkRetryJobsResponse{Affected: int32(result.Affected)}, nil
}

// BulkDeleteJobs deletes multiple jobs by ID or status filter.
func (g *GRPCServer) BulkDeleteJobs(ctx context.Context, req *pb.BulkDeleteJobsRequest) (*pb.BulkDeleteJobsResponse, error) {
	result, err := g.svc.BulkDeleteJobs(ctx, BulkRequest{IDs: req.GetIds(), Status: req.GetStatus()})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.BulkDeleteJobsResponse{Affected: int32(result.Affected)}, nil
}

// --- Bulk Workflow Operations ---

// BulkCancelWorkflows cancels multiple workflows by ID or status filter.
func (g *GRPCServer) BulkCancelWorkflows(ctx context.Context, req *pb.BulkCancelWorkflowsRequest) (*pb.BulkCancelWorkflowsResponse, error) {
	result, err := g.svc.BulkCancelWorkflows(ctx, BulkRequest{IDs: req.GetIds(), Status: req.GetStatus()})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.BulkCancelWorkflowsResponse{Affected: int32(result.Affected)}, nil
}

// BulkRetryWorkflows retries multiple workflows by ID or status filter.
func (g *GRPCServer) BulkRetryWorkflows(ctx context.Context, req *pb.BulkRetryWorkflowsRequest) (*pb.BulkRetryWorkflowsResponse, error) {
	result, err := g.svc.BulkRetryWorkflows(ctx, BulkRequest{IDs: req.GetIds(), Status: req.GetStatus()})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.BulkRetryWorkflowsResponse{Affected: int32(result.Affected)}, nil
}

// BulkDeleteWorkflows deletes multiple workflows by ID or status filter.
func (g *GRPCServer) BulkDeleteWorkflows(ctx context.Context, req *pb.BulkDeleteWorkflowsRequest) (*pb.BulkDeleteWorkflowsResponse, error) {
	result, err := g.svc.BulkDeleteWorkflows(ctx, BulkRequest{IDs: req.GetIds(), Status: req.GetStatus()})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.BulkDeleteWorkflowsResponse{Affected: int32(result.Affected)}, nil
}

// --- Bulk Batch Operations ---

// BulkCancelBatches cancels multiple batches by ID or status filter.
func (g *GRPCServer) BulkCancelBatches(ctx context.Context, req *pb.BulkCancelBatchesRequest) (*pb.BulkCancelBatchesResponse, error) {
	result, err := g.svc.BulkCancelBatches(ctx, BulkRequest{IDs: req.GetIds(), Status: req.GetStatus()})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.BulkCancelBatchesResponse{Affected: int32(result.Affected)}, nil
}

// BulkRetryBatches retries multiple batches by ID or status filter.
func (g *GRPCServer) BulkRetryBatches(ctx context.Context, req *pb.BulkRetryBatchesRequest) (*pb.BulkRetryBatchesResponse, error) {
	result, err := g.svc.BulkRetryBatches(ctx, BulkRequest{IDs: req.GetIds(), Status: req.GetStatus()})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.BulkRetryBatchesResponse{Affected: int32(result.Affected)}, nil
}

// BulkDeleteBatches deletes multiple batches by ID or status filter.
func (g *GRPCServer) BulkDeleteBatches(ctx context.Context, req *pb.BulkDeleteBatchesRequest) (*pb.BulkDeleteBatchesResponse, error) {
	result, err := g.svc.BulkDeleteBatches(ctx, BulkRequest{IDs: req.GetIds(), Status: req.GetStatus()})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.BulkDeleteBatchesResponse{Affected: int32(result.Affected)}, nil
}
