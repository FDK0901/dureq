package monitor

import (
	"context"

	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// ListBatches returns paginated batch instances with optional status filter.
func (g *GRPCServer) ListBatches(ctx context.Context, req *pb.ListBatchesRequest) (*pb.ListBatchesResponse, error) {
	pp := parsePaginationPB(req.Pagination)

	filter := store.BatchFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	if req.Status != nil {
		s := types.WorkflowStatus(*req.Status)
		filter.Status = &s
	}

	batches, total, err := g.svc.ListBatches(ctx, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.BatchInstance, len(batches))
	for i := range batches {
		out[i] = batchInstanceToPB(&batches[i])
	}

	return &pb.ListBatchesResponse{
		Batches: out,
		Total:   int32(total),
	}, nil
}

// GetBatch retrieves a single batch instance by ID.
func (g *GRPCServer) GetBatch(ctx context.Context, req *pb.GetBatchRequest) (*pb.GetBatchResponse, error) {
	batch, err := g.svc.GetBatch(ctx, req.BatchId)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.GetBatchResponse{
		Batch: batchInstanceToPB(batch),
	}, nil
}

// GetBatchResults returns all item results for a batch.
func (g *GRPCServer) GetBatchResults(ctx context.Context, req *pb.GetBatchResultsRequest) (*pb.GetBatchResultsResponse, error) {
	results, err := g.svc.GetBatchResults(ctx, req.BatchId)
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.BatchItemResult, len(results))
	for i, r := range results {
		out[i] = batchItemResultToPB(r)
	}

	return &pb.GetBatchResultsResponse{
		Results: out,
	}, nil
}

// GetBatchItemResult retrieves a single batch item result.
func (g *GRPCServer) GetBatchItemResult(ctx context.Context, req *pb.GetBatchItemResultRequest) (*pb.GetBatchItemResultResponse, error) {
	result, err := g.svc.GetBatchItemResult(ctx, req.BatchId, req.ItemId)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.GetBatchItemResultResponse{
		Result: batchItemResultToPB(result),
	}, nil
}

// CancelBatch cancels a batch and all its non-terminal items and child jobs.
func (g *GRPCServer) CancelBatch(ctx context.Context, req *pb.CancelBatchRequest) (*pb.CancelBatchResponse, error) {
	if err := g.svc.CancelBatch(ctx, req.BatchId); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.CancelBatchResponse{
		BatchId: req.BatchId,
	}, nil
}

// RetryBatch resets a batch for a new execution attempt, optionally retrying only failed items.
func (g *GRPCServer) RetryBatch(ctx context.Context, req *pb.RetryBatchRequest) (*pb.RetryBatchResponse, error) {
	if err := g.svc.RetryBatch(ctx, req.BatchId, req.RetryFailedOnly); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.RetryBatchResponse{
		BatchId: req.BatchId,
	}, nil
}
