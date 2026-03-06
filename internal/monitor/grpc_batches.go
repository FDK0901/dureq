package monitor

import (
	"context"
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	batches, total, err := g.store.ListBatchInstances(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list batches: %v", err)
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
	batch, _, err := g.store.GetBatch(ctx, req.BatchId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "batch not found: %v", err)
	}

	return &pb.GetBatchResponse{
		Batch: batchInstanceToPB(batch),
	}, nil
}

// GetBatchResults returns all item results for a batch.
func (g *GRPCServer) GetBatchResults(ctx context.Context, req *pb.GetBatchResultsRequest) (*pb.GetBatchResultsResponse, error) {
	results, err := g.store.ListBatchItemResults(ctx, req.BatchId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list batch results: %v", err)
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
	result, err := g.store.GetBatchItemResult(ctx, req.BatchId, req.ItemId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get batch item result: %v", err)
	}
	if result == nil {
		return nil, status.Errorf(codes.NotFound, "batch item result not found")
	}

	return &pb.GetBatchItemResultResponse{
		Result: batchItemResultToPB(result),
	}, nil
}

// CancelBatch cancels a batch and all its non-terminal items and child jobs.
func (g *GRPCServer) CancelBatch(ctx context.Context, req *pb.CancelBatchRequest) (*pb.CancelBatchResponse, error) {
	batch, rev, err := g.store.GetBatch(ctx, req.BatchId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "batch not found: %v", err)
	}
	if batch.Status.IsTerminal() {
		return nil, status.Errorf(codes.FailedPrecondition, "batch is already in terminal state")
	}

	now := time.Now()
	batch.Status = types.WorkflowStatusCancelled
	batch.CompletedAt = &now
	batch.UpdatedAt = now

	// Cancel in-flight child jobs.
	for id, state := range batch.ItemStates {
		if !state.Status.IsTerminal() {
			state.Status = types.JobStatusCancelled
			state.FinishedAt = &now
			batch.ItemStates[id] = state

			if state.JobID != "" {
				if childJob, childRev, err := g.store.GetJob(ctx, state.JobID); err == nil {
					if !childJob.Status.IsTerminal() {
						childJob.Status = types.JobStatusCancelled
						childJob.UpdatedAt = now
						g.store.UpdateJob(ctx, childJob, childRev)
						g.store.SignalCancelActiveRuns(ctx, state.JobID)
					}
				}
			}
		}
	}

	if _, err := g.store.UpdateBatch(ctx, batch, rev); err != nil {
		return nil, status.Errorf(codes.Internal, "update batch: %v", err)
	}

	g.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventBatchCancelled,
		JobID:     req.BatchId,
		Timestamp: now,
	})

	return &pb.CancelBatchResponse{
		BatchId: req.BatchId,
	}, nil
}

// RetryBatch resets a batch for a new execution attempt, optionally retrying only failed items.
func (g *GRPCServer) RetryBatch(ctx context.Context, req *pb.RetryBatchRequest) (*pb.RetryBatchResponse, error) {
	batch, rev, err := g.store.GetBatch(ctx, req.BatchId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "batch not found: %v", err)
	}
	if batch.Status == types.WorkflowStatusRunning {
		return nil, status.Errorf(codes.FailedPrecondition, "batch is still running; cancel it first")
	}

	retryFailedOnly := req.RetryFailedOnly

	now := time.Now()

	// Reset counters.
	batch.FailedItems = 0
	batch.RunningItems = 0
	batch.PendingItems = 0
	batch.CompletedItems = 0

	for id, state := range batch.ItemStates {
		// Cancel in-flight child jobs from the previous attempt.
		if state.JobID != "" && !state.Status.IsTerminal() {
			if childJob, childRev, err := g.store.GetJob(ctx, state.JobID); err == nil {
				if !childJob.Status.IsTerminal() {
					childJob.Status = types.JobStatusCancelled
					childJob.UpdatedAt = now
					g.store.UpdateJob(ctx, childJob, childRev)
					g.store.SignalCancelActiveRuns(ctx, state.JobID)
				}
			}
		}

		if retryFailedOnly && state.Status != types.JobStatusFailed {
			if state.Status == types.JobStatusCompleted {
				batch.CompletedItems++
			}
			continue
		}
		state.Status = types.JobStatusPending
		state.JobID = ""
		state.Error = nil
		state.StartedAt = nil
		state.FinishedAt = nil
		batch.ItemStates[id] = state
		batch.PendingItems++
	}

	batch.Attempt++
	batch.Status = types.WorkflowStatusRunning
	batch.UpdatedAt = now
	batch.CompletedAt = nil
	batch.NextChunkIndex = 0

	if _, err := g.store.UpdateBatch(ctx, batch, rev); err != nil {
		return nil, status.Errorf(codes.Internal, "update batch: %v", err)
	}

	g.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventBatchRetrying,
		JobID:     req.BatchId,
		Attempt:   batch.Attempt,
		Timestamp: now,
	})

	return &pb.RetryBatchResponse{
		BatchId: req.BatchId,
	}, nil
}
