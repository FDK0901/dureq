package monitor

import (
	"context"
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
	"github.com/FDK0901/dureq/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- Bulk Job Operations ---

// BulkCancelJobs cancels multiple jobs by ID or status filter.
func (g *GRPCServer) BulkCancelJobs(ctx context.Context, req *pb.BulkCancelJobsRequest) (*pb.BulkCancelJobsResponse, error) {
	ids := req.GetIds()
	if req.GetStatus() != "" && len(ids) == 0 {
		jobs, _, err := g.store.ListJobsPaginated(ctx, jobFilterByStatus(req.GetStatus(), maxPageLimit))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list jobs by status: %v", err)
		}
		for _, j := range jobs {
			ids = append(ids, j.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	var affected int32
	now := time.Now()
	for _, id := range ids {
		job, rev, err := g.store.GetJob(ctx, id)
		if err != nil || job.Status.IsTerminal() {
			continue
		}
		job.Status = types.JobStatusCancelled
		job.UpdatedAt = now
		if _, err := g.store.UpdateJob(ctx, job, rev); err == nil {
			g.store.DeleteSchedule(ctx, id)
			g.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventJobCancelled, JobID: id, Timestamp: now,
			})
			affected++
		}
	}
	return &pb.BulkCancelJobsResponse{Affected: affected}, nil
}

// BulkRetryJobs retries multiple jobs by ID or status filter.
func (g *GRPCServer) BulkRetryJobs(ctx context.Context, req *pb.BulkRetryJobsRequest) (*pb.BulkRetryJobsResponse, error) {
	ids := req.GetIds()
	if req.GetStatus() != "" && len(ids) == 0 {
		jobs, _, err := g.store.ListJobsPaginated(ctx, jobFilterByStatus(req.GetStatus(), maxPageLimit))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list jobs by status: %v", err)
		}
		for _, j := range jobs {
			ids = append(ids, j.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	var affected int32
	now := time.Now()
	for _, id := range ids {
		job, rev, err := g.store.GetJob(ctx, id)
		if err != nil {
			continue
		}
		job.Status = types.JobStatusPending
		job.Attempt = 0
		job.LastError = nil
		job.UpdatedAt = now
		if _, err := g.store.UpdateJob(ctx, job, rev); err != nil {
			continue
		}
		if err := g.dispatcher.Dispatch(ctx, job, 0); err != nil {
			continue
		}
		g.dispatcher.PublishEvent(types.JobEvent{
			Type: types.EventJobRetrying, JobID: id, Timestamp: now,
		})
		affected++
	}
	return &pb.BulkRetryJobsResponse{Affected: affected}, nil
}

// BulkDeleteJobs deletes multiple jobs by ID or status filter.
func (g *GRPCServer) BulkDeleteJobs(ctx context.Context, req *pb.BulkDeleteJobsRequest) (*pb.BulkDeleteJobsResponse, error) {
	ids := req.GetIds()
	if req.GetStatus() != "" && len(ids) == 0 {
		jobs, _, err := g.store.ListJobsPaginated(ctx, jobFilterByStatus(req.GetStatus(), maxPageLimit))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list jobs by status: %v", err)
		}
		for _, j := range jobs {
			ids = append(ids, j.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	var affected int32
	for _, id := range ids {
		if err := g.store.DeleteJob(ctx, id); err == nil {
			g.store.DeleteSchedule(ctx, id)
			affected++
		}
	}
	return &pb.BulkDeleteJobsResponse{Affected: affected}, nil
}

// --- Bulk Workflow Operations ---

// BulkCancelWorkflows cancels multiple workflows by ID or status filter.
func (g *GRPCServer) BulkCancelWorkflows(ctx context.Context, req *pb.BulkCancelWorkflowsRequest) (*pb.BulkCancelWorkflowsResponse, error) {
	ids := req.GetIds()
	if req.GetStatus() != "" && len(ids) == 0 {
		s := types.WorkflowStatus(req.GetStatus())
		wfs, _, err := g.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&s, maxPageLimit))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list workflows by status: %v", err)
		}
		for _, wf := range wfs {
			ids = append(ids, wf.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	var affected int32
	now := time.Now()
	for _, id := range ids {
		wf, rev, err := g.store.GetWorkflow(ctx, id)
		if err != nil || wf.Status.IsTerminal() {
			continue
		}
		wf.Status = types.WorkflowStatusCancelled
		wf.CompletedAt = &now
		wf.UpdatedAt = now
		for name, state := range wf.Tasks {
			if !state.Status.IsTerminal() {
				state.Status = types.JobStatusCancelled
				state.FinishedAt = &now
				wf.Tasks[name] = state

				if state.JobID != "" {
					if childJob, childRev, err := g.store.GetJob(ctx, state.JobID); err == nil {
						if !childJob.Status.IsTerminal() {
							childJob.Status = types.JobStatusCancelled
							childJob.UpdatedAt = now
							g.store.UpdateJob(ctx, childJob, childRev)
						}
					}
				}
			}
		}
		if _, err := g.store.UpdateWorkflow(ctx, wf, rev); err == nil {
			g.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventWorkflowCancelled, JobID: id, Timestamp: now,
			})
			affected++
		}
	}
	return &pb.BulkCancelWorkflowsResponse{Affected: affected}, nil
}

// BulkRetryWorkflows retries multiple workflows by ID or status filter.
func (g *GRPCServer) BulkRetryWorkflows(ctx context.Context, req *pb.BulkRetryWorkflowsRequest) (*pb.BulkRetryWorkflowsResponse, error) {
	ids := req.GetIds()
	if req.GetStatus() != "" && len(ids) == 0 {
		s := types.WorkflowStatus(req.GetStatus())
		wfs, _, err := g.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&s, maxPageLimit))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list workflows by status: %v", err)
		}
		for _, wf := range wfs {
			ids = append(ids, wf.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	var affected int32
	now := time.Now()
	for _, id := range ids {
		wf, rev, err := g.store.GetWorkflow(ctx, id)
		if err != nil {
			continue
		}
		wf.Attempt++
		wf.Status = types.WorkflowStatusRunning
		wf.UpdatedAt = now
		wf.CompletedAt = nil
		for name, state := range wf.Tasks {
			state.Status = types.JobStatusPending
			state.JobID = ""
			state.Error = nil
			state.StartedAt = nil
			state.FinishedAt = nil
			wf.Tasks[name] = state
		}
		if _, err := g.store.UpdateWorkflow(ctx, wf, rev); err == nil {
			g.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventWorkflowRetrying, JobID: id, Attempt: wf.Attempt, Timestamp: now,
			})
			affected++
		}
	}
	return &pb.BulkRetryWorkflowsResponse{Affected: affected}, nil
}

// BulkDeleteWorkflows deletes multiple workflows by ID or status filter.
func (g *GRPCServer) BulkDeleteWorkflows(ctx context.Context, req *pb.BulkDeleteWorkflowsRequest) (*pb.BulkDeleteWorkflowsResponse, error) {
	ids := req.GetIds()
	if req.GetStatus() != "" && len(ids) == 0 {
		s := types.WorkflowStatus(req.GetStatus())
		wfs, _, err := g.store.ListWorkflowInstances(ctx, workflowFilterByStatus(&s, maxPageLimit))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list workflows by status: %v", err)
		}
		for _, wf := range wfs {
			ids = append(ids, wf.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	var affected int32
	for _, id := range ids {
		if err := g.store.DeleteWorkflow(ctx, id); err == nil {
			affected++
		}
	}
	return &pb.BulkDeleteWorkflowsResponse{Affected: affected}, nil
}

// --- Bulk Batch Operations ---

// BulkCancelBatches cancels multiple batches by ID or status filter.
func (g *GRPCServer) BulkCancelBatches(ctx context.Context, req *pb.BulkCancelBatchesRequest) (*pb.BulkCancelBatchesResponse, error) {
	ids := req.GetIds()
	if req.GetStatus() != "" && len(ids) == 0 {
		s := types.WorkflowStatus(req.GetStatus())
		batches, _, err := g.store.ListBatchInstances(ctx, batchFilterByStatus(&s, maxPageLimit))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list batches by status: %v", err)
		}
		for _, b := range batches {
			ids = append(ids, b.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	var affected int32
	now := time.Now()
	for _, id := range ids {
		batch, rev, err := g.store.GetBatch(ctx, id)
		if err != nil || batch.Status.IsTerminal() {
			continue
		}
		batch.Status = types.WorkflowStatusCancelled
		batch.CompletedAt = &now
		batch.UpdatedAt = now

		// Cancel in-flight child jobs.
		for itemID, state := range batch.ItemStates {
			if !state.Status.IsTerminal() {
				state.Status = types.JobStatusCancelled
				state.FinishedAt = &now
				batch.ItemStates[itemID] = state

				if state.JobID != "" {
					if childJob, childRev, err := g.store.GetJob(ctx, state.JobID); err == nil {
						if !childJob.Status.IsTerminal() {
							childJob.Status = types.JobStatusCancelled
							childJob.UpdatedAt = now
							g.store.UpdateJob(ctx, childJob, childRev)
						}
					}
				}
			}
		}

		if _, err := g.store.UpdateBatch(ctx, batch, rev); err == nil {
			g.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventBatchCancelled, JobID: id, Timestamp: now,
			})
			affected++
		}
	}
	return &pb.BulkCancelBatchesResponse{Affected: affected}, nil
}

// BulkRetryBatches retries multiple batches by ID or status filter.
func (g *GRPCServer) BulkRetryBatches(ctx context.Context, req *pb.BulkRetryBatchesRequest) (*pb.BulkRetryBatchesResponse, error) {
	ids := req.GetIds()
	if req.GetStatus() != "" && len(ids) == 0 {
		s := types.WorkflowStatus(req.GetStatus())
		batches, _, err := g.store.ListBatchInstances(ctx, batchFilterByStatus(&s, maxPageLimit))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list batches by status: %v", err)
		}
		for _, b := range batches {
			ids = append(ids, b.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	var affected int32
	now := time.Now()
	for _, id := range ids {
		batch, rev, err := g.store.GetBatch(ctx, id)
		if err != nil {
			continue
		}
		batch.Attempt++
		batch.Status = types.WorkflowStatusRunning
		batch.UpdatedAt = now
		batch.CompletedAt = nil
		batch.FailedItems = 0
		batch.RunningItems = 0
		batch.PendingItems = batch.TotalItems
		batch.CompletedItems = 0
		for itemID, state := range batch.ItemStates {
			state.Status = types.JobStatusPending
			state.JobID = ""
			state.Error = nil
			state.StartedAt = nil
			state.FinishedAt = nil
			batch.ItemStates[itemID] = state
		}
		batch.NextChunkIndex = 0
		if _, err := g.store.UpdateBatch(ctx, batch, rev); err == nil {
			g.dispatcher.PublishEvent(types.JobEvent{
				Type: types.EventBatchRetrying, JobID: id, Attempt: batch.Attempt, Timestamp: now,
			})
			affected++
		}
	}
	return &pb.BulkRetryBatchesResponse{Affected: affected}, nil
}

// BulkDeleteBatches deletes multiple batches by ID or status filter.
func (g *GRPCServer) BulkDeleteBatches(ctx context.Context, req *pb.BulkDeleteBatchesRequest) (*pb.BulkDeleteBatchesResponse, error) {
	ids := req.GetIds()
	if req.GetStatus() != "" && len(ids) == 0 {
		s := types.WorkflowStatus(req.GetStatus())
		batches, _, err := g.store.ListBatchInstances(ctx, batchFilterByStatus(&s, maxPageLimit))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list batches by status: %v", err)
		}
		for _, b := range batches {
			ids = append(ids, b.ID)
		}
	}
	if len(ids) > maxPageLimit {
		ids = ids[:maxPageLimit]
	}

	var affected int32
	for _, id := range ids {
		if err := g.store.DeleteBatch(ctx, id); err == nil {
			affected++
		}
	}
	return &pb.BulkDeleteBatchesResponse{Affected: affected}, nil
}
