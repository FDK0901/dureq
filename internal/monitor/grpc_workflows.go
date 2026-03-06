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

// ListWorkflows returns paginated workflow instances with optional status filter.
func (g *GRPCServer) ListWorkflows(ctx context.Context, req *pb.ListWorkflowsRequest) (*pb.ListWorkflowsResponse, error) {
	pp := parsePaginationPB(req.Pagination)

	filter := store.WorkflowFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	if req.Status != nil {
		s := types.WorkflowStatus(*req.Status)
		filter.Status = &s
	}

	workflows, total, err := g.store.ListWorkflowInstances(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list workflows: %v", err)
	}

	out := make([]*pb.WorkflowInstance, len(workflows))
	for i := range workflows {
		out[i] = workflowInstanceToPB(&workflows[i])
	}

	return &pb.ListWorkflowsResponse{
		Workflows: out,
		Total:     int32(total),
	}, nil
}

// GetWorkflow retrieves a single workflow instance by ID.
func (g *GRPCServer) GetWorkflow(ctx context.Context, req *pb.GetWorkflowRequest) (*pb.GetWorkflowResponse, error) {
	wf, _, err := g.store.GetWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "workflow not found: %v", err)
	}

	return &pb.GetWorkflowResponse{
		Workflow: workflowInstanceToPB(wf),
	}, nil
}

// CancelWorkflow cancels a workflow and all its non-terminal tasks and child jobs.
func (g *GRPCServer) CancelWorkflow(ctx context.Context, req *pb.CancelWorkflowRequest) (*pb.CancelWorkflowResponse, error) {
	wf, rev, err := g.store.GetWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "workflow not found: %v", err)
	}
	if wf.Status.IsTerminal() {
		return nil, status.Errorf(codes.FailedPrecondition, "workflow is already in terminal state")
	}

	now := time.Now()
	wf.Status = types.WorkflowStatusCancelled
	wf.CompletedAt = &now
	wf.UpdatedAt = now

	// Cancel non-terminal tasks.
	for name, state := range wf.Tasks {
		if !state.Status.IsTerminal() {
			state.Status = types.JobStatusCancelled
			state.FinishedAt = &now
			wf.Tasks[name] = state

			// Cancel the child job if it exists.
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

	if _, err := g.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		return nil, status.Errorf(codes.Internal, "update workflow: %v", err)
	}

	g.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowCancelled,
		JobID:     req.WorkflowId,
		Timestamp: now,
	})

	return &pb.CancelWorkflowResponse{
		WorkflowId: req.WorkflowId,
	}, nil
}

// RetryWorkflow resets a workflow and all its tasks for a new execution attempt.
func (g *GRPCServer) RetryWorkflow(ctx context.Context, req *pb.RetryWorkflowRequest) (*pb.RetryWorkflowResponse, error) {
	wf, rev, err := g.store.GetWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "workflow not found: %v", err)
	}
	if wf.Status == types.WorkflowStatusRunning {
		return nil, status.Errorf(codes.FailedPrecondition, "workflow is still running; cancel it first")
	}

	now := time.Now()

	// Cancel in-flight child jobs from the previous attempt, then reset.
	for name, state := range wf.Tasks {
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
		state.Status = types.JobStatusPending
		state.JobID = ""
		state.Error = nil
		state.StartedAt = nil
		state.FinishedAt = nil
		wf.Tasks[name] = state
	}

	wf.Attempt++
	wf.Status = types.WorkflowStatusRunning
	wf.UpdatedAt = now
	wf.CompletedAt = nil

	if _, err := g.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		return nil, status.Errorf(codes.Internal, "update workflow: %v", err)
	}

	g.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventWorkflowRetrying,
		JobID:     req.WorkflowId,
		Attempt:   wf.Attempt,
		Timestamp: now,
	})

	return &pb.RetryWorkflowResponse{
		WorkflowId: req.WorkflowId,
	}, nil
}
