package monitor

import (
	"context"

	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
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

	workflows, total, err := g.svc.ListWorkflows(ctx, filter)
	if err != nil {
		return nil, toGRPCError(err)
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
	wf, err := g.svc.GetWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.GetWorkflowResponse{
		Workflow: workflowInstanceToPB(wf),
	}, nil
}

// CancelWorkflow cancels a workflow and all its non-terminal tasks and child jobs.
func (g *GRPCServer) CancelWorkflow(ctx context.Context, req *pb.CancelWorkflowRequest) (*pb.CancelWorkflowResponse, error) {
	if err := g.svc.CancelWorkflow(ctx, req.WorkflowId); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.CancelWorkflowResponse{
		WorkflowId: req.WorkflowId,
	}, nil
}

// RetryWorkflow resets a workflow and all its tasks for a new execution attempt.
func (g *GRPCServer) RetryWorkflow(ctx context.Context, req *pb.RetryWorkflowRequest) (*pb.RetryWorkflowResponse, error) {
	if err := g.svc.RetryWorkflow(ctx, req.WorkflowId); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.RetryWorkflowResponse{
		WorkflowId: req.WorkflowId,
	}, nil
}
