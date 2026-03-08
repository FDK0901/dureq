package monitor

import (
	"context"

	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// ListJobs returns a paginated list of jobs, optionally filtered by status,
// task type, tag, or search term.
func (g *GRPCServer) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	pp := parsePaginationPB(req.GetPagination())

	filter := store.JobFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	if req.Status != nil {
		s := types.JobStatus(*req.Status)
		filter.Status = &s
	}
	if req.TaskType != nil {
		tt := types.TaskType(*req.TaskType)
		filter.TaskType = &tt
	}
	if req.Tag != nil {
		filter.Tag = req.Tag
	}
	if req.Search != nil {
		filter.Search = req.Search
	}

	jobs, total, err := g.svc.ListJobs(ctx, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}

	pbJobs := make([]*pb.Job, 0, len(jobs))
	for i := range jobs {
		pbJobs = append(pbJobs, jobToPB(&jobs[i]))
	}

	return &pb.ListJobsResponse{
		Jobs:  pbJobs,
		Total: int32(total),
	}, nil
}

// GetJob returns a single job by ID.
func (g *GRPCServer) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	job, err := g.svc.GetJob(ctx, req.GetJobId())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.GetJobResponse{Job: jobToPB(job)}, nil
}

// DeleteJob removes a job and its schedule.
func (g *GRPCServer) DeleteJob(ctx context.Context, req *pb.DeleteJobRequest) (*pb.DeleteJobResponse, error) {
	if err := g.svc.DeleteJob(ctx, req.GetJobId()); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.DeleteJobResponse{JobId: req.GetJobId()}, nil
}

// CancelJob transitions a job to cancelled, removes its schedule, publishes
// cancel signals to in-flight workers, and emits a JobCancelled event.
func (g *GRPCServer) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	if err := g.svc.CancelJob(ctx, req.GetJobId()); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.CancelJobResponse{JobId: req.GetJobId()}, nil
}

// RetryJob resets a job to pending, clears its attempt counter and error,
// dispatches it for immediate execution, and emits a JobRetrying event.
func (g *GRPCServer) RetryJob(ctx context.Context, req *pb.RetryJobRequest) (*pb.RetryJobResponse, error) {
	if err := g.svc.RetryJob(ctx, req.GetJobId()); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.RetryJobResponse{JobId: req.GetJobId()}, nil
}

// ListJobEvents returns recent events for a specific job.
func (g *GRPCServer) ListJobEvents(ctx context.Context, req *pb.ListJobEventsRequest) (*pb.ListJobEventsResponse, error) {
	jobID := req.GetJobId()

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}

	events, _, err := g.svc.ListJobEvents(ctx, store.EventFilter{
		JobID: &jobID,
		Limit: limit,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}

	pbEvents := make([]*pb.JobEvent, 0, len(events))
	for i := range events {
		pbEvents = append(pbEvents, jobEventToPB(&events[i]))
	}

	return &pb.ListJobEventsResponse{Events: pbEvents}, nil
}

// ListJobRuns returns recent runs for a specific job.
func (g *GRPCServer) ListJobRuns(ctx context.Context, req *pb.ListJobRunsRequest) (*pb.ListJobRunsResponse, error) {
	jobID := req.GetJobId()

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}

	runs, _, err := g.svc.ListJobRuns(ctx, store.RunFilter{
		JobID: &jobID,
		Limit: limit,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}

	pbRuns := make([]*pb.JobRun, 0, len(runs))
	for i := range runs {
		pbRuns = append(pbRuns, jobRunToPB(&runs[i]))
	}

	return &pb.ListJobRunsResponse{Runs: pbRuns}, nil
}

// UpdateJobPayload replaces the payload of a pending or scheduled job.
func (g *GRPCServer) UpdateJobPayload(ctx context.Context, req *pb.UpdateJobPayloadRequest) (*pb.UpdateJobPayloadResponse, error) {
	if err := g.svc.UpdateJobPayload(ctx, req.GetJobId(), req.GetPayload()); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.UpdateJobPayloadResponse{JobId: req.GetJobId()}, nil
}
