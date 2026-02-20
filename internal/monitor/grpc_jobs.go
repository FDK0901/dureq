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

	jobs, total, err := g.store.ListJobsPaginated(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list jobs: %v", err)
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
	job, _, err := g.store.GetJob(ctx, req.GetJobId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job not found: %v", err)
	}
	return &pb.GetJobResponse{Job: jobToPB(job)}, nil
}

// DeleteJob removes a job and its schedule.
func (g *GRPCServer) DeleteJob(ctx context.Context, req *pb.DeleteJobRequest) (*pb.DeleteJobResponse, error) {
	if err := g.store.DeleteJob(ctx, req.GetJobId()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete job: %v", err)
	}
	g.store.DeleteSchedule(ctx, req.GetJobId())

	return &pb.DeleteJobResponse{JobId: req.GetJobId()}, nil
}

// CancelJob transitions a job to cancelled, removes its schedule, publishes
// cancel signals to in-flight workers, and emits a JobCancelled event.
func (g *GRPCServer) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	jobID := req.GetJobId()

	job, rev, err := g.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job not found: %v", err)
	}
	if job.Status.IsTerminal() {
		return nil, status.Error(codes.FailedPrecondition, "job is already in terminal state")
	}

	now := time.Now()
	job.Status = types.JobStatusCancelled
	job.UpdatedAt = now

	if _, err := g.store.UpdateJob(ctx, job, rev); err != nil {
		return nil, status.Errorf(codes.Internal, "update job: %v", err)
	}

	g.store.DeleteSchedule(ctx, jobID)

	// Publish cancel signals to in-flight workers via Redis Pub/Sub.
	runs, _ := g.store.ListActiveRunsByJobID(ctx, jobID)
	for _, run := range runs {
		g.store.Client().Do(ctx, g.store.Client().B().Publish().Channel(g.store.CancelChannel()).Message(run.ID).Build())
	}

	g.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventJobCancelled,
		JobID:     jobID,
		Timestamp: now,
	})

	return &pb.CancelJobResponse{JobId: jobID}, nil
}

// RetryJob resets a job to pending, clears its attempt counter and error,
// dispatches it for immediate execution, and emits a JobRetrying event.
func (g *GRPCServer) RetryJob(ctx context.Context, req *pb.RetryJobRequest) (*pb.RetryJobResponse, error) {
	jobID := req.GetJobId()

	job, rev, err := g.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job not found: %v", err)
	}

	now := time.Now()
	job.Status = types.JobStatusPending
	job.Attempt = 0
	job.LastError = nil
	job.UpdatedAt = now

	if _, err := g.store.UpdateJob(ctx, job, rev); err != nil {
		return nil, status.Errorf(codes.Internal, "update job: %v", err)
	}

	if err := g.dispatcher.Dispatch(ctx, job, 0); err != nil {
		return nil, status.Errorf(codes.Internal, "dispatch job: %v", err)
	}

	g.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventJobRetrying,
		JobID:     jobID,
		Timestamp: now,
	})

	return &pb.RetryJobResponse{JobId: jobID}, nil
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

	events, _, err := g.store.ListJobEvents(ctx, store.EventFilter{
		JobID: &jobID,
		Limit: limit,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list job events: %v", err)
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

	runs, _, err := g.store.ListJobRuns(ctx, store.RunFilter{
		JobID: &jobID,
		Limit: limit,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list job runs: %v", err)
	}

	pbRuns := make([]*pb.JobRun, 0, len(runs))
	for i := range runs {
		pbRuns = append(pbRuns, jobRunToPB(&runs[i]))
	}

	return &pb.ListJobRunsResponse{Runs: pbRuns}, nil
}

// UpdateJobPayload replaces the payload of a pending or scheduled job.
func (g *GRPCServer) UpdateJobPayload(ctx context.Context, req *pb.UpdateJobPayloadRequest) (*pb.UpdateJobPayloadResponse, error) {
	if len(req.GetPayload()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "payload is required")
	}

	jobID := req.GetJobId()

	job, rev, err := g.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job not found: %v", err)
	}

	// Only allow payload update for non-running, non-terminal jobs.
	if job.Status.IsTerminal() || job.Status == types.JobStatusRunning {
		return nil, status.Error(codes.FailedPrecondition, "can only update payload of pending or scheduled jobs")
	}

	job.Payload = req.GetPayload()
	job.UpdatedAt = time.Now()

	if _, err := g.store.UpdateJob(ctx, job, rev); err != nil {
		return nil, status.Errorf(codes.Internal, "update job: %v", err)
	}

	return &pb.UpdateJobPayloadResponse{JobId: jobID}, nil
}
