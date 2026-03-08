package monitor

import (
	"context"
	"encoding/json"

	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// --- Schedules ---

// ListSchedules returns a paginated list of schedule entries.
func (g *GRPCServer) ListSchedules(ctx context.Context, req *pb.ListSchedulesRequest) (*pb.ListSchedulesResponse, error) {
	pp := parsePaginationPB(req.GetPagination())

	filter := store.ScheduleFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	schedules, total, err := g.svc.ListSchedules(ctx, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.ScheduleEntry, len(schedules))
	for i := range schedules {
		out[i] = scheduleEntryToPB(&schedules[i])
	}

	return &pb.ListSchedulesResponse{
		Schedules: out,
		Total:     int32(total),
	}, nil
}

// GetSchedule returns a single schedule entry by job ID.
func (g *GRPCServer) GetSchedule(ctx context.Context, req *pb.GetScheduleRequest) (*pb.GetScheduleResponse, error) {
	sched, err := g.svc.GetSchedule(ctx, req.GetJobId())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.GetScheduleResponse{
		Schedule: scheduleEntryToPB(sched),
	}, nil
}

// --- Nodes ---

// ListNodes returns all registered server nodes.
func (g *GRPCServer) ListNodes(ctx context.Context, _ *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	nodes, err := g.svc.ListNodes(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.NodeInfo, len(nodes))
	for i, n := range nodes {
		out[i] = nodeInfoToPB(n)
	}

	return &pb.ListNodesResponse{Nodes: out}, nil
}

// GetNode returns a single node by ID.
func (g *GRPCServer) GetNode(ctx context.Context, req *pb.GetNodeRequest) (*pb.GetNodeResponse, error) {
	node, err := g.svc.GetNode(ctx, req.GetNodeId())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.GetNodeResponse{Node: nodeInfoToPB(node)}, nil
}

// --- Runs ---

// ListRuns returns all active runs.
func (g *GRPCServer) ListRuns(ctx context.Context, _ *pb.ListRunsRequest) (*pb.ListRunsResponse, error) {
	runs, err := g.svc.ListActiveRuns(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.JobRun, len(runs))
	for i, r := range runs {
		out[i] = jobRunToPB(r)
	}

	return &pb.ListRunsResponse{Runs: out}, nil
}

// GetRun returns a single run by ID.
func (g *GRPCServer) GetRun(ctx context.Context, req *pb.GetRunRequest) (*pb.GetRunResponse, error) {
	run, err := g.svc.GetRun(ctx, req.GetRunId())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.GetRunResponse{Run: jobRunToPB(run)}, nil
}

// --- History ---

// ListHistoryRuns returns paginated historical runs with optional status and job_id filters.
func (g *GRPCServer) ListHistoryRuns(ctx context.Context, req *pb.ListHistoryRunsRequest) (*pb.ListHistoryRunsResponse, error) {
	pp := parsePaginationPB(req.GetPagination())

	filter := store.RunFilter{
		Sort:   pp.Sort,
		Limit:  pp.Limit,
		Offset: pp.Offset,
	}
	if s := req.GetStatus(); s != "" {
		rs := types.RunStatus(s)
		filter.Status = &rs
	}
	if jid := req.GetJobId(); jid != "" {
		filter.JobID = &jid
	}

	runs, total, err := g.svc.ListHistoryRuns(ctx, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.JobRun, len(runs))
	for i := range runs {
		out[i] = jobRunToPB(&runs[i])
	}

	return &pb.ListHistoryRunsResponse{
		Runs:  out,
		Total: int32(total),
	}, nil
}

// ListHistoryEvents returns paginated historical events with optional job_id filter.
func (g *GRPCServer) ListHistoryEvents(ctx context.Context, req *pb.ListHistoryEventsRequest) (*pb.ListHistoryEventsResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}

	offset := int(req.GetOffset())
	if offset < 0 {
		offset = 0
	}

	filter := store.EventFilter{Limit: limit, Offset: offset}
	if jid := req.GetJobId(); jid != "" {
		filter.JobID = &jid
	}

	events, total, err := g.svc.ListHistoryEvents(ctx, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.JobEvent, len(events))
	for i := range events {
		out[i] = jobEventToPB(&events[i])
	}

	return &pb.ListHistoryEventsResponse{
		Events: out,
		Total:  int32(total),
	}, nil
}

// --- DLQ ---

// ListDLQ returns paginated dead-letter queue messages.
func (g *GRPCServer) ListDLQ(ctx context.Context, req *pb.ListDLQRequest) (*pb.ListDLQResponse, error) {
	pp := parsePaginationPB(req.GetPagination())

	msgs, total, err := g.svc.ListDLQ(ctx, pp.Limit, pp.Offset)
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.WorkMessage, len(msgs))
	for i, m := range msgs {
		out[i] = workMessageToPB(m)
	}

	return &pb.ListDLQResponse{
		Messages: out,
		Total:    int32(total),
	}, nil
}

// --- Groups ---

// ListGroups returns all active aggregation groups with their sizes.
func (g *GRPCServer) ListGroups(ctx context.Context, _ *pb.ListGroupsRequest) (*pb.ListGroupsResponse, error) {
	groups, err := g.svc.ListGroups(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.GroupInfo, 0, len(groups))
	for _, gi := range groups {
		out = append(out, &pb.GroupInfo{Name: gi.Name, Size: gi.Size})
	}

	return &pb.ListGroupsResponse{Groups: out}, nil
}

// --- Queues ---

// ListQueues returns queue/tier information including pause state and size.
func (g *GRPCServer) ListQueues(ctx context.Context, _ *pb.ListQueuesRequest) (*pb.ListQueuesResponse, error) {
	queues, err := g.svc.ListQueues(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.QueueInfo, 0, len(queues))
	for _, q := range queues {
		out = append(out, &pb.QueueInfo{
			Name:       q.Name,
			Weight:     int32(q.Weight),
			FetchBatch: int32(q.FetchBatch),
			Paused:     q.Paused,
			Size:       q.Size,
		})
	}

	return &pb.ListQueuesResponse{Queues: out}, nil
}

// PauseQueue pauses a queue/tier by name.
func (g *GRPCServer) PauseQueue(ctx context.Context, req *pb.PauseQueueRequest) (*pb.PauseQueueResponse, error) {
	if err := g.svc.PauseQueue(ctx, req.GetTierName()); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.PauseQueueResponse{TierName: req.GetTierName()}, nil
}

// ResumeQueue resumes a paused queue/tier by name.
func (g *GRPCServer) ResumeQueue(ctx context.Context, req *pb.ResumeQueueRequest) (*pb.ResumeQueueResponse, error) {
	if err := g.svc.ResumeQueue(ctx, req.GetTierName()); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.ResumeQueueResponse{TierName: req.GetTierName()}, nil
}

// --- Stats ---

// GetStats returns overall system statistics including job counts, active schedules,
// runs, and nodes.
func (g *GRPCServer) GetStats(ctx context.Context, _ *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	stats, err := g.svc.GetStats(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}

	counts := make(map[string]int32, len(stats.JobCounts))
	for s, c := range stats.JobCounts {
		counts[string(s)] = int32(c)
	}

	return &pb.GetStatsResponse{
		JobCounts:       counts,
		ActiveSchedules: int32(stats.ActiveSchedules),
		ActiveRuns:      int32(stats.ActiveRuns),
		ActiveNodes:     int32(stats.ActiveNodes),
	}, nil
}

// GetDailyStats returns aggregated daily processing statistics.
func (g *GRPCServer) GetDailyStats(ctx context.Context, req *pb.GetDailyStatsRequest) (*pb.GetDailyStatsResponse, error) {
	entries, err := g.svc.GetDailyStats(ctx, int(req.GetDays()))
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make([]*pb.DailyStatsEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &pb.DailyStatsEntry{
			Date:      e.Date,
			Processed: e.Processed,
			Failed:    e.Failed,
		})
	}

	return &pb.GetDailyStatsResponse{Entries: out}, nil
}

// GetRedisInfo returns parsed Redis INFO output organized by section.
func (g *GRPCServer) GetRedisInfo(ctx context.Context, req *pb.GetRedisInfoRequest) (*pb.GetRedisInfoResponse, error) {
	sections, err := g.svc.GetRedisInfo(ctx, req.GetSection())
	if err != nil {
		return nil, toGRPCError(err)
	}

	out := make(map[string]*pb.RedisInfoSection, len(sections))
	for name, entries := range sections {
		out[name] = &pb.RedisInfoSection{Entries: entries}
	}

	return &pb.GetRedisInfoResponse{Sections: out}, nil
}

// GetSyncRetries returns the current SyncRetrier statistics as an opaque JSON blob.
func (g *GRPCServer) GetSyncRetries(_ context.Context, _ *pb.GetSyncRetriesRequest) (*pb.GetSyncRetriesResponse, error) {
	result := g.svc.GetSyncRetries()
	data, err := json.Marshal(result)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.GetSyncRetriesResponse{Data: data}, nil
}

// --- Health ---

// Health returns a simple health check response.
func (g *GRPCServer) Health(_ context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Status: "ok"}, nil
}
