package monitor

import (
	"context"
	"encoding/json"

	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	schedules, total, err := g.store.ListSchedulesPaginated(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list schedules: %v", err)
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
	sched, _, err := g.store.GetSchedule(ctx, req.GetJobId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "schedule not found: %v", err)
	}
	return &pb.GetScheduleResponse{
		Schedule: scheduleEntryToPB(sched),
	}, nil
}

// --- Nodes ---

// ListNodes returns all registered server nodes.
func (g *GRPCServer) ListNodes(ctx context.Context, _ *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	nodes, err := g.store.ListNodes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list nodes: %v", err)
	}

	out := make([]*pb.NodeInfo, len(nodes))
	for i, n := range nodes {
		out[i] = nodeInfoToPB(n)
	}

	return &pb.ListNodesResponse{Nodes: out}, nil
}

// GetNode returns a single node by ID.
func (g *GRPCServer) GetNode(ctx context.Context, req *pb.GetNodeRequest) (*pb.GetNodeResponse, error) {
	node, err := g.store.GetNode(ctx, req.GetNodeId())
	if err != nil || node == nil {
		return nil, status.Errorf(codes.NotFound, "node not found")
	}
	return &pb.GetNodeResponse{Node: nodeInfoToPB(node)}, nil
}

// --- Runs ---

// ListRuns returns all active runs.
func (g *GRPCServer) ListRuns(ctx context.Context, _ *pb.ListRunsRequest) (*pb.ListRunsResponse, error) {
	runs, err := g.store.ListRuns(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list runs: %v", err)
	}

	out := make([]*pb.JobRun, len(runs))
	for i, r := range runs {
		out[i] = jobRunToPB(r)
	}

	return &pb.ListRunsResponse{Runs: out}, nil
}

// GetRun returns a single run by ID.
func (g *GRPCServer) GetRun(ctx context.Context, req *pb.GetRunRequest) (*pb.GetRunResponse, error) {
	run, _, err := g.store.GetRun(ctx, req.GetRunId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "run not found: %v", err)
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

	runs, total, err := g.store.ListJobRuns(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list history runs: %v", err)
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

	events, total, err := g.store.ListJobEvents(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list history events: %v", err)
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

	msgs, err := g.store.ListDLQ(ctx, 1000)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list dlq: %v", err)
	}

	total := len(msgs)
	if pp.Offset < len(msgs) {
		msgs = msgs[pp.Offset:]
	} else {
		msgs = nil
	}
	if pp.Limit > 0 && len(msgs) > pp.Limit {
		msgs = msgs[:pp.Limit]
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
	groups, err := g.store.ListGroups(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list groups: %v", err)
	}

	out := make([]*pb.GroupInfo, 0, len(groups))
	for _, name := range groups {
		size, err := g.store.GetGroupSize(ctx, name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get group size for %q: %v", name, err)
		}
		out = append(out, &pb.GroupInfo{Name: name, Size: size})
	}

	return &pb.ListGroupsResponse{Groups: out}, nil
}

// --- Queues ---

// ListQueues returns queue/tier information including pause state and size.
func (g *GRPCServer) ListQueues(ctx context.Context, _ *pb.ListQueuesRequest) (*pb.ListQueuesResponse, error) {
	tiers := g.store.Config().Tiers
	paused, err := g.store.ListPausedQueues(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list paused queues: %v", err)
	}
	pausedSet := make(map[string]bool, len(paused))
	for _, p := range paused {
		pausedSet[p] = true
	}

	// Use pending job count instead of legacy XLEN (streams are unused in v2 actor path).
	jobCounts, err := g.store.GetActiveJobStats(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job stats for queue size: %v", err)
	}
	pendingCount := int64(jobCounts[types.JobStatusPending])

	queues := make([]*pb.QueueInfo, 0, len(tiers))
	for _, tier := range tiers {
		queues = append(queues, &pb.QueueInfo{
			Name:       tier.Name,
			Weight:     int32(tier.Weight),
			FetchBatch: int32(tier.FetchBatch),
			Paused:     pausedSet[tier.Name],
			Size:       pendingCount,
		})
	}

	return &pb.ListQueuesResponse{Queues: queues}, nil
}

// PauseQueue pauses a queue/tier by name.
func (g *GRPCServer) PauseQueue(ctx context.Context, req *pb.PauseQueueRequest) (*pb.PauseQueueResponse, error) {
	if err := g.store.PauseQueue(ctx, req.GetTierName()); err != nil {
		return nil, status.Errorf(codes.Internal, "pause queue: %v", err)
	}
	return &pb.PauseQueueResponse{TierName: req.GetTierName()}, nil
}

// ResumeQueue resumes a paused queue/tier by name.
func (g *GRPCServer) ResumeQueue(ctx context.Context, req *pb.ResumeQueueRequest) (*pb.ResumeQueueResponse, error) {
	if err := g.store.UnpauseQueue(ctx, req.GetTierName()); err != nil {
		return nil, status.Errorf(codes.Internal, "resume queue: %v", err)
	}
	return &pb.ResumeQueueResponse{TierName: req.GetTierName()}, nil
}

// --- Stats ---

// GetStats returns overall system statistics including job counts, active schedules,
// runs, and nodes.
func (g *GRPCServer) GetStats(ctx context.Context, _ *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	jobCounts, err := g.store.GetActiveJobStats(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job stats: %v", err)
	}

	schedules, err := g.store.ListSchedules(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list schedules: %v", err)
	}
	runs, err := g.store.ListRuns(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list runs: %v", err)
	}
	nodes, err := g.store.ListNodes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list nodes: %v", err)
	}

	counts := make(map[string]int32, len(jobCounts))
	for s, c := range jobCounts {
		counts[string(s)] = int32(c)
	}

	return &pb.GetStatsResponse{
		JobCounts:       counts,
		ActiveSchedules: int32(len(schedules)),
		ActiveRuns:      int32(len(runs)),
		ActiveNodes:     int32(len(nodes)),
	}, nil
}

// GetDailyStats returns aggregated daily processing statistics.
func (g *GRPCServer) GetDailyStats(ctx context.Context, req *pb.GetDailyStatsRequest) (*pb.GetDailyStatsResponse, error) {
	days := int(req.GetDays())
	if days <= 0 {
		days = 7
	}
	if days > 90 {
		days = 90
	}

	entries, err := g.store.GetDailyStats(ctx, days)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get daily stats: %v", err)
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
	section := req.GetSection()
	if section == "" {
		section = "all"
	}

	info, err := g.store.Client().Do(ctx, g.store.Client().B().Info().Section(section).Build()).ToString()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get redis info: %v", err)
	}

	sections := store.ParseRedisInfo(info)
	out := make(map[string]*pb.RedisInfoSection, len(sections))
	for name, entries := range sections {
		out[name] = &pb.RedisInfoSection{Entries: entries}
	}

	return &pb.GetRedisInfoResponse{Sections: out}, nil
}

// GetSyncRetries returns the current SyncRetrier statistics as an opaque JSON blob.
func (g *GRPCServer) GetSyncRetries(_ context.Context, _ *pb.GetSyncRetriesRequest) (*pb.GetSyncRetriesResponse, error) {
	if g.syncRetryStats == nil {
		return &pb.GetSyncRetriesResponse{Data: []byte("{}")}, nil
	}

	result := g.syncRetryStats()
	data, err := json.Marshal(result)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal sync retries: %v", err)
	}
	return &pb.GetSyncRetriesResponse{Data: data}, nil
}

// --- Health ---

// Health returns a simple health check response.
func (g *GRPCServer) Health(_ context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Status: "ok"}, nil
}
