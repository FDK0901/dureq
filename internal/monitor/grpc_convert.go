package monitor

import (
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq/monitor/v1"
	"github.com/FDK0901/dureq/pkg/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- Timestamp / Duration helpers ---

func timeToTSPB(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func optTimeToTSPB(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timeToTSPB(*t)
}

func optDurationToPB(d *types.Duration) *durationpb.Duration {
	if d == nil {
		return nil
	}
	return durationpb.New(d.Std())
}

// --- RetryPolicy ---

func retryPolicyToPB(rp *types.RetryPolicy) *pb.RetryPolicy {
	if rp == nil {
		return nil
	}
	return &pb.RetryPolicy{
		MaxAttempts:  int32(rp.MaxAttempts),
		InitialDelay: durationpb.New(rp.InitialDelay),
		MaxDelay:     durationpb.New(rp.MaxDelay),
		Multiplier:   rp.Multiplier,
		Jitter:       rp.Jitter,
	}
}

// --- Schedule ---

func scheduleToPB(s *types.Schedule) *pb.Schedule {
	if s == nil {
		return nil
	}

	out := &pb.Schedule{
		Type:     string(s.Type),
		RunAt:    optTimeToTSPB(s.RunAt),
		Interval: optDurationToPB(s.Interval),
		Timezone: s.Timezone,
		StartsAt: optTimeToTSPB(s.StartsAt),
		EndsAt:   optTimeToTSPB(s.EndsAt),
	}

	if s.CronExpr != nil {
		out.CronExpr = proto.String(*s.CronExpr)
	}

	if s.RegularInterval != nil {
		out.RegularInterval = proto.Uint32(uint32(*s.RegularInterval))
	}

	if len(s.AtTimes) > 0 {
		out.AtTimes = make([]*pb.AtTime, len(s.AtTimes))
		for i, at := range s.AtTimes {
			out.AtTimes[i] = &pb.AtTime{
				Hour:   uint32(at.Hour),
				Minute: uint32(at.Minute),
				Second: uint32(at.Second),
			}
		}
	}

	if len(s.IncludedDays) > 0 {
		out.IncludedDays = make([]int32, len(s.IncludedDays))
		for i, d := range s.IncludedDays {
			out.IncludedDays[i] = int32(d)
		}
	}

	return out
}

// --- Job ---

func jobToPB(j *types.Job) *pb.Job {
	if j == nil {
		return nil
	}

	out := &pb.Job{
		Id:          j.ID,
		TaskType:    string(j.TaskType),
		Payload:     []byte(j.Payload),
		Schedule:    scheduleToPB(&j.Schedule),
		RetryPolicy: retryPolicyToPB(j.RetryPolicy),
		Status:      string(j.Status),
		Attempt:     int32(j.Attempt),
		Tags:        j.Tags,
		Headers:     j.Headers,
		CreatedAt:   timeToTSPB(j.CreatedAt),
		UpdatedAt:   timeToTSPB(j.UpdatedAt),
		LastRunAt:   optTimeToTSPB(j.LastRunAt),
		NextRunAt:   optTimeToTSPB(j.NextRunAt),
		CompletedAt: optTimeToTSPB(j.CompletedAt),
	}

	if j.LastError != nil {
		out.LastError = proto.String(*j.LastError)
	}
	if j.UniqueKey != nil {
		out.UniqueKey = proto.String(*j.UniqueKey)
	}
	if j.DLQAfter != nil {
		out.DlqAfter = proto.Int32(int32(*j.DLQAfter))
	}
	if j.WorkflowID != nil {
		out.WorkflowId = proto.String(*j.WorkflowID)
	}
	if j.WorkflowTask != nil {
		out.WorkflowTask = proto.String(*j.WorkflowTask)
	}
	if j.BatchID != nil {
		out.BatchId = proto.String(*j.BatchID)
	}
	if j.BatchItem != nil {
		out.BatchItem = proto.String(*j.BatchItem)
	}
	if j.BatchRole != nil {
		out.BatchRole = proto.String(*j.BatchRole)
	}
	if j.Priority != nil {
		out.Priority = proto.Int32(int32(int(*j.Priority)))
	}
	if j.ScheduleToStartTimeout != nil {
		out.ScheduleToStartTimeout = durationpb.New(j.ScheduleToStartTimeout.Std())
	}

	return out
}

// --- JobRun ---

func jobRunToPB(r *types.JobRun) *pb.JobRun {
	if r == nil {
		return nil
	}

	out := &pb.JobRun{
		Id:              r.ID,
		JobId:           r.JobID,
		NodeId:          r.NodeID,
		Status:          string(r.Status),
		Attempt:         int32(r.Attempt),
		StartedAt:       timeToTSPB(r.StartedAt),
		FinishedAt:      optTimeToTSPB(r.FinishedAt),
		Duration:        durationpb.New(r.Duration),
		LastHeartbeatAt: optTimeToTSPB(r.LastHeartbeatAt),
	}

	if r.Error != nil {
		out.Error = proto.String(*r.Error)
	}

	return out
}

// --- BatchProgress ---

func batchProgressToPB(bp *types.BatchProgress) *pb.BatchProgress {
	if bp == nil {
		return nil
	}
	return &pb.BatchProgress{
		BatchId: bp.BatchID,
		Done:    int32(bp.Done),
		Total:   int32(bp.Total),
		Failed:  int32(bp.Failed),
		Step:    bp.Step,
	}
}

// --- JobEvent ---

func jobEventToPB(e *types.JobEvent) *pb.JobEvent {
	if e == nil {
		return nil
	}

	out := &pb.JobEvent{
		Type:           string(e.Type),
		JobId:          e.JobID,
		RunId:          e.RunID,
		NodeId:         e.NodeID,
		TaskType:       string(e.TaskType),
		Attempt:        int32(e.Attempt),
		Timestamp:      timeToTSPB(e.Timestamp),
		BatchProgress:  batchProgressToPB(e.BatchProgress),
		AffectedRunIds: e.AffectedRunIDs,
	}

	if e.Error != nil {
		out.Error = proto.String(*e.Error)
	}

	return out
}

// --- ScheduleEntry ---

func scheduleEntryToPB(se *types.ScheduleEntry) *pb.ScheduleEntry {
	if se == nil {
		return nil
	}
	return &pb.ScheduleEntry{
		JobId:     se.JobID,
		NextRunAt: timeToTSPB(se.NextRunAt),
		Schedule:  scheduleToPB(&se.Schedule),
	}
}

// --- PoolStats ---

func poolStatsToPB(ps *types.PoolStats) *pb.PoolStats {
	if ps == nil {
		return nil
	}
	return &pb.PoolStats{
		RunningWorkers: int32(ps.RunningWorkers),
		IdleWorkers:    int32(ps.IdleWorkers),
		MaxConcurrency: int32(ps.MaxConcurrency),
		TotalSubmitted: ps.TotalSubmitted,
		TotalCompleted: ps.TotalCompleted,
		TotalFailed:    ps.TotalFailed,
		QueueLength:    int32(ps.QueueLength),
	}
}

// --- NodeInfo ---

func nodeInfoToPB(n *types.NodeInfo) *pb.NodeInfo {
	if n == nil {
		return nil
	}
	return &pb.NodeInfo{
		NodeId:        n.NodeID,
		Address:       n.Address,
		TaskTypes:     n.TaskTypes,
		StartedAt:     timeToTSPB(n.StartedAt),
		LastHeartbeat: timeToTSPB(n.LastHeartbeat),
		PoolStats:     poolStatsToPB(n.PoolStats),
		ActiveRunIds:  n.ActiveRunIDs,
	}
}

// --- WorkMessage (DLQ) ---

func workMessageToPB(wm *types.WorkMessage) *pb.WorkMessage {
	if wm == nil {
		return nil
	}
	return &pb.WorkMessage{
		RunId:        wm.RunID,
		JobId:        wm.JobID,
		TaskType:     string(wm.TaskType),
		Payload:      []byte(wm.Payload),
		Attempt:      int32(wm.Attempt),
		Deadline:     timeToTSPB(wm.Deadline),
		Metadata:     wm.Metadata,
		Headers:      wm.Headers,
		Priority:     int32(int(wm.Priority)),
		DispatchedAt: timeToTSPB(wm.DispatchedAt),
	}
}

// --- WorkflowTaskState ---

func workflowTaskStateToPB(ts *types.WorkflowTaskState) *pb.WorkflowTaskState {
	if ts == nil {
		return nil
	}

	out := &pb.WorkflowTaskState{
		Name:       ts.Name,
		JobId:      ts.JobID,
		Status:     string(ts.Status),
		StartedAt:  optTimeToTSPB(ts.StartedAt),
		FinishedAt: optTimeToTSPB(ts.FinishedAt),
	}

	if ts.Error != nil {
		out.Error = proto.String(*ts.Error)
	}

	return out
}

// --- WorkflowTask ---

func workflowTaskToPB(t *types.WorkflowTask) *pb.WorkflowTask {
	if t == nil {
		return nil
	}
	return &pb.WorkflowTask{
		Name:      t.Name,
		TaskType:  string(t.TaskType),
		Payload:   []byte(t.Payload),
		DependsOn: t.DependsOn,
	}
}

// --- WorkflowDefinition ---

func workflowDefinitionToPB(def *types.WorkflowDefinition) *pb.WorkflowDefinition {
	if def == nil {
		return nil
	}

	out := &pb.WorkflowDefinition{
		Name:             def.Name,
		ExecutionTimeout: optDurationToPB(def.ExecutionTimeout),
		RetryPolicy:      retryPolicyToPB(def.RetryPolicy),
	}

	if len(def.Tasks) > 0 {
		out.Tasks = make([]*pb.WorkflowTask, len(def.Tasks))
		for i := range def.Tasks {
			out.Tasks[i] = workflowTaskToPB(&def.Tasks[i])
		}
	}

	if def.DefaultPriority != nil {
		out.DefaultPriority = proto.Int32(int32(int(*def.DefaultPriority)))
	}

	return out
}

// --- WorkflowInstance ---

func workflowInstanceToPB(wf *types.WorkflowInstance) *pb.WorkflowInstance {
	if wf == nil {
		return nil
	}

	out := &pb.WorkflowInstance{
		Id:           wf.ID,
		WorkflowName: wf.WorkflowName,
		Status:       string(wf.Status),
		Definition:   workflowDefinitionToPB(&wf.Definition),
		Input:        []byte(wf.Input),
		Deadline:     optTimeToTSPB(wf.Deadline),
		Attempt:      int32(wf.Attempt),
		MaxAttempts:  int32(wf.MaxAttempts),
		CreatedAt:    timeToTSPB(wf.CreatedAt),
		UpdatedAt:    timeToTSPB(wf.UpdatedAt),
		CompletedAt:  optTimeToTSPB(wf.CompletedAt),
	}

	if len(wf.Tasks) > 0 {
		out.Tasks = make(map[string]*pb.WorkflowTaskState, len(wf.Tasks))
		for name, state := range wf.Tasks {
			out.Tasks[name] = workflowTaskStateToPB(&state)
		}
	}

	return out
}

// --- BatchItem ---

func batchItemToPB(bi *types.BatchItem) *pb.BatchItem {
	if bi == nil {
		return nil
	}
	return &pb.BatchItem{
		Id:      bi.ID,
		Payload: []byte(bi.Payload),
	}
}

// --- BatchDefinition ---

func batchDefinitionToPB(def *types.BatchDefinition) *pb.BatchDefinition {
	if def == nil {
		return nil
	}

	out := &pb.BatchDefinition{
		Name:             def.Name,
		OnetimePayload:   []byte(def.OnetimePayload),
		ItemTaskType:     string(def.ItemTaskType),
		FailurePolicy:    string(def.FailurePolicy),
		ChunkSize:        int32(def.ChunkSize),
		ItemRetryPolicy:  retryPolicyToPB(def.ItemRetryPolicy),
		ExecutionTimeout: optDurationToPB(def.ExecutionTimeout),
		RetryPolicy:      retryPolicyToPB(def.RetryPolicy),
	}

	if def.OnetimeTaskType != nil {
		out.OnetimeTaskType = proto.String(string(*def.OnetimeTaskType))
	}

	if len(def.Items) > 0 {
		out.Items = make([]*pb.BatchItem, len(def.Items))
		for i := range def.Items {
			out.Items[i] = batchItemToPB(&def.Items[i])
		}
	}

	if def.DefaultPriority != nil {
		out.DefaultPriority = proto.Int32(int32(int(*def.DefaultPriority)))
	}

	return out
}

// --- BatchOnetimeState ---

func batchOnetimeStateToPB(s *types.BatchOnetimeState) *pb.BatchOnetimeState {
	if s == nil {
		return nil
	}

	out := &pb.BatchOnetimeState{
		JobId:      s.JobID,
		Status:     string(s.Status),
		ResultData: []byte(s.ResultData),
		StartedAt:  optTimeToTSPB(s.StartedAt),
		FinishedAt: optTimeToTSPB(s.FinishedAt),
	}

	if s.Error != nil {
		out.Error = proto.String(*s.Error)
	}

	return out
}

// --- BatchItemState ---

func batchItemStateToPB(s *types.BatchItemState) *pb.BatchItemState {
	if s == nil {
		return nil
	}

	out := &pb.BatchItemState{
		ItemId:     s.ItemID,
		JobId:      s.JobID,
		Status:     string(s.Status),
		StartedAt:  optTimeToTSPB(s.StartedAt),
		FinishedAt: optTimeToTSPB(s.FinishedAt),
	}

	if s.Error != nil {
		out.Error = proto.String(*s.Error)
	}

	return out
}

// --- BatchItemResult ---

func batchItemResultToPB(r *types.BatchItemResult) *pb.BatchItemResult {
	if r == nil {
		return nil
	}

	out := &pb.BatchItemResult{
		BatchId: r.BatchID,
		ItemId:  r.ItemID,
		Success: r.Success,
		Output:  []byte(r.Output),
	}

	if r.Error != nil {
		out.Error = proto.String(*r.Error)
	}

	return out
}

// --- BatchInstance ---

func batchInstanceToPB(b *types.BatchInstance) *pb.BatchInstance {
	if b == nil {
		return nil
	}

	out := &pb.BatchInstance{
		Id:             b.ID,
		Name:           b.Name,
		Status:         string(b.Status),
		Definition:     batchDefinitionToPB(&b.Definition),
		OnetimeState:   batchOnetimeStateToPB(b.OnetimeState),
		TotalItems:     int32(b.TotalItems),
		CompletedItems: int32(b.CompletedItems),
		FailedItems:    int32(b.FailedItems),
		RunningItems:   int32(b.RunningItems),
		PendingItems:   int32(b.PendingItems),
		NextChunkIndex: int32(b.NextChunkIndex),
		Deadline:       optTimeToTSPB(b.Deadline),
		Attempt:        int32(b.Attempt),
		MaxAttempts:    int32(b.MaxAttempts),
		CreatedAt:      timeToTSPB(b.CreatedAt),
		UpdatedAt:      timeToTSPB(b.UpdatedAt),
		CompletedAt:    optTimeToTSPB(b.CompletedAt),
	}

	if len(b.ItemStates) > 0 {
		out.ItemStates = make(map[string]*pb.BatchItemState, len(b.ItemStates))
		for id, state := range b.ItemStates {
			out.ItemStates[id] = batchItemStateToPB(&state)
		}
	}

	return out
}

// --- Pagination ---

// parsePaginationPB converts a proto PaginationRequest to the internal pageParams.
func parsePaginationPB(req *pb.PaginationRequest) pageParams {
	pp := pageParams{Limit: 10, Sort: "newest"}

	if req == nil {
		return pp
	}

	if req.Limit > 0 {
		pp.Limit = int(req.Limit)
	}
	if pp.Limit > maxPageLimit {
		pp.Limit = maxPageLimit
	}

	if req.Offset > 0 {
		pp.Offset = int(req.Offset)
	}

	switch req.Sort {
	case pb.SortOrder_SORT_ORDER_OLDEST:
		pp.Sort = "oldest"
	default:
		pp.Sort = "newest"
	}

	return pp
}
