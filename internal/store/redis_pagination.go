package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
)

// ============================================================
// Paginated Job listing
// ============================================================

// ListJobsPaginated returns a paginated, filtered list of jobs backed by Redis sorted sets.
func (s *RedisStore) ListJobsPaginated(ctx context.Context, filter JobFilter) ([]types.Job, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	// Choose the best index for the filter.
	indexKey := JobsByCreatedKey(s.prefix)
	if filter.Status != nil {
		indexKey = JobsByStatusKey(s.prefix, string(*filter.Status))
	} else if filter.TaskType != nil {
		indexKey = JobsByTaskTypeKey(s.prefix, string(*filter.TaskType))
	} else if filter.Tag != nil {
		indexKey = JobsByTagKey(s.prefix, *filter.Tag)
	}

	// Total count from this index.
	total, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(indexKey).Build()).AsInt64()
	if err != nil {
		return nil, 0, fmt.Errorf("count jobs: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	// Fetch IDs with offset/limit.
	var ids []string
	start := int64(filter.Offset)
	stop := int64(filter.Offset + filter.Limit - 1)
	if filter.Sort == "oldest" {
		ids, err = s.rdb.Do(ctx, s.rdb.B().Zrange().Key(indexKey).Min(strconv.FormatInt(start, 10)).Max(strconv.FormatInt(stop, 10)).Build()).AsStrSlice()
	} else {
		// Default: newest first
		ids, err = s.rdb.Do(ctx, s.rdb.B().Zrevrange().Key(indexKey).Start(start).Stop(stop).Build()).AsStrSlice()
	}
	if err != nil {
		return nil, 0, fmt.Errorf("paginate jobs: %w", err)
	}
	if len(ids) == 0 {
		return nil, int(total), nil
	}

	// Hydrate with pipeline.
	jobs, err := s.hydrateJobValues(ctx, ids)
	if err != nil {
		return nil, 0, err
	}

	// Apply search filter (substring match on ID or TaskType) in memory.
	if filter.Search != nil && *filter.Search != "" {
		search := strings.ToLower(*filter.Search)
		filtered := jobs[:0]
		for _, j := range jobs {
			if strings.Contains(strings.ToLower(j.ID), search) ||
				strings.Contains(strings.ToLower(string(j.TaskType)), search) {
				filtered = append(filtered, j)
			}
		}
		jobs = filtered
	}

	return jobs, int(total), nil
}

// ============================================================
// Paginated Schedule listing
// ============================================================

// ListSchedulesPaginated returns a paginated list of schedules.
func (s *RedisStore) ListSchedulesPaginated(ctx context.Context, filter ScheduleFilter) ([]types.ScheduleEntry, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	indexKey := SchedulesDueKey(s.prefix)

	total, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(indexKey).Build()).AsInt64()
	if err != nil {
		return nil, 0, fmt.Errorf("count schedules: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	var ids []string
	start := int64(filter.Offset)
	stop := int64(filter.Offset + filter.Limit - 1)
	if filter.Sort == "oldest" {
		ids, err = s.rdb.Do(ctx, s.rdb.B().Zrange().Key(indexKey).Min(strconv.FormatInt(start, 10)).Max(strconv.FormatInt(stop, 10)).Build()).AsStrSlice()
	} else {
		ids, err = s.rdb.Do(ctx, s.rdb.B().Zrevrange().Key(indexKey).Start(start).Stop(stop).Build()).AsStrSlice()
	}
	if err != nil {
		return nil, 0, fmt.Errorf("paginate schedules: %w", err)
	}
	if len(ids) == 0 {
		return nil, int(total), nil
	}

	scheds, err := s.hydrateScheduleValues(ctx, ids)
	if err != nil {
		return nil, 0, err
	}
	return scheds, int(total), nil
}

// ============================================================
// Paginated Job Run listing (history)
// ============================================================

// ListJobRuns returns paginated job runs from history.
func (s *RedisStore) ListJobRuns(ctx context.Context, filter RunFilter) ([]types.JobRun, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	// Choose index based on filter.
	indexKey := HistoryRunsKey(s.prefix)
	if filter.JobID != nil {
		indexKey = HistoryRunsByJobKey(s.prefix, *filter.JobID)
	} else if filter.Status != nil {
		indexKey = HistoryRunsByStatusKey(s.prefix, string(*filter.Status))
	}

	// Time range bounds.
	min, max := "-inf", "+inf"
	if filter.Since != nil {
		min = strconv.FormatFloat(float64(filter.Since.UnixNano()), 'f', 0, 64)
	}
	if filter.Until != nil {
		max = strconv.FormatFloat(float64(filter.Until.UnixNano()), 'f', 0, 64)
	}

	total, err := s.rdb.Do(ctx, s.rdb.B().Zcount().Key(indexKey).Min(min).Max(max).Build()).AsInt64()
	if err != nil {
		return nil, 0, fmt.Errorf("count runs: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	var ids []string
	if filter.Sort == "oldest" {
		ids, err = s.rdb.Do(ctx, s.rdb.B().Zrangebyscore().Key(indexKey).Min(min).Max(max).Limit(int64(filter.Offset), int64(filter.Limit)).Build()).AsStrSlice()
	} else {
		ids, err = s.rdb.Do(ctx, s.rdb.B().Zrevrangebyscore().Key(indexKey).Max(max).Min(min).Limit(int64(filter.Offset), int64(filter.Limit)).Build()).AsStrSlice()
	}
	if err != nil {
		return nil, 0, fmt.Errorf("paginate runs: %w", err)
	}
	if len(ids) == 0 {
		return nil, int(total), nil
	}

	runs, err := s.hydrateRunValues(ctx, ids)
	if err != nil {
		return nil, 0, err
	}
	return runs, int(total), nil
}

// ============================================================
// Paginated Job Event listing (history)
// ============================================================

// ListJobEvents returns paginated job events from history.
func (s *RedisStore) ListJobEvents(ctx context.Context, filter EventFilter) ([]types.JobEvent, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	indexKey := HistoryEventsKey(s.prefix)
	if filter.JobID != nil {
		indexKey = HistoryEventsByJobKey(s.prefix, *filter.JobID)
	} else if filter.EventType != nil {
		indexKey = HistoryEventsByTypeKey(s.prefix, string(*filter.EventType))
	}

	min, max := "-inf", "+inf"
	if filter.Since != nil {
		min = strconv.FormatFloat(float64(filter.Since.UnixNano()), 'f', 0, 64)
	}
	if filter.Until != nil {
		max = strconv.FormatFloat(float64(filter.Until.UnixNano()), 'f', 0, 64)
	}

	total, err := s.rdb.Do(ctx, s.rdb.B().Zcount().Key(indexKey).Min(min).Max(max).Build()).AsInt64()
	if err != nil {
		return nil, 0, fmt.Errorf("count events: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrevrangebyscore().Key(indexKey).Max(max).Min(min).Limit(int64(filter.Offset), int64(filter.Limit)).Build()).AsStrSlice()
	if err != nil {
		return nil, 0, fmt.Errorf("paginate events: %w", err)
	}
	if len(ids) == 0 {
		return nil, int(total), nil
	}

	events, err := s.hydrateEventValues(ctx, ids)
	if err != nil {
		return nil, 0, err
	}
	return events, int(total), nil
}

// ============================================================
// Paginated Workflow listing
// ============================================================

// ListWorkflowInstances returns paginated workflow instances.
func (s *RedisStore) ListWorkflowInstances(ctx context.Context, filter WorkflowFilter) ([]types.WorkflowInstance, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	indexKey := WorkflowsByCreatedKey(s.prefix)
	if filter.Status != nil {
		indexKey = WorkflowsByStatusKey(s.prefix, string(*filter.Status))
	}

	total, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(indexKey).Build()).AsInt64()
	if err != nil {
		return nil, 0, fmt.Errorf("count workflows: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	var ids []string
	start := int64(filter.Offset)
	stop := int64(filter.Offset + filter.Limit - 1)
	if filter.Sort == "oldest" {
		ids, err = s.rdb.Do(ctx, s.rdb.B().Zrange().Key(indexKey).Min(strconv.FormatInt(start, 10)).Max(strconv.FormatInt(stop, 10)).Build()).AsStrSlice()
	} else {
		ids, err = s.rdb.Do(ctx, s.rdb.B().Zrevrange().Key(indexKey).Start(start).Stop(stop).Build()).AsStrSlice()
	}
	if err != nil {
		return nil, 0, fmt.Errorf("paginate workflows: %w", err)
	}
	if len(ids) == 0 {
		return nil, int(total), nil
	}

	wfs, err := s.hydrateWorkflowValues(ctx, ids)
	if err != nil {
		return nil, 0, err
	}

	// In-memory filter by workflow name if specified.
	if filter.WorkflowName != nil && *filter.WorkflowName != "" {
		name := *filter.WorkflowName
		filtered := wfs[:0]
		for _, wf := range wfs {
			if wf.WorkflowName == name {
				filtered = append(filtered, wf)
			}
		}
		wfs = filtered
	}

	return wfs, int(total), nil
}

// GetWorkflowByID retrieves a workflow instance by ID (convenience for monitor API).
func (s *RedisStore) GetWorkflowByID(ctx context.Context, id string) (*types.WorkflowInstance, error) {
	wf, _, err := s.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}
	return wf, nil
}

// ============================================================
// Paginated Batch listing
// ============================================================

// ListBatchInstances returns paginated batch instances.
func (s *RedisStore) ListBatchInstances(ctx context.Context, filter BatchFilter) ([]types.BatchInstance, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	indexKey := BatchesByCreatedKey(s.prefix)
	if filter.Status != nil {
		indexKey = BatchesByStatusKey(s.prefix, string(*filter.Status))
	}

	total, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(indexKey).Build()).AsInt64()
	if err != nil {
		return nil, 0, fmt.Errorf("count batches: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	var ids []string
	start := int64(filter.Offset)
	stop := int64(filter.Offset + filter.Limit - 1)
	if filter.Sort == "oldest" {
		ids, err = s.rdb.Do(ctx, s.rdb.B().Zrange().Key(indexKey).Min(strconv.FormatInt(start, 10)).Max(strconv.FormatInt(stop, 10)).Build()).AsStrSlice()
	} else {
		ids, err = s.rdb.Do(ctx, s.rdb.B().Zrevrange().Key(indexKey).Start(start).Stop(stop).Build()).AsStrSlice()
	}
	if err != nil {
		return nil, 0, fmt.Errorf("paginate batches: %w", err)
	}
	if len(ids) == 0 {
		return nil, int(total), nil
	}

	batches, err := s.hydrateBatchValues(ctx, ids)
	if err != nil {
		return nil, 0, err
	}

	// In-memory filter by name if specified.
	if filter.Name != nil && *filter.Name != "" {
		name := *filter.Name
		filtered := batches[:0]
		for _, b := range batches {
			if b.Name == name {
				filtered = append(filtered, b)
			}
		}
		batches = filtered
	}

	return batches, int(total), nil
}

// GetBatchByID retrieves a batch instance by ID (convenience for monitor API).
func (s *RedisStore) GetBatchByID(ctx context.Context, id string) (*types.BatchInstance, error) {
	batch, _, err := s.GetBatch(ctx, id)
	if err != nil {
		return nil, err
	}
	return batch, nil
}

// SaveWorkflowInstance saves a workflow instance (alias for history compatibility).
func (s *RedisStore) SaveWorkflowInstance(ctx context.Context, wf *types.WorkflowInstance) error {
	_, err := s.SaveWorkflow(ctx, wf)
	return err
}

// SaveBatchInstance saves a batch instance (alias for history compatibility).
func (s *RedisStore) SaveBatchInstance(ctx context.Context, batch *types.BatchInstance) error {
	_, err := s.SaveBatch(ctx, batch)
	return err
}

// ============================================================
// Value hydration helpers (return value types, not pointer types)
// ============================================================

func (s *RedisStore) hydrateJobValues(ctx context.Context, ids []string) ([]types.Job, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(JobKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	jobs := make([]types.Job, 0, len(ids))
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var job types.Job
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &job) == nil {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func (s *RedisStore) hydrateScheduleValues(ctx context.Context, ids []string) ([]types.ScheduleEntry, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(ScheduleKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	var scheds []types.ScheduleEntry
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var sched types.ScheduleEntry
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &sched) == nil {
			scheds = append(scheds, sched)
		}
	}
	return scheds, nil
}

func (s *RedisStore) hydrateRunValues(ctx context.Context, ids []string) ([]types.JobRun, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(HistoryRunKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	var runs []types.JobRun
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var run types.JobRun
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &run) == nil {
			runs = append(runs, run)
		}
	}
	return runs, nil
}

func (s *RedisStore) hydrateEventValues(ctx context.Context, ids []string) ([]types.JobEvent, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(HistoryEventKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	var events []types.JobEvent
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var event types.JobEvent
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &event) == nil {
			events = append(events, event)
		}
	}
	return events, nil
}

func (s *RedisStore) hydrateWorkflowValues(ctx context.Context, ids []string) ([]types.WorkflowInstance, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(WorkflowKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	var wfs []types.WorkflowInstance
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var wf types.WorkflowInstance
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &wf) == nil {
			wfs = append(wfs, wf)
		}
	}
	return wfs, nil
}

func (s *RedisStore) hydrateBatchValues(ctx context.Context, ids []string) ([]types.BatchInstance, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(BatchKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	var batches []types.BatchInstance
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var batch types.BatchInstance
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &batch) == nil {
			batches = append(batches, batch)
		}
	}
	return batches, nil
}
