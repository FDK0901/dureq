package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

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

	if filter.Search != nil && *filter.Search != "" {
		return s.searchJobs(ctx, indexKey, filter)
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

	return jobs, int(total), nil
}

const (
	jobSearchChunkSize = 500
	// maxJobSearchScan bounds the work of a search: jobs past it are not looked at.
	maxJobSearchScan = 100_000
)

// scanMatches returns a page of what matches in an index, how many match, and whether some of
// the index were not looked at: then there may be more. It looks at MaxNameScan at most, and,
// if skipTotal, at no more than it takes to fill the page.
func scanMatches[T any](ctx context.Context, s *RedisStore, indexKey, sort string, offset, limit int, skipTotal bool, hydrate func(context.Context, []string) ([]T, error), match func(*T) bool) ([]T, int, bool, error) {
	var page []T
	total, scanned := 0, 0
	stopped := false
	err := s.scanIndex(ctx, indexKey, sort, nameScanChunkSize, func(ids []string) (bool, error) {
		ids = ids[:min(len(ids), s.cfg.MaxNameScan-scanned)]
		values, err := hydrate(ctx, ids)
		if err != nil {
			return false, err
		}
		for i := range values {
			if !match(&values[i]) {
				continue
			}
			if total >= offset && len(page) < limit {
				page = append(page, values[i])
			}
			total++
			if skipTotal && total >= offset+limit {
				stopped = true
				return false, nil
			}
		}
		scanned += len(ids)
		if scanned >= s.cfg.MaxNameScan {
			stopped = true
			return false, ctx.Err()
		}
		return true, ctx.Err()
	})
	if err != nil {
		return nil, 0, false, err
	}
	if stopped && !skipTotal {
		// It may have stopped at the end.
		if n, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(indexKey).Build()).AsInt64(); err == nil && n <= int64(scanned) {
			stopped = false
		}
	}
	return page, total, stopped, nil
}

// nameScanChunkSize is how many workflows or batches are read at once to look for a name. They
// may be large.
const nameScanChunkSize = 100

// searchJobs returns the page of the jobs whose ID or task type contains filter.Search,
// and how many there are. It reads the jobs of the index one by one.
func (s *RedisStore) searchJobs(ctx context.Context, indexKey string, filter JobFilter) ([]types.Job, int, error) {
	search := strings.ToLower(*filter.Search)
	var page []types.Job
	total, scanned := 0, 0
	err := s.scanIndex(ctx, indexKey, filter.Sort, jobSearchChunkSize, func(ids []string) (bool, error) {
		jobs, err := s.hydrateJobValues(ctx, ids)
		if err != nil {
			return false, err
		}
		for _, j := range jobs {
			if !strings.Contains(strings.ToLower(j.ID), search) &&
				!strings.Contains(strings.ToLower(string(j.TaskType)), search) {
				continue
			}
			if total >= filter.Offset && len(page) < filter.Limit {
				page = append(page, j)
			}
			total++
		}
		scanned += len(ids)
		return scanned < maxJobSearchScan, ctx.Err()
	})
	if err != nil {
		return nil, 0, fmt.Errorf("search jobs: %w", err)
	}
	return page, total, nil
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

	for {
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
		if filter.JobID == nil && filter.Status != nil {
			removed, err := s.removeStaleRuns(ctx, indexKey, *filter.Status, runs)
			if err != nil {
				return nil, 0, err
			}
			if removed {
				continue
			}
		}
		return runs, int(total), nil
	}
}

// removeStaleRuns removes the runs that have a different status by now from the index of
// a status, and reports whether there were any. Versions before v0.3.1 left a run that
// was retried in the index of every status it has had.
func (s *RedisStore) removeStaleRuns(ctx context.Context, indexKey string, status types.RunStatus, runs []types.JobRun) (bool, error) {
	var stale []string
	for _, run := range runs {
		if run.Status != status {
			stale = append(stale, run.ID)
		}
	}
	if len(stale) == 0 {
		return false, nil
	}
	if err := s.rdb.Do(ctx, s.rdb.B().Zrem().Key(indexKey).Member(stale...).Build()).Error(); err != nil {
		return false, fmt.Errorf("remove stale runs: %w", err)
	}
	return true, nil
}

// ============================================================
// Paginated Job Event listing (history)
// ============================================================

const (
	eventScanChunkSize = 1000
	// eventClockSkew is how far the clock of the node that published an event, which is
	// the event's Timestamp, may be off that of Redis, which is the ID in the stream.
	eventClockSkew = time.Minute
)

// ListJobEvents returns a page of the events, newest first, and how many there are. They
// are read from the event stream, which keeps about the last EventStreamMaxLen of them.
// If filter.SkipTotal is set, the count goes no further than the page.
func (s *RedisStore) ListJobEvents(ctx context.Context, filter EventFilter) ([]types.JobEvent, int, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	start, end := "-", "+"
	if filter.Since != nil {
		start = strconv.FormatInt(max(0, filter.Since.Add(-eventClockSkew).UnixMilli()), 10)
	}
	if filter.Until != nil {
		end = strconv.FormatInt(max(0, filter.Until.Add(eventClockSkew).UnixMilli()), 10)
	}

	// Without a filter there is no need to read all events to count them.
	unfiltered := filter.JobID == nil && filter.EventType == nil && filter.Since == nil && filter.Until == nil

	var page []types.JobEvent
	total := 0
	for {
		entries, err := s.rdb.Do(ctx, s.rdb.B().Xrevrange().Key(EventsStreamKey(s.prefix)).End(end).Start(start).Count(eventScanChunkSize).Build()).AsXRange()
		if err != nil {
			return nil, 0, fmt.Errorf("list events: %w", err)
		}
		for _, entry := range entries {
			if filter.JobID != nil && entry.FieldValues["job_id"] != *filter.JobID {
				continue
			}
			if filter.EventType != nil && entry.FieldValues["type"] != string(*filter.EventType) {
				continue
			}
			inPage := total >= filter.Offset && len(page) < filter.Limit
			if inPage || filter.Since != nil || filter.Until != nil {
				var event types.JobEvent
				if err := sonic.ConfigFastest.Unmarshal([]byte(entry.FieldValues["data"]), &event); err != nil {
					continue
				}
				if filter.Since != nil && event.Timestamp.Before(*filter.Since) || filter.Until != nil && event.Timestamp.After(*filter.Until) {
					continue
				}
				if inPage {
					page = append(page, event)
				}
			}
			total++
		}
		if len(entries) < eventScanChunkSize || filter.SkipTotal && len(page) == filter.Limit {
			return page, total, nil
		}
		if unfiltered && len(page) == filter.Limit {
			n, err := s.rdb.Do(ctx, s.rdb.B().Xlen().Key(EventsStreamKey(s.prefix)).Build()).AsInt64()
			if err != nil {
				return nil, 0, fmt.Errorf("count events: %w", err)
			}
			return page, int(n), nil
		}
		end = "(" + entries[len(entries)-1].ID
	}
}

// ============================================================
// Paginated Workflow listing
// ============================================================

// ListWorkflowInstances returns paginated workflow instances. With a name, total is how many
// match among those that were looked at; ListWorkflowInstancesPage says whether that is all.
func (s *RedisStore) ListWorkflowInstances(ctx context.Context, filter WorkflowFilter) ([]types.WorkflowInstance, int, error) {
	wfs, total, _, err := s.ListWorkflowInstancesPage(ctx, filter)
	return wfs, total, err
}

// ListWorkflowInstancesPage is ListWorkflowInstances, and also reports whether some workflows
// were not looked at, so that there may be more that match than total: with a name, it looks
// at MaxNameScan at most, and with SkipTotal at what fills the page.
func (s *RedisStore) ListWorkflowInstancesPage(ctx context.Context, filter WorkflowFilter) ([]types.WorkflowInstance, int, bool, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	indexKey := WorkflowsByCreatedKey(s.prefix)
	if filter.Status != nil {
		indexKey = WorkflowsByStatusKey(s.prefix, string(*filter.Status))
	}

	if filter.WorkflowName != nil && *filter.WorkflowName != "" {
		name := *filter.WorkflowName
		return scanMatches(ctx, s, indexKey, filter.Sort, filter.Offset, filter.Limit, filter.SkipTotal, s.hydrateWorkflowValues, func(v *types.WorkflowInstance) bool {
			return v.WorkflowName == name
		})
	}

	total, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(indexKey).Build()).AsInt64()
	if err != nil {
		return nil, 0, false, fmt.Errorf("count workflows: %w", err)
	}
	if total == 0 {
		return nil, 0, false, nil
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
		return nil, 0, false, fmt.Errorf("paginate workflows: %w", err)
	}
	if len(ids) == 0 {
		return nil, int(total), false, nil
	}

	wfs, err := s.hydrateWorkflowValues(ctx, ids)
	if err != nil {
		return nil, 0, false, err
	}
	return wfs, int(total), false, nil
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

// ListBatchInstances returns paginated batch instances. With a name, total is how many match
// among those that were looked at; ListBatchInstancesPage says whether that is all.
func (s *RedisStore) ListBatchInstances(ctx context.Context, filter BatchFilter) ([]types.BatchInstance, int, error) {
	batches, total, _, err := s.ListBatchInstancesPage(ctx, filter)
	return batches, total, err
}

// ListBatchInstancesPage is ListBatchInstances, and also reports whether some batches were not
// looked at, so that there may be more that match than total: with a name, it looks at
// MaxNameScan at most, and with SkipTotal at what fills the page.
func (s *RedisStore) ListBatchInstancesPage(ctx context.Context, filter BatchFilter) ([]types.BatchInstance, int, bool, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}

	indexKey := BatchesByCreatedKey(s.prefix)
	if filter.Status != nil {
		indexKey = BatchesByStatusKey(s.prefix, string(*filter.Status))
	}

	if filter.Name != nil && *filter.Name != "" {
		name := *filter.Name
		return scanMatches(ctx, s, indexKey, filter.Sort, filter.Offset, filter.Limit, filter.SkipTotal, s.hydrateBatchValues, func(v *types.BatchInstance) bool {
			return v.Name == name
		})
	}

	total, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(indexKey).Build()).AsInt64()
	if err != nil {
		return nil, 0, false, fmt.Errorf("count batches: %w", err)
	}
	if total == 0 {
		return nil, 0, false, nil
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
		return nil, 0, false, fmt.Errorf("paginate batches: %w", err)
	}
	if len(ids) == 0 {
		return nil, int(total), false, nil
	}

	batches, err := s.hydrateBatchValues(ctx, ids)
	if err != nil {
		return nil, 0, false, err
	}
	return batches, int(total), false, nil
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
			nameWorkflowTasks(&wf)
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
