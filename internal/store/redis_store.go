package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog"
	"github.com/bytedance/sonic"
	"github.com/redis/rueidis"
)

// Sentinel errors for the Redis store.
var (
	ErrCASConflict   = errors.New("dureq: CAS conflict")
	ErrAlreadyExists = errors.New("dureq: already exists")
)

// --- Filter types (formerly in history.go) ---

// RunFilter specifies criteria for listing job runs.
type RunFilter struct {
	JobID    *string
	NodeID   *string
	Status   *types.RunStatus
	TaskType *types.TaskType
	Tags     []string
	Since    *time.Time
	Until    *time.Time
	Limit    int
	Offset   int
	Sort     string // "newest" (default) or "oldest"
}

// EventFilter specifies criteria for listing job events.
type EventFilter struct {
	JobID     *string
	EventType *types.EventType
	Since     *time.Time
	Until     *time.Time
	Limit     int
	Offset    int
}

// StatsFilter specifies criteria for aggregated stats queries.
type StatsFilter struct {
	TaskType *types.TaskType
	Tags     []string
	Since    *time.Time
	Until    *time.Time
}

// JobStats holds aggregated statistics.
type JobStats struct {
	TotalJobs   int64                     `json:"total_jobs"`
	TotalRuns   int64                     `json:"total_runs"`
	ByStatus    map[types.JobStatus]int64 `json:"by_status"`
	ByTaskType  map[types.TaskType]int64  `json:"by_task_type"`
	AvgDuration time.Duration             `json:"avg_duration"`
	SuccessRate float64                   `json:"success_rate"`
	FailureRate float64                   `json:"failure_rate"`
}

// WorkflowFilter specifies criteria for listing workflow instances.
type WorkflowFilter struct {
	WorkflowName *string
	Status       *types.WorkflowStatus
	Sort         string // "newest" or "oldest"
	Limit        int
	Offset       int
}

// BatchFilter specifies criteria for listing batch instances.
type BatchFilter struct {
	Name   *string
	Status *types.WorkflowStatus
	Sort   string // "newest" or "oldest"
	Limit  int
	Offset int
}

// JobFilter specifies criteria for listing active jobs.
type JobFilter struct {
	Status   *types.JobStatus
	TaskType *types.TaskType
	Tag      *string
	Search   *string // substring match on ID or TaskType
	Sort     string  // "newest" or "oldest"
	Limit    int
	Offset   int
}

// ScheduleFilter specifies criteria for listing active schedules.
type ScheduleFilter struct {
	Sort   string // "newest" or "oldest"
	Limit  int
	Offset int
}

// --- RedisStore ---

// RedisStore manages all dureq state in Redis, replacing both NATSStore and HistoryStore.
type RedisStore struct {
	rdb    rueidis.Client
	cfg    RedisStoreConfig
	prefix string
	logger chainedlog.Logger

	// Pre-compiled Lua scripts (all single-key for Redis Cluster compatibility)
	scriptSaveJob         *rueidis.Lua
	scriptCreateJob       *rueidis.Lua
	scriptCASJob          *rueidis.Lua
	scriptCASUpdate       *rueidis.Lua
	scriptCreateNX        *rueidis.Lua
	scriptLeaderRefresh   *rueidis.Lua
	scriptMoveDelayed     *rueidis.Lua
	scriptFlushGroup      *rueidis.Lua
	scriptClaimSideEffect *rueidis.Lua
	scriptAcquireConcSlot *rueidis.Lua
	scriptReleaseConcSlot *rueidis.Lua
}

// NewRedisStore creates a new Redis store and registers tier configuration.
func NewRedisStore(rdb rueidis.Client, cfg RedisStoreConfig, logger chainedlog.Logger) (*RedisStore, error) {
	cfg.defaults()

	s := &RedisStore{
		rdb:    rdb,
		cfg:    cfg,
		prefix: cfg.prefix(),
		logger: logger,

		scriptSaveJob:         rueidis.NewLuaScript(luaSaveJobSingleKey),
		scriptCreateJob:       rueidis.NewLuaScript(luaCreateIfNotExists),
		scriptCASJob:          rueidis.NewLuaScript(luaCASUpdateJobSingleKey),
		scriptCASUpdate:       rueidis.NewLuaScript(luaCASUpdate),
		scriptCreateNX:        rueidis.NewLuaScript(luaCreateIfNotExists),
		scriptLeaderRefresh:   rueidis.NewLuaScript(luaLeaderRefresh),
		scriptMoveDelayed:     rueidis.NewLuaScript(luaMoveDelayedToStream),
		scriptFlushGroup:      rueidis.NewLuaScript(luaFlushGroup),
		scriptClaimSideEffect: rueidis.NewLuaScript(luaClaimSideEffect),
		scriptAcquireConcSlot: rueidis.NewLuaScript(luaAcquireConcurrencySlot),
		scriptReleaseConcSlot: rueidis.NewLuaScript(luaReleaseConcurrencySlot),
	}

	// Register tiers in sorted set so workers can discover them.
	ctx := context.Background()
	for _, tier := range cfg.Tiers {
		s.rdb.Do(ctx, s.rdb.B().Zadd().Key(TiersKey(s.prefix)).ScoreMember().ScoreMember(float64(tier.Weight), tier.Name).Build())
	}

	return s, nil
}

// --- Accessors ---

func (s *RedisStore) Client() rueidis.Client   { return s.rdb }
func (s *RedisStore) Prefix() string           { return s.prefix }
func (s *RedisStore) Config() RedisStoreConfig { return s.cfg }

func (s *RedisStore) Ping(ctx context.Context) error {
	return s.rdb.Do(ctx, s.rdb.B().Ping().Build()).Error()
}

func (s *RedisStore) Close() error {
	s.rdb.Close()
	return nil
}

// ============================================================
// Job operations
// ============================================================

// SaveJob upserts a job and maintains all secondary indexes.
// The job hash is updated atomically via a single-key Lua script;
// secondary indexes are updated individually (best-effort) for Redis Cluster compatibility.
func (s *RedisStore) SaveJob(ctx context.Context, job *types.Job) (uint64, error) {
	data, err := sonic.ConfigFastest.Marshal(job)
	if err != nil {
		return 0, fmt.Errorf("marshal job: %w", err)
	}

	// Single-key Lua: upsert job hash, returns "old_data\nnew_version".
	result, err := s.scriptSaveJob.Exec(ctx, s.rdb,
		[]string{JobKey(s.prefix, job.ID)},
		[]string{string(data)},
	).ToString()
	if err != nil {
		return 0, fmt.Errorf("save job: %w", err)
	}

	oldData, ver := parseSaveJobResult(result)
	s.updateJobIndexes(ctx, job, oldData)
	s.updateJobAssociationIndexes(ctx, job)
	s.mirrorJobPayload(ctx, job) // best-effort JSON mirror for JSONPath search
	return ver, nil
}

// CreateJob atomically creates a job only if it doesn't already exist.
// Secondary indexes are added individually after creation.
func (s *RedisStore) CreateJob(ctx context.Context, job *types.Job) (uint64, error) {
	data, err := sonic.ConfigFastest.Marshal(job)
	if err != nil {
		return 0, fmt.Errorf("marshal job: %w", err)
	}

	// Single-key Lua: create hash only if key doesn't exist.
	_, err = s.scriptCreateJob.Exec(ctx, s.rdb,
		[]string{JobKey(s.prefix, job.ID)},
		[]string{string(data)},
	).ToString()
	if err != nil {
		if strings.Contains(err.Error(), "ALREADY_EXISTS") {
			return 0, ErrAlreadyExists
		}
		return 0, fmt.Errorf("create job: %w", err)
	}

	// Add all indexes (no old data to diff against).
	s.addJobIndexes(ctx, job)
	s.updateJobAssociationIndexes(ctx, job)
	s.mirrorJobPayload(ctx, job) // best-effort JSON mirror for JSONPath search
	return 1, nil
}

// GetJob retrieves a job by ID.
func (s *RedisStore) GetJob(ctx context.Context, jobID string) (*types.Job, uint64, error) {
	result, err := s.rdb.Do(ctx, s.rdb.B().Hgetall().Key(JobKey(s.prefix, jobID)).Build()).AsStrMap()
	if err != nil {
		return nil, 0, fmt.Errorf("get job: %w", err)
	}
	if len(result) == 0 {
		return nil, 0, types.ErrJobNotFound
	}

	dataStr, ok := result["data"]
	if !ok {
		return nil, 0, fmt.Errorf("get job: missing data field")
	}

	var job types.Job
	if err := sonic.ConfigFastest.Unmarshal([]byte(dataStr), &job); err != nil {
		return nil, 0, fmt.Errorf("unmarshal job: %w", err)
	}

	ver := parseVersion(result["_version"])
	return &job, ver, nil
}

// UpdateJob performs a CAS update on a job and maintains all secondary indexes.
// The job hash is updated atomically via a single-key Lua script;
// secondary indexes are diffed and updated individually.
func (s *RedisStore) UpdateJob(ctx context.Context, job *types.Job, expectedRev uint64) (uint64, error) {
	data, err := sonic.ConfigFastest.Marshal(job)
	if err != nil {
		return 0, fmt.Errorf("marshal job: %w", err)
	}

	// Single-key CAS Lua: returns "old_data\nnew_version".
	result, err := s.scriptCASJob.Exec(ctx, s.rdb,
		[]string{JobKey(s.prefix, job.ID)},
		[]string{strconv.FormatUint(expectedRev, 10), string(data)},
	).ToString()
	if err != nil {
		if strings.Contains(err.Error(), "CAS_CONFLICT") {
			return 0, ErrCASConflict
		}
		return 0, fmt.Errorf("update job (CAS): %w", err)
	}

	oldData, ver := parseSaveJobResult(result)

	// Validate state transition (log invalid transitions for audit).
	// Extract status via cheap string search to avoid full JSON unmarshal on hot path.
	if oldData != "" {
		if oldStatus := extractJSONStatus(oldData); oldStatus != "" {
			from := types.JobStatus(oldStatus)
			if from != job.Status && !types.ValidJobTransition(from, job.Status) {
				s.logger.Warn().
					String("job_id", job.ID).
					String("from", string(from)).
					String("to", string(job.Status)).
					Msg("store: invalid job state transition")
			}
		}
	}

	s.updateJobIndexes(ctx, job, oldData)
	s.updateJobAssociationIndexes(ctx, job)
	s.mirrorJobPayload(ctx, job) // best-effort JSON mirror for JSONPath search

	// Append audit trail entry for the state transition.
	s.appendJobAudit(ctx, job.ID, job.Status, job.LastError)

	return ver, nil
}

// DeleteJob removes a job and cleans all secondary indexes individually.
func (s *RedisStore) DeleteJob(ctx context.Context, jobID string) error {
	// Read old data for index cleanup.
	oldData, err := s.rdb.Do(ctx, s.rdb.B().Hget().Key(JobKey(s.prefix, jobID)).Field("data").Build()).ToString()
	if err != nil && !rueidis.IsRedisNil(err) {
		return fmt.Errorf("read job for deletion: %w", err)
	}

	// Delete the job hash key.
	if err := s.rdb.Do(ctx, s.rdb.B().Del().Key(JobKey(s.prefix, jobID)).Build()).Error(); err != nil {
		return fmt.Errorf("delete job: %w", err)
	}

	// Clean all secondary indexes from old data.
	if oldData != "" {
		var old types.Job
		if sonic.ConfigFastest.Unmarshal([]byte(oldData), &old) == nil {
			s.removeJobIndexes(ctx, jobID, &old)
		}
	}
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.rdb.Do(bgCtx, s.rdb.B().JsonDel().Key(JobPayloadKey(s.prefix, jobID)).Path("$").Build())
	}()
	return nil
}

// ListJobs returns all jobs (use sparingly for large datasets).
func (s *RedisStore) ListJobs(ctx context.Context) ([]*types.Job, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrange().Key(JobsByCreatedKey(s.prefix)).Min("0").Max("-1").Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list job IDs: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateJobs(ctx, ids)
}

// ListJobsByStatus returns all jobs with the given status.
func (s *RedisStore) ListJobsByStatus(ctx context.Context, status types.JobStatus) ([]*types.Job, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrange().Key(JobsByStatusKey(s.prefix, string(status))).Min("0").Max("-1").Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list jobs by status: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateJobs(ctx, ids)
}

// JobStatusCounts returns job counts by status using ZCARD on index sets.
func (s *RedisStore) JobStatusCounts(ctx context.Context) (map[types.JobStatus]int, error) {
	statuses := []types.JobStatus{
		types.JobStatusPending, types.JobStatusScheduled, types.JobStatusRunning,
		types.JobStatusCompleted, types.JobStatusFailed, types.JobStatusRetrying,
		types.JobStatusDead, types.JobStatusCancelled,
	}

	cmds := make(rueidis.Commands, 0, len(statuses))
	for _, st := range statuses {
		cmds = append(cmds, s.rdb.B().Zcard().Key(JobsByStatusKey(s.prefix, string(st))).Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	counts := make(map[types.JobStatus]int, len(statuses))
	for i, st := range statuses {
		if n, err := results[i].AsInt64(); err == nil && n > 0 {
			counts[st] = int(n)
		}
	}
	return counts, nil
}

// GetActiveJobStats returns aggregated counts by job status (alias for monitor API).
func (s *RedisStore) GetActiveJobStats(ctx context.Context) (map[types.JobStatus]int, error) {
	return s.JobStatusCounts(ctx)
}

// GetJobStats returns aggregated job statistics.
func (s *RedisStore) GetJobStats(ctx context.Context, _ StatsFilter) (*JobStats, error) {
	totalJobs, _ := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(JobsByCreatedKey(s.prefix)).Build()).AsInt64()
	totalRuns, _ := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(HistoryRunsKey(s.prefix)).Build()).AsInt64()

	statuses := []types.JobStatus{
		types.JobStatusPending, types.JobStatusScheduled, types.JobStatusRunning,
		types.JobStatusCompleted, types.JobStatusFailed, types.JobStatusRetrying,
		types.JobStatusDead, types.JobStatusCancelled,
	}

	cmds := make(rueidis.Commands, 0, len(statuses))
	for _, st := range statuses {
		cmds = append(cmds, s.rdb.B().Zcard().Key(JobsByStatusKey(s.prefix, string(st))).Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	byStatus := make(map[types.JobStatus]int64)
	for i, st := range statuses {
		if n, err := results[i].AsInt64(); err == nil && n > 0 {
			byStatus[st] = n
		}
	}

	return &JobStats{
		TotalJobs:  totalJobs,
		TotalRuns:  totalRuns,
		ByStatus:   byStatus,
		ByTaskType: make(map[types.TaskType]int64),
	}, nil
}

// ============================================================
// Schedule operations
// ============================================================

// SaveSchedule upserts a schedule and updates the due sorted set.
func (s *RedisStore) SaveSchedule(ctx context.Context, entry *types.ScheduleEntry) (uint64, error) {
	data, err := sonic.ConfigFastest.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("marshal schedule: %w", err)
	}

	cmds := make(rueidis.Commands, 0, 3)
	cmds = append(cmds, s.rdb.B().Hset().Key(ScheduleKey(s.prefix, entry.JobID)).FieldValue().FieldValue("data", string(data)).Build())
	cmds = append(cmds, s.rdb.B().Hincrby().Key(ScheduleKey(s.prefix, entry.JobID)).Field("_version").Increment(1).Build())
	cmds = append(cmds, s.rdb.B().Zadd().Key(SchedulesDueKey(s.prefix)).ScoreMember().ScoreMember(float64(entry.NextRunAt.UnixNano()), entry.JobID).Build())

	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return 0, fmt.Errorf("save schedule: %w", err)
		}
	}
	ver, _ := results[1].AsInt64()
	return uint64(ver), nil
}

// GetSchedule retrieves a schedule entry by job ID.
func (s *RedisStore) GetSchedule(ctx context.Context, jobID string) (*types.ScheduleEntry, uint64, error) {
	result, err := s.rdb.Do(ctx, s.rdb.B().Hgetall().Key(ScheduleKey(s.prefix, jobID)).Build()).AsStrMap()
	if err != nil {
		return nil, 0, fmt.Errorf("get schedule: %w", err)
	}
	if len(result) == 0 {
		return nil, 0, types.ErrScheduleNotFound
	}

	var sched types.ScheduleEntry
	if err := sonic.ConfigFastest.Unmarshal([]byte(result["data"]), &sched); err != nil {
		return nil, 0, fmt.Errorf("unmarshal schedule: %w", err)
	}
	return &sched, parseVersion(result["_version"]), nil
}

// DeleteSchedule removes a schedule and its index entry.
func (s *RedisStore) DeleteSchedule(ctx context.Context, jobID string) error {
	cmds := make(rueidis.Commands, 0, 2)
	cmds = append(cmds, s.rdb.B().Del().Key(ScheduleKey(s.prefix, jobID)).Build())
	cmds = append(cmds, s.rdb.B().Zrem().Key(SchedulesDueKey(s.prefix)).Member(jobID).Build())
	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return err
		}
	}
	return nil
}

// ListDueSchedules returns schedules where NextRunAt <= cutoff.
// O(log N + M) using sorted set.
func (s *RedisStore) ListDueSchedules(ctx context.Context, cutoff time.Time) ([]*types.ScheduleEntry, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrangebyscore().Key(SchedulesDueKey(s.prefix)).Min("-inf").Max(strconv.FormatInt(cutoff.UnixNano(), 10)).Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list due schedules: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateSchedules(ctx, ids)
}

// ListSchedules returns all active schedule entries.
func (s *RedisStore) ListSchedules(ctx context.Context) ([]*types.ScheduleEntry, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrange().Key(SchedulesDueKey(s.prefix)).Min("0").Max("-1").Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateSchedules(ctx, ids)
}

// ============================================================
// Run operations
// ============================================================

// SaveRun upserts a job run and maintains the active set and by-job index.
func (s *RedisStore) SaveRun(ctx context.Context, run *types.JobRun) (uint64, error) {
	data, err := sonic.ConfigFastest.Marshal(run)
	if err != nil {
		return 0, fmt.Errorf("marshal run: %w", err)
	}

	cmds := make(rueidis.Commands, 0, 6)
	cmds = append(cmds, s.rdb.B().Hset().Key(RunKey(s.prefix, run.ID)).FieldValue().FieldValue("data", string(data)).Build())
	cmds = append(cmds, s.rdb.B().Hincrby().Key(RunKey(s.prefix, run.ID)).Field("_version").Increment(1).Build())

	if !run.Status.IsTerminal() {
		cmds = append(cmds, s.rdb.B().Sadd().Key(RunsActiveKey(s.prefix)).Member(run.ID).Build())
		cmds = append(cmds, s.rdb.B().Sadd().Key(RunsActiveByJobKey(s.prefix, run.JobID)).Member(run.ID).Build())
	} else {
		cmds = append(cmds, s.rdb.B().Srem().Key(RunsActiveKey(s.prefix)).Member(run.ID).Build())
		cmds = append(cmds, s.rdb.B().Srem().Key(RunsActiveByJobKey(s.prefix, run.JobID)).Member(run.ID).Build())
	}
	cmds = append(cmds, s.rdb.B().Zadd().Key(RunsByJobKey(s.prefix, run.JobID)).ScoreMember().ScoreMember(float64(run.StartedAt.UnixNano()), run.ID).Build())

	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return 0, fmt.Errorf("save run: %w", err)
		}
	}
	ver, _ := results[1].AsInt64()
	return uint64(ver), nil
}

// GetRun retrieves a job run by ID.
func (s *RedisStore) GetRun(ctx context.Context, runID string) (*types.JobRun, uint64, error) {
	result, err := s.rdb.Do(ctx, s.rdb.B().Hgetall().Key(RunKey(s.prefix, runID)).Build()).AsStrMap()
	if err != nil {
		return nil, 0, fmt.Errorf("get run: %w", err)
	}
	if len(result) == 0 {
		return nil, 0, types.ErrRunNotFound
	}

	var run types.JobRun
	if err := sonic.ConfigFastest.Unmarshal([]byte(result["data"]), &run); err != nil {
		return nil, 0, fmt.Errorf("unmarshal run: %w", err)
	}
	return &run, parseVersion(result["_version"]), nil
}

// DeleteRun removes a run and cleans its indexes.
func (s *RedisStore) DeleteRun(ctx context.Context, runID string) error {
	dataStr, err := s.rdb.Do(ctx, s.rdb.B().Hget().Key(RunKey(s.prefix, runID)).Field("data").Build()).ToString()
	if err != nil && !rueidis.IsRedisNil(err) {
		return err
	}

	cmds := make(rueidis.Commands, 0, 5)
	cmds = append(cmds, s.rdb.B().Del().Key(RunKey(s.prefix, runID)).Build())
	cmds = append(cmds, s.rdb.B().Srem().Key(RunsActiveKey(s.prefix)).Member(runID).Build())

	if dataStr != "" {
		var run types.JobRun
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &run) == nil {
			cmds = append(cmds, s.rdb.B().Zrem().Key(RunsByJobKey(s.prefix, run.JobID)).Member(runID).Build())
			cmds = append(cmds, s.rdb.B().Srem().Key(RunsActiveByJobKey(s.prefix, run.JobID)).Member(runID).Build())
		}
	}

	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return err
		}
	}
	return nil
}

// ListRuns returns all active (non-terminal) runs.
func (s *RedisStore) ListRuns(ctx context.Context) ([]*types.JobRun, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Smembers().Key(RunsActiveKey(s.prefix)).Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list active runs: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateRuns(ctx, ids)
}

// ListActiveRunsByJobID returns all active runs belonging to a specific job.
// Uses the per-job active run set (RunsActiveByJobKey) for O(per-job) lookup.
func (s *RedisStore) ListActiveRunsByJobID(ctx context.Context, jobID string) ([]*types.JobRun, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Smembers().Key(RunsActiveByJobKey(s.prefix, jobID)).Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list active runs by job: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateRuns(ctx, ids)
}

// CancelChannel returns the Redis Pub/Sub channel name used for task cancellation signals.
func (s *RedisStore) CancelChannel() string {
	return CancelChannelKey(s.prefix)
}

// RequestCancel sets a durable cancel flag for a run and publishes a Pub/Sub
// signal for immediate delivery. The flag has a 24-hour TTL for auto-cleanup.
// This dual-write pattern ensures cancellation survives worker disconnects:
// Pub/Sub is the fast path; the persisted flag is the durable fallback.
func (s *RedisStore) RequestCancel(ctx context.Context, runID string) error {
	flagKey := CancelFlagKey(s.prefix, runID)
	// SET with 24h TTL — auto-cleanup for completed runs
	setCmd := s.rdb.B().Set().Key(flagKey).Value("1").Ex(24 * time.Hour).Build()
	pubCmd := s.rdb.B().Publish().Channel(CancelChannelKey(s.prefix)).Message(runID).Build()
	results := s.rdb.DoMulti(ctx, setCmd, pubCmd)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return fmt.Errorf("request cancel for run %s: %w", runID, err)
		}
	}
	return nil
}

// IsCancelRequested checks the durable cancel flag for a run.
func (s *RedisStore) IsCancelRequested(ctx context.Context, runID string) (bool, error) {
	flagKey := CancelFlagKey(s.prefix, runID)
	result := s.rdb.Do(ctx, s.rdb.B().Exists().Key(flagKey).Build())
	count, err := result.AsInt64()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// ClearCancelFlag removes the durable cancel flag after a run completes.
func (s *RedisStore) ClearCancelFlag(ctx context.Context, runID string) {
	s.rdb.Do(ctx, s.rdb.B().Del().Key(CancelFlagKey(s.prefix, runID)).Build())
}

// HasActiveRunForJob returns true if any non-terminal run exists for the given jobID.
// Uses the per-job active run set for O(1) check.
func (s *RedisStore) HasActiveRunForJob(ctx context.Context, jobID string) (bool, error) {
	count, err := s.rdb.Do(ctx, s.rdb.B().Scard().Key(RunsActiveByJobKey(s.prefix, jobID)).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("check active runs for job: %w", err)
	}
	return count > 0, nil
}

// ============================================================
// Overlap buffer operations (schedule overlap policy)
// ============================================================

// SaveOverlapBuffer stores a buffered dispatch timestamp for BUFFER_ONE policy.
// Overwrites any previous buffered entry.
func (s *RedisStore) SaveOverlapBuffer(ctx context.Context, jobID string, nextRunAt time.Time) error {
	return s.rdb.Do(ctx, s.rdb.B().Set().Key(OverlapBufferedKey(s.prefix, jobID)).Value(strconv.FormatInt(nextRunAt.UnixNano(), 10)).Ex(s.cfg.OverlapBufferTTL).Build()).Error()
}

// PopOverlapBuffer retrieves and deletes the buffered dispatch for BUFFER_ONE.
func (s *RedisStore) PopOverlapBuffer(ctx context.Context, jobID string) (time.Time, bool, error) {
	val, err := s.rdb.Do(ctx, s.rdb.B().Getdel().Key(OverlapBufferedKey(s.prefix, jobID)).Build()).ToString()
	if rueidis.IsRedisNil(err) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("pop overlap buffer: %w", err)
	}
	nanos, _ := strconv.ParseInt(val, 10, 64)
	return time.Unix(0, nanos), true, nil
}

// PushOverlapBufferAll appends to the BUFFER_ALL queue.
func (s *RedisStore) PushOverlapBufferAll(ctx context.Context, jobID string, nextRunAt time.Time) error {
	key := OverlapBufferListKey(s.prefix, jobID)
	cmds := make(rueidis.Commands, 0, 2)
	cmds = append(cmds, s.rdb.B().Rpush().Key(key).Element(strconv.FormatInt(nextRunAt.UnixNano(), 10)).Build())
	cmds = append(cmds, s.rdb.B().Expire().Key(key).Seconds(int64(s.cfg.OverlapBufferTTL.Seconds())).Build())
	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return err
		}
	}
	return nil
}

// PopOverlapBufferAll pops the next buffered dispatch from BUFFER_ALL.
func (s *RedisStore) PopOverlapBufferAll(ctx context.Context, jobID string) (time.Time, bool, error) {
	val, err := s.rdb.Do(ctx, s.rdb.B().Lpop().Key(OverlapBufferListKey(s.prefix, jobID)).Build()).ToString()
	if rueidis.IsRedisNil(err) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("pop overlap buffer all: %w", err)
	}
	nanos, _ := strconv.ParseInt(val, 10, 64)
	return time.Unix(0, nanos), true, nil
}

// ============================================================
// Node operations
// ============================================================

// SaveNode upserts a node with TTL for automatic dead-node cleanup.
func (s *RedisStore) SaveNode(ctx context.Context, node *types.NodeInfo) (uint64, error) {
	data, err := sonic.ConfigFastest.Marshal(node)
	if err != nil {
		return 0, fmt.Errorf("marshal node: %w", err)
	}

	key := NodeKey(s.prefix, node.NodeID)
	cmds := make(rueidis.Commands, 0, 3)
	cmds = append(cmds, s.rdb.B().Hset().Key(key).FieldValue().FieldValue("data", string(data)).FieldValue("_version", "1").Build())
	cmds = append(cmds, s.rdb.B().Expire().Key(key).Seconds(int64(s.cfg.NodeTTL.Seconds())).Build())
	cmds = append(cmds, s.rdb.B().Sadd().Key(NodesAllKey(s.prefix)).Member(node.NodeID).Build())

	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return 0, fmt.Errorf("save node: %w", err)
		}
	}
	return 1, nil
}

// GetNode retrieves a node by ID, returns nil if not found or TTL expired.
func (s *RedisStore) GetNode(ctx context.Context, nodeID string) (*types.NodeInfo, error) {
	result, err := s.rdb.Do(ctx, s.rdb.B().Hgetall().Key(NodeKey(s.prefix, nodeID)).Build()).AsStrMap()
	if err != nil || len(result) == 0 {
		return nil, nil
	}

	var node types.NodeInfo
	if err := sonic.ConfigFastest.Unmarshal([]byte(result["data"]), &node); err != nil {
		return nil, fmt.Errorf("unmarshal node: %w", err)
	}
	return &node, nil
}

// ListNodes returns all live nodes, pruning stale entries from the tracking set.
func (s *RedisStore) ListNodes(ctx context.Context) ([]*types.NodeInfo, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Smembers().Key(NodesAllKey(s.prefix)).Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list node IDs: %w", err)
	}

	var nodes []*types.NodeInfo
	var staleIDs []string

	for _, id := range ids {
		result, err := s.rdb.Do(ctx, s.rdb.B().Hgetall().Key(NodeKey(s.prefix, id)).Build()).AsStrMap()
		if err != nil || len(result) == 0 {
			staleIDs = append(staleIDs, id)
			continue
		}
		var node types.NodeInfo
		if err := sonic.ConfigFastest.Unmarshal([]byte(result["data"]), &node); err != nil {
			continue
		}
		nodes = append(nodes, &node)
	}

	// Prune stale entries whose hash keys have expired.
	if len(staleIDs) > 0 {
		cmds := make(rueidis.Commands, 0, len(staleIDs))
		for _, id := range staleIDs {
			cmds = append(cmds, s.rdb.B().Srem().Key(NodesAllKey(s.prefix)).Member(id).Build())
		}
		s.rdb.DoMulti(ctx, cmds...)
	}
	return nodes, nil
}

// ============================================================
// Unique key deduplication
// ============================================================

// CheckUniqueKey checks whether a unique key is claimed. Returns (jobID, exists, error).
func (s *RedisStore) CheckUniqueKey(ctx context.Context, uniqueKey string) (string, bool, error) {
	val, err := s.rdb.Do(ctx, s.rdb.B().Get().Key(UniqueKeyKey(s.prefix, uniqueKey)).Build()).ToString()
	if rueidis.IsRedisNil(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return val, true, nil
}

// SetUniqueKey atomically claims a unique key for a job ID.
func (s *RedisStore) SetUniqueKey(ctx context.Context, uniqueKey, jobID string) error {
	err := s.rdb.Do(ctx, s.rdb.B().Set().Key(UniqueKeyKey(s.prefix, uniqueKey)).Value(jobID).Nx().Build()).Error()
	if err == nil {
		return nil
	}
	if rueidis.IsRedisNil(err) {
		return types.ErrDuplicateJob
	}
	return err
}

// DeleteUniqueKey releases a unique key.
func (s *RedisStore) DeleteUniqueKey(ctx context.Context, uniqueKey string) error {
	return s.rdb.Do(ctx, s.rdb.B().Del().Key(UniqueKeyKey(s.prefix, uniqueKey)).Build()).Error()
}

// ============================================================
// Workflow operations
// ============================================================

// SaveWorkflow upserts a workflow instance and maintains sorted-set indexes.
func (s *RedisStore) SaveWorkflow(ctx context.Context, wf *types.WorkflowInstance) (uint64, error) {
	data, err := sonic.ConfigFastest.Marshal(wf)
	if err != nil {
		return 0, fmt.Errorf("marshal workflow: %w", err)
	}

	key := WorkflowKey(s.prefix, wf.ID)
	oldStatus := hashFieldFromJSON(ctx, s.rdb, key, "status")

	cmds := make(rueidis.Commands, 0, 6)
	cmds = append(cmds, s.rdb.B().Hset().Key(key).FieldValue().FieldValue("data", string(data)).Build())
	incrIdx := len(cmds)
	cmds = append(cmds, s.rdb.B().Hincrby().Key(key).Field("_version").Increment(1).Build())

	score := float64(wf.CreatedAt.UnixNano())
	cmds = append(cmds, s.rdb.B().Zadd().Key(WorkflowsByCreatedKey(s.prefix)).ScoreMember().ScoreMember(score, wf.ID).Build())
	if oldStatus != "" && oldStatus != string(wf.Status) {
		cmds = append(cmds, s.rdb.B().Zrem().Key(WorkflowsByStatusKey(s.prefix, oldStatus)).Member(wf.ID).Build())
	}
	cmds = append(cmds, s.rdb.B().Zadd().Key(WorkflowsByStatusKey(s.prefix, string(wf.Status))).ScoreMember().ScoreMember(score, wf.ID).Build())

	// Deadline index: track workflows with deadlines for efficient timeout checks.
	if wf.Deadline != nil {
		cmds = append(cmds, s.rdb.B().Zadd().Key(WorkflowDeadlinesKey(s.prefix)).ScoreMember().ScoreMember(float64(wf.Deadline.Unix()), wf.ID).Build())
	}

	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return 0, fmt.Errorf("save workflow: %w", err)
		}
	}
	ver, _ := results[incrIdx].AsInt64()
	s.mirrorWorkflowPayloads(ctx, wf) // best-effort JSON mirror for JSONPath search
	return uint64(ver), nil
}

// GetWorkflow retrieves a workflow instance by ID.
func (s *RedisStore) GetWorkflow(ctx context.Context, id string) (*types.WorkflowInstance, uint64, error) {
	result, err := s.rdb.Do(ctx, s.rdb.B().Hgetall().Key(WorkflowKey(s.prefix, id)).Build()).AsStrMap()
	if err != nil {
		return nil, 0, fmt.Errorf("get workflow: %w", err)
	}
	if len(result) == 0 {
		return nil, 0, types.ErrWorkflowNotFound
	}

	var wf types.WorkflowInstance
	if err := sonic.ConfigFastest.Unmarshal([]byte(result["data"]), &wf); err != nil {
		return nil, 0, fmt.Errorf("unmarshal workflow: %w", err)
	}
	return &wf, parseVersion(result["_version"]), nil
}

// UpdateWorkflow performs a CAS update on a workflow instance.
func (s *RedisStore) UpdateWorkflow(ctx context.Context, wf *types.WorkflowInstance, expectedRev uint64) (uint64, error) {
	key := WorkflowKey(s.prefix, wf.ID)
	oldStatus := hashFieldFromJSON(ctx, s.rdb, key, "status")

	data, err := sonic.ConfigFastest.Marshal(wf)
	if err != nil {
		return 0, fmt.Errorf("marshal workflow: %w", err)
	}

	result, err := s.scriptCASUpdate.Exec(ctx, s.rdb,
		[]string{key},
		[]string{
			strconv.FormatUint(expectedRev, 10),
			string(data),
			strconv.FormatUint(expectedRev+1, 10),
		},
	).ToString()
	if err != nil {
		if strings.Contains(err.Error(), "CAS_CONFLICT") {
			return 0, ErrCASConflict
		}
		return 0, fmt.Errorf("update workflow (CAS): %w", err)
	}

	ver, _ := strconv.ParseUint(result, 10, 64)

	// Update status index (remove old, add new).
	score := float64(wf.CreatedAt.UnixNano())
	if oldStatus != "" && oldStatus != string(wf.Status) {
		s.rdb.Do(ctx, s.rdb.B().Zrem().Key(WorkflowsByStatusKey(s.prefix, oldStatus)).Member(wf.ID).Build())
	}
	s.rdb.Do(ctx, s.rdb.B().Zadd().Key(WorkflowsByStatusKey(s.prefix, string(wf.Status))).ScoreMember().ScoreMember(score, wf.ID).Build())

	// Remove from deadline index when terminal (no longer needs timeout checks).
	if wf.Status.IsTerminal() {
		s.rdb.Do(ctx, s.rdb.B().Zrem().Key(WorkflowDeadlinesKey(s.prefix)).Member(wf.ID).Build())
	}

	s.mirrorWorkflowPayloads(ctx, wf) // best-effort JSON mirror for JSONPath search
	return ver, nil
}

// CompactWorkflowTasks moves completed tasks exceeding the threshold to the archive hash.
// Only non-terminal + the most recent `keep` completed tasks remain in the main Tasks map.
func (s *RedisStore) CompactWorkflowTasks(ctx context.Context, wf *types.WorkflowInstance, keep int) (int, error) {
	if keep <= 0 {
		keep = 50
	}

	// Collect completed tasks sorted by finish time.
	type completedTask struct {
		name       string
		state      types.WorkflowTaskState
		finishedAt time.Time
	}
	var completed []completedTask
	for name, state := range wf.Tasks {
		if state.Status == types.JobStatusCompleted && state.FinishedAt != nil {
			completed = append(completed, completedTask{name: name, state: state, finishedAt: *state.FinishedAt})
		}
	}

	if len(completed) <= keep {
		return 0, nil
	}

	// Sort by finish time ascending (oldest first).
	for i := 0; i < len(completed); i++ {
		for j := i + 1; j < len(completed); j++ {
			if completed[j].finishedAt.Before(completed[i].finishedAt) {
				completed[i], completed[j] = completed[j], completed[i]
			}
		}
	}

	// Archive the oldest completed tasks beyond the keep threshold.
	toArchive := completed[:len(completed)-keep]
	archiveKey := WorkflowArchiveKey(s.prefix, wf.ID)

	cmds := make(rueidis.Commands, 0, len(toArchive))
	for _, t := range toArchive {
		data, err := sonic.ConfigFastest.Marshal(t.state)
		if err != nil {
			continue
		}
		cmds = append(cmds, s.rdb.B().Hset().Key(archiveKey).FieldValue().FieldValue(t.name, string(data)).Build())
		delete(wf.Tasks, t.name)
	}

	if len(cmds) > 0 {
		for _, resp := range s.rdb.DoMulti(ctx, cmds...) {
			if err := resp.Error(); err != nil {
				return 0, fmt.Errorf("archive workflow tasks: %w", err)
			}
		}
	}

	wf.ArchivedTaskCount += len(toArchive)
	return len(toArchive), nil
}

// GetArchivedTask retrieves a single archived task state from the archive hash.
func (s *RedisStore) GetArchivedTask(ctx context.Context, wfID, taskName string) (*types.WorkflowTaskState, error) {
	data, err := s.rdb.Do(ctx, s.rdb.B().Hget().Key(WorkflowArchiveKey(s.prefix, wfID)).Field(taskName).Build()).ToString()
	if err != nil {
		return nil, err
	}
	var state types.WorkflowTaskState
	if err := sonic.ConfigFastest.Unmarshal([]byte(data), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// TrimWorkflowEvents trims a workflow's event index to the most recent maxEntries.
func (s *RedisStore) TrimWorkflowEvents(ctx context.Context, wfID string, maxEntries int64) error {
	key := WorkflowEventsKey(s.prefix, wfID)
	count, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(key).Build()).ToInt64()
	if err != nil || count <= maxEntries {
		return nil
	}
	// Remove oldest entries (lowest scores) beyond the limit.
	removeCount := count - maxEntries
	return s.rdb.Do(ctx, s.rdb.B().Zremrangebyrank().Key(key).Start(0).Stop(removeCount-1).Build()).Error()
}

// DeleteWorkflow removes a workflow instance, all indexes, and associated data
// (signal stream, event index, task archive, JSON mirror).
func (s *RedisStore) DeleteWorkflow(ctx context.Context, id string) error {
	oldStatus := hashFieldFromJSON(ctx, s.rdb, WorkflowKey(s.prefix, id), "status")

	cmds := make(rueidis.Commands, 0, 8)
	cmds = append(cmds, s.rdb.B().Del().Key(WorkflowKey(s.prefix, id)).Build())
	cmds = append(cmds, s.rdb.B().Zrem().Key(WorkflowsByCreatedKey(s.prefix)).Member(id).Build())
	cmds = append(cmds, s.rdb.B().Zrem().Key(WorkflowDeadlinesKey(s.prefix)).Member(id).Build())
	// Deep cleanup: signal stream, event index, task archive.
	cmds = append(cmds, s.rdb.B().Del().Key(WorkflowSignalStreamKey(s.prefix, id)).Build())
	cmds = append(cmds, s.rdb.B().Del().Key(WorkflowEventsKey(s.prefix, id)).Build())
	cmds = append(cmds, s.rdb.B().Del().Key(WorkflowArchiveKey(s.prefix, id)).Build())
	if oldStatus != "" {
		cmds = append(cmds, s.rdb.B().Zrem().Key(WorkflowsByStatusKey(s.prefix, oldStatus)).Member(id).Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)
	var firstErr error
	for _, r := range results {
		if err := r.Error(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.rdb.Do(bgCtx, s.rdb.B().JsonDel().Key(WorkflowPayloadKey(s.prefix, id)).Path("$").Build())
	}()
	return firstErr
}

// ListWorkflows returns all workflow instances.
func (s *RedisStore) ListWorkflows(ctx context.Context) ([]*types.WorkflowInstance, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrange().Key(WorkflowsByCreatedKey(s.prefix)).Min("0").Max("-1").Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateWorkflows(ctx, ids)
}

// ListDueWorkflowDeadlines returns workflows whose deadlines have passed.
// Uses the deadline sorted set for O(due) instead of O(all-running).
func (s *RedisStore) ListDueWorkflowDeadlines(ctx context.Context, now time.Time) ([]*types.WorkflowInstance, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrangebyscore().Key(WorkflowDeadlinesKey(s.prefix)).Min("0").Max(strconv.FormatFloat(float64(now.Unix()), 'f', 0, 64)).Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list due workflow deadlines: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateWorkflows(ctx, ids)
}

// ListDueBatchDeadlines returns batches whose deadlines have passed.
// Uses the deadline sorted set for O(due) instead of O(all-running).
func (s *RedisStore) ListDueBatchDeadlines(ctx context.Context, now time.Time) ([]*types.BatchInstance, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrangebyscore().Key(BatchDeadlinesKey(s.prefix)).Min("0").Max(strconv.FormatFloat(float64(now.Unix()), 'f', 0, 64)).Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list due batch deadlines: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateBatches(ctx, ids)
}

// ListWorkflowsByStatus returns workflow instances with the given status.
func (s *RedisStore) ListWorkflowsByStatus(ctx context.Context, status types.WorkflowStatus) ([]*types.WorkflowInstance, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrange().Key(WorkflowsByStatusKey(s.prefix, string(status))).Min("0").Max("-1").Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list workflows by status: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateWorkflows(ctx, ids)
}

// ============================================================
// Batch operations
// ============================================================

// SaveBatch upserts a batch instance and maintains sorted-set indexes.
func (s *RedisStore) SaveBatch(ctx context.Context, batch *types.BatchInstance) (uint64, error) {
	data, err := sonic.ConfigFastest.Marshal(batch)
	if err != nil {
		return 0, fmt.Errorf("marshal batch: %w", err)
	}

	key := BatchKey(s.prefix, batch.ID)
	oldStatus := hashFieldFromJSON(ctx, s.rdb, key, "status")

	cmds := make(rueidis.Commands, 0, 6)
	cmds = append(cmds, s.rdb.B().Hset().Key(key).FieldValue().FieldValue("data", string(data)).Build())
	incrIdx := len(cmds)
	cmds = append(cmds, s.rdb.B().Hincrby().Key(key).Field("_version").Increment(1).Build())

	score := float64(batch.CreatedAt.UnixNano())
	cmds = append(cmds, s.rdb.B().Zadd().Key(BatchesByCreatedKey(s.prefix)).ScoreMember().ScoreMember(score, batch.ID).Build())
	if oldStatus != "" && oldStatus != string(batch.Status) {
		cmds = append(cmds, s.rdb.B().Zrem().Key(BatchesByStatusKey(s.prefix, oldStatus)).Member(batch.ID).Build())
	}
	cmds = append(cmds, s.rdb.B().Zadd().Key(BatchesByStatusKey(s.prefix, string(batch.Status))).ScoreMember().ScoreMember(score, batch.ID).Build())

	// Deadline index: track batches with deadlines for efficient timeout checks.
	if batch.Deadline != nil {
		cmds = append(cmds, s.rdb.B().Zadd().Key(BatchDeadlinesKey(s.prefix)).ScoreMember().ScoreMember(float64(batch.Deadline.Unix()), batch.ID).Build())
	}

	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return 0, fmt.Errorf("save batch: %w", err)
		}
	}
	ver, _ := results[incrIdx].AsInt64()
	s.mirrorBatchPayloads(ctx, batch) // best-effort JSON mirror for JSONPath search
	return uint64(ver), nil
}

// GetBatch retrieves a batch instance by ID.
func (s *RedisStore) GetBatch(ctx context.Context, batchID string) (*types.BatchInstance, uint64, error) {
	result, err := s.rdb.Do(ctx, s.rdb.B().Hgetall().Key(BatchKey(s.prefix, batchID)).Build()).AsStrMap()
	if err != nil {
		return nil, 0, fmt.Errorf("get batch: %w", err)
	}
	if len(result) == 0 {
		return nil, 0, types.ErrBatchNotFound
	}

	var batch types.BatchInstance
	if err := sonic.ConfigFastest.Unmarshal([]byte(result["data"]), &batch); err != nil {
		return nil, 0, fmt.Errorf("unmarshal batch: %w", err)
	}
	return &batch, parseVersion(result["_version"]), nil
}

// UpdateBatch performs a CAS update on a batch instance.
func (s *RedisStore) UpdateBatch(ctx context.Context, batch *types.BatchInstance, expectedRev uint64) (uint64, error) {
	key := BatchKey(s.prefix, batch.ID)
	oldStatus := hashFieldFromJSON(ctx, s.rdb, key, "status")

	data, err := sonic.ConfigFastest.Marshal(batch)
	if err != nil {
		return 0, fmt.Errorf("marshal batch: %w", err)
	}

	result, err := s.scriptCASUpdate.Exec(ctx, s.rdb,
		[]string{key},
		[]string{
			strconv.FormatUint(expectedRev, 10),
			string(data),
			strconv.FormatUint(expectedRev+1, 10),
		},
	).ToString()
	if err != nil {
		if strings.Contains(err.Error(), "CAS_CONFLICT") {
			return 0, ErrCASConflict
		}
		return 0, fmt.Errorf("update batch (CAS): %w", err)
	}

	ver, _ := strconv.ParseUint(result, 10, 64)

	// Update status index (remove old, add new).
	score := float64(batch.CreatedAt.UnixNano())
	if oldStatus != "" && oldStatus != string(batch.Status) {
		s.rdb.Do(ctx, s.rdb.B().Zrem().Key(BatchesByStatusKey(s.prefix, oldStatus)).Member(batch.ID).Build())
	}
	s.rdb.Do(ctx, s.rdb.B().Zadd().Key(BatchesByStatusKey(s.prefix, string(batch.Status))).ScoreMember().ScoreMember(score, batch.ID).Build())

	// Remove from deadline index when terminal (no longer needs timeout checks).
	if batch.Status.IsTerminal() {
		s.rdb.Do(ctx, s.rdb.B().Zrem().Key(BatchDeadlinesKey(s.prefix)).Member(batch.ID).Build())
	}

	s.mirrorBatchPayloads(ctx, batch) // best-effort JSON mirror for JSONPath search
	return ver, nil
}

// DeleteBatch removes a batch instance, all indexes, and associated data
// (batch item results, JSON mirror).
func (s *RedisStore) DeleteBatch(ctx context.Context, batchID string) error {
	oldStatus := hashFieldFromJSON(ctx, s.rdb, BatchKey(s.prefix, batchID), "status")

	cmds := make(rueidis.Commands, 0, 6)
	cmds = append(cmds, s.rdb.B().Del().Key(BatchKey(s.prefix, batchID)).Build())
	cmds = append(cmds, s.rdb.B().Zrem().Key(BatchesByCreatedKey(s.prefix)).Member(batchID).Build())
	cmds = append(cmds, s.rdb.B().Zrem().Key(BatchDeadlinesKey(s.prefix)).Member(batchID).Build())
	if oldStatus != "" {
		cmds = append(cmds, s.rdb.B().Zrem().Key(BatchesByStatusKey(s.prefix, oldStatus)).Member(batchID).Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)
	var firstErr error
	for _, r := range results {
		if err := r.Error(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// Deep cleanup: batch item results + JSON mirror (async, best-effort).
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Delete individual batch result keys listed in the results set.
		resultsSetKey := BatchResultsSetKey(s.prefix, batchID)
		itemIDs, err := s.rdb.Do(bgCtx, s.rdb.B().Smembers().Key(resultsSetKey).Build()).AsStrSlice()
		if err == nil && len(itemIDs) > 0 {
			delCmds := make(rueidis.Commands, 0, len(itemIDs)+1)
			for _, itemID := range itemIDs {
				delCmds = append(delCmds, s.rdb.B().Del().Key(BatchResultKey(s.prefix, batchID, itemID)).Build())
			}
			delCmds = append(delCmds, s.rdb.B().Del().Key(resultsSetKey).Build())
			s.rdb.DoMulti(bgCtx, delCmds...)
		}
		s.rdb.Do(bgCtx, s.rdb.B().JsonDel().Key(BatchPayloadKey(s.prefix, batchID)).Path("$").Build())
	}()
	return firstErr
}

// ListBatches returns all batch instances.
func (s *RedisStore) ListBatches(ctx context.Context) ([]*types.BatchInstance, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrange().Key(BatchesByCreatedKey(s.prefix)).Min("0").Max("-1").Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list batches: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateBatches(ctx, ids)
}

// ListBatchesByStatus returns batch instances with the given status.
func (s *RedisStore) ListBatchesByStatus(ctx context.Context, status types.WorkflowStatus) ([]*types.BatchInstance, error) {
	ids, err := s.rdb.Do(ctx, s.rdb.B().Zrange().Key(BatchesByStatusKey(s.prefix, string(status))).Min("0").Max("-1").Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list batches by status: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.hydrateBatches(ctx, ids)
}

// ============================================================
// Batch item result operations
// ============================================================

// SaveBatchItemResult stores a single batch item result.
func (s *RedisStore) SaveBatchItemResult(ctx context.Context, result *types.BatchItemResult) error {
	data, err := sonic.ConfigFastest.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal batch item result: %w", err)
	}

	cmds := make(rueidis.Commands, 0, 2)
	cmds = append(cmds, s.rdb.B().Hset().Key(BatchResultKey(s.prefix, result.BatchID, result.ItemID)).FieldValue().FieldValue("data", string(data)).Build())
	cmds = append(cmds, s.rdb.B().Sadd().Key(BatchResultsSetKey(s.prefix, result.BatchID)).Member(result.ItemID).Build())
	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return err
		}
	}
	return nil
}

// GetBatchItemResult retrieves a single batch item result.
func (s *RedisStore) GetBatchItemResult(ctx context.Context, batchID, itemID string) (*types.BatchItemResult, error) {
	dataStr, err := s.rdb.Do(ctx, s.rdb.B().Hget().Key(BatchResultKey(s.prefix, batchID, itemID)).Field("data").Build()).ToString()
	if rueidis.IsRedisNil(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get batch item result: %w", err)
	}

	var result types.BatchItemResult
	if err := sonic.ConfigFastest.Unmarshal([]byte(dataStr), &result); err != nil {
		return nil, fmt.Errorf("unmarshal batch item result: %w", err)
	}
	return &result, nil
}

// ListBatchItemResults returns all item results for a batch.
func (s *RedisStore) ListBatchItemResults(ctx context.Context, batchID string) ([]*types.BatchItemResult, error) {
	itemIDs, err := s.rdb.Do(ctx, s.rdb.B().Smembers().Key(BatchResultsSetKey(s.prefix, batchID)).Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list batch result IDs: %w", err)
	}
	if len(itemIDs) == 0 {
		return nil, nil
	}

	cmds := make(rueidis.Commands, 0, len(itemIDs))
	for _, itemID := range itemIDs {
		cmds = append(cmds, s.rdb.B().Hget().Key(BatchResultKey(s.prefix, batchID, itemID)).Field("data").Build())
	}
	pipeResults := s.rdb.DoMulti(ctx, cmds...)

	var results []*types.BatchItemResult
	for _, r := range pipeResults {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var result types.BatchItemResult
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &result) == nil {
			results = append(results, &result)
		}
	}
	return results, nil
}

// ============================================================
// DLQ operations
// ============================================================

// ListDLQ reads recent messages from the DLQ stream.
func (s *RedisStore) ListDLQ(ctx context.Context, limit int) ([]*types.WorkMessage, error) {
	if limit <= 0 {
		limit = 50
	}

	msgs, err := s.rdb.Do(ctx, s.rdb.B().Xrevrange().Key(DLQStreamKey(s.prefix)).End("+").Start("-").Count(int64(limit)).Build()).AsXRange()
	if err != nil {
		return nil, fmt.Errorf("list DLQ: %w", err)
	}

	var result []*types.WorkMessage
	for _, msg := range msgs {
		wm := WorkMessageFromStreamValues(msg.FieldValues)
		result = append(result, &wm)
	}
	return result, nil
}

// ============================================================
// History: Job Runs
// ============================================================

// SaveJobRun persists a job run to the history indexes.
func (s *RedisStore) SaveJobRun(ctx context.Context, run *types.JobRun) error {
	data, err := sonic.ConfigFastest.Marshal(run)
	if err != nil {
		return fmt.Errorf("marshal job run: %w", err)
	}

	score := float64(run.StartedAt.UnixNano())
	cmds := make(rueidis.Commands, 0, 4)
	cmds = append(cmds, s.rdb.B().Hset().Key(HistoryRunKey(s.prefix, run.ID)).FieldValue().FieldValue("data", string(data)).Build())
	cmds = append(cmds, s.rdb.B().Zadd().Key(HistoryRunsKey(s.prefix)).ScoreMember().ScoreMember(score, run.ID).Build())
	cmds = append(cmds, s.rdb.B().Zadd().Key(HistoryRunsByJobKey(s.prefix, run.JobID)).ScoreMember().ScoreMember(score, run.ID).Build())
	cmds = append(cmds, s.rdb.B().Zadd().Key(HistoryRunsByStatusKey(s.prefix, string(run.Status))).ScoreMember().ScoreMember(score, run.ID).Build())

	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return err
		}
	}
	return nil
}

// GetHistoryRun retrieves a run from the history store by run ID.
func (s *RedisStore) GetHistoryRun(ctx context.Context, runID string) (*types.JobRun, uint64, error) {
	result, err := s.rdb.Do(ctx, s.rdb.B().Hgetall().Key(HistoryRunKey(s.prefix, runID)).Build()).AsStrMap()
	if err != nil {
		return nil, 0, fmt.Errorf("get history run: %w", err)
	}
	if len(result) == 0 {
		return nil, 0, types.ErrRunNotFound
	}

	var run types.JobRun
	if err := sonic.ConfigFastest.Unmarshal([]byte(result["data"]), &run); err != nil {
		return nil, 0, fmt.Errorf("unmarshal history run: %w", err)
	}
	return &run, parseVersion(result["_version"]), nil
}

// ============================================================
// History: Job Events
// ============================================================

// SaveJobEvent persists a job event to the history indexes.
func (s *RedisStore) SaveJobEvent(ctx context.Context, event *types.JobEvent) error {
	data, err := sonic.ConfigFastest.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal job event: %w", err)
	}

	eventID := fmt.Sprintf("%s_%s_%d", event.JobID, string(event.Type), event.Timestamp.UnixNano())
	score := float64(event.Timestamp.UnixNano())

	cmds := make(rueidis.Commands, 0, 4)
	cmds = append(cmds, s.rdb.B().Hset().Key(HistoryEventKey(s.prefix, eventID)).FieldValue().FieldValue("data", string(data)).Build())
	cmds = append(cmds, s.rdb.B().Zadd().Key(HistoryEventsKey(s.prefix)).ScoreMember().ScoreMember(score, eventID).Build())
	if event.JobID != "" {
		cmds = append(cmds, s.rdb.B().Zadd().Key(HistoryEventsByJobKey(s.prefix, event.JobID)).ScoreMember().ScoreMember(score, eventID).Build())
	}
	cmds = append(cmds, s.rdb.B().Zadd().Key(HistoryEventsByTypeKey(s.prefix, string(event.Type))).ScoreMember().ScoreMember(score, eventID).Build())

	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return err
		}
	}
	return nil
}

// ============================================================
// Cleanup
// ============================================================

// Cleanup removes history entries older than the given time.
func (s *RedisStore) Cleanup(ctx context.Context, olderThan time.Time) (int64, error) {
	cutoff := strconv.FormatFloat(float64(olderThan.UnixNano()), 'f', 0, 64)
	var cleaned int64

	// Clean old history runs.
	runIDs, err := s.rdb.Do(ctx, s.rdb.B().Zrangebyscore().Key(HistoryRunsKey(s.prefix)).Min("-inf").Max(cutoff).Build()).AsStrSlice()
	if err != nil {
		return 0, err
	}
	if len(runIDs) > 0 {
		// Read run data to extract JobID/Status for secondary index cleanup.
		readCmds := make(rueidis.Commands, 0, len(runIDs))
		for _, id := range runIDs {
			readCmds = append(readCmds, s.rdb.B().Hget().Key(HistoryRunKey(s.prefix, id)).Field("data").Build())
		}
		readResults := s.rdb.DoMulti(ctx, readCmds...)

		for i, id := range runIDs {
			cmds := make(rueidis.Commands, 0, 5)
			cmds = append(cmds, s.rdb.B().Del().Key(HistoryRunKey(s.prefix, id)).Build())
			cmds = append(cmds, s.rdb.B().Zrem().Key(HistoryRunsKey(s.prefix)).Member(id).Build())

			// Clean secondary indexes if we can read the run data.
			if dataStr, err := readResults[i].ToString(); err == nil {
				var run types.JobRun
				if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &run) == nil {
					if run.JobID != "" {
						cmds = append(cmds, s.rdb.B().Zrem().Key(HistoryRunsByJobKey(s.prefix, run.JobID)).Member(id).Build())
					}
					if run.Status != "" {
						cmds = append(cmds, s.rdb.B().Zrem().Key(HistoryRunsByStatusKey(s.prefix, string(run.Status))).Member(id).Build())
					}
				}
			}

			s.rdb.DoMulti(ctx, cmds...)
			cleaned++
		}
	}

	// Clean old history events.
	eventIDs, err := s.rdb.Do(ctx, s.rdb.B().Zrangebyscore().Key(HistoryEventsKey(s.prefix)).Min("-inf").Max(cutoff).Build()).AsStrSlice()
	if err != nil {
		return cleaned, err
	}
	if len(eventIDs) > 0 {
		// Read event data to extract JobID/Type for secondary index cleanup.
		readCmds := make(rueidis.Commands, 0, len(eventIDs))
		for _, id := range eventIDs {
			readCmds = append(readCmds, s.rdb.B().Hget().Key(HistoryEventKey(s.prefix, id)).Field("data").Build())
		}
		readResults := s.rdb.DoMulti(ctx, readCmds...)

		for i, id := range eventIDs {
			cmds := make(rueidis.Commands, 0, 5)
			cmds = append(cmds, s.rdb.B().Del().Key(HistoryEventKey(s.prefix, id)).Build())
			cmds = append(cmds, s.rdb.B().Zrem().Key(HistoryEventsKey(s.prefix)).Member(id).Build())

			// Clean secondary indexes if we can read the event data.
			if dataStr, err := readResults[i].ToString(); err == nil {
				var event types.JobEvent
				if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &event) == nil {
					if event.JobID != "" {
						cmds = append(cmds, s.rdb.B().Zrem().Key(HistoryEventsByJobKey(s.prefix, event.JobID)).Member(id).Build())
					}
					if event.Type != "" {
						cmds = append(cmds, s.rdb.B().Zrem().Key(HistoryEventsByTypeKey(s.prefix, string(event.Type))).Member(id).Build())
					}
				}
			}

			s.rdb.DoMulti(ctx, cmds...)
			cleaned++
		}
	}

	return cleaned, nil
}

// ============================================================
// Hydration helpers — DoMulti HGET for bulk reads
// ============================================================

func (s *RedisStore) hydrateJobs(ctx context.Context, ids []string) ([]*types.Job, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(JobKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	jobs := make([]*types.Job, 0, len(ids))
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var job types.Job
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &job) == nil {
			jobs = append(jobs, &job)
		}
	}
	return jobs, nil
}

func (s *RedisStore) hydrateSchedules(ctx context.Context, ids []string) ([]*types.ScheduleEntry, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(ScheduleKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	var scheds []*types.ScheduleEntry
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var sched types.ScheduleEntry
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &sched) == nil {
			scheds = append(scheds, &sched)
		}
	}
	return scheds, nil
}

func (s *RedisStore) hydrateRuns(ctx context.Context, ids []string) ([]*types.JobRun, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(RunKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	var runs []*types.JobRun
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var run types.JobRun
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &run) == nil {
			runs = append(runs, &run)
		}
	}
	return runs, nil
}

func (s *RedisStore) hydrateWorkflows(ctx context.Context, ids []string) ([]*types.WorkflowInstance, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(WorkflowKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	var wfs []*types.WorkflowInstance
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var wf types.WorkflowInstance
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &wf) == nil {
			wfs = append(wfs, &wf)
		}
	}
	return wfs, nil
}

func (s *RedisStore) hydrateBatches(ctx context.Context, ids []string) ([]*types.BatchInstance, error) {
	cmds := make(rueidis.Commands, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, s.rdb.B().Hget().Key(BatchKey(s.prefix, id)).Field("data").Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	var batches []*types.BatchInstance
	for _, r := range results {
		dataStr, err := r.ToString()
		if err != nil {
			continue
		}
		var batch types.BatchInstance
		if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &batch) == nil {
			batches = append(batches, &batch)
		}
	}
	return batches, nil
}

// ============================================================
// Internal helpers
// ============================================================

func (s *RedisStore) updateJobAssociationIndexes(ctx context.Context, job *types.Job) {
	score := float64(job.CreatedAt.UnixNano())
	if job.WorkflowID != nil && *job.WorkflowID != "" {
		s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByWorkflowKey(s.prefix, *job.WorkflowID)).ScoreMember().ScoreMember(score, job.ID).Build())
	}
	if job.BatchID != nil && *job.BatchID != "" {
		s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByBatchKey(s.prefix, *job.BatchID)).ScoreMember().ScoreMember(score, job.ID).Build())
	}
}

// addJobIndexes adds all secondary indexes for a newly created job.
func (s *RedisStore) addJobIndexes(ctx context.Context, job *types.Job) {
	score := float64(job.CreatedAt.UnixNano())
	s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByCreatedKey(s.prefix)).ScoreMember().ScoreMember(score, job.ID).Build())
	s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByStatusKey(s.prefix, string(job.Status))).ScoreMember().ScoreMember(score, job.ID).Build())
	s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByTaskTypeKey(s.prefix, string(job.TaskType))).ScoreMember().ScoreMember(score, job.ID).Build())
	for _, tag := range job.Tags {
		s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByTagKey(s.prefix, tag)).ScoreMember().ScoreMember(score, job.ID).Build())
	}
}

// updateJobIndexes diffs old and new job data and updates secondary indexes accordingly.
func (s *RedisStore) updateJobIndexes(ctx context.Context, job *types.Job, oldDataStr string) {
	score := float64(job.CreatedAt.UnixNano())

	// Always ensure by_created is current.
	s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByCreatedKey(s.prefix)).ScoreMember().ScoreMember(score, job.ID).Build())

	if oldDataStr == "" {
		// No previous data — add all indexes as new.
		s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByStatusKey(s.prefix, string(job.Status))).ScoreMember().ScoreMember(score, job.ID).Build())
		s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByTaskTypeKey(s.prefix, string(job.TaskType))).ScoreMember().ScoreMember(score, job.ID).Build())
		for _, tag := range job.Tags {
			s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByTagKey(s.prefix, tag)).ScoreMember().ScoreMember(score, job.ID).Build())
		}
		return
	}

	var old types.Job
	if sonic.ConfigFastest.Unmarshal([]byte(oldDataStr), &old) != nil {
		// Can't parse old data — just add new indexes.
		s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByStatusKey(s.prefix, string(job.Status))).ScoreMember().ScoreMember(score, job.ID).Build())
		s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByTaskTypeKey(s.prefix, string(job.TaskType))).ScoreMember().ScoreMember(score, job.ID).Build())
		for _, tag := range job.Tags {
			s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByTagKey(s.prefix, tag)).ScoreMember().ScoreMember(score, job.ID).Build())
		}
		return
	}

	// Status index diff.
	if old.Status != job.Status {
		s.rdb.Do(ctx, s.rdb.B().Zrem().Key(JobsByStatusKey(s.prefix, string(old.Status))).Member(job.ID).Build())
	}
	s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByStatusKey(s.prefix, string(job.Status))).ScoreMember().ScoreMember(score, job.ID).Build())

	// TaskType index diff.
	if old.TaskType != job.TaskType {
		s.rdb.Do(ctx, s.rdb.B().Zrem().Key(JobsByTaskTypeKey(s.prefix, string(old.TaskType))).Member(job.ID).Build())
	}
	s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByTaskTypeKey(s.prefix, string(job.TaskType))).ScoreMember().ScoreMember(score, job.ID).Build())

	// Tags index diff.
	oldTags := make(map[string]bool, len(old.Tags))
	for _, t := range old.Tags {
		oldTags[t] = true
	}
	newTags := make(map[string]bool, len(job.Tags))
	for _, t := range job.Tags {
		newTags[t] = true
	}
	for t := range oldTags {
		if !newTags[t] {
			s.rdb.Do(ctx, s.rdb.B().Zrem().Key(JobsByTagKey(s.prefix, t)).Member(job.ID).Build())
		}
	}
	for _, t := range job.Tags {
		s.rdb.Do(ctx, s.rdb.B().Zadd().Key(JobsByTagKey(s.prefix, t)).ScoreMember().ScoreMember(score, job.ID).Build())
	}

	// Clean old unique key if changed.
	if old.UniqueKey != nil && *old.UniqueKey != "" {
		if job.UniqueKey == nil || *job.UniqueKey != *old.UniqueKey {
			s.rdb.Do(ctx, s.rdb.B().Del().Key(UniqueKeyKey(s.prefix, *old.UniqueKey)).Build())
		}
	}
}

// removeJobIndexes removes all secondary indexes for a deleted job.
func (s *RedisStore) removeJobIndexes(ctx context.Context, jobID string, old *types.Job) {
	s.rdb.Do(ctx, s.rdb.B().Zrem().Key(JobsByCreatedKey(s.prefix)).Member(jobID).Build())
	s.rdb.Do(ctx, s.rdb.B().Zrem().Key(JobsByStatusKey(s.prefix, string(old.Status))).Member(jobID).Build())
	s.rdb.Do(ctx, s.rdb.B().Zrem().Key(JobsByTaskTypeKey(s.prefix, string(old.TaskType))).Member(jobID).Build())
	for _, tag := range old.Tags {
		s.rdb.Do(ctx, s.rdb.B().Zrem().Key(JobsByTagKey(s.prefix, tag)).Member(jobID).Build())
	}
	if old.UniqueKey != nil && *old.UniqueKey != "" {
		s.rdb.Do(ctx, s.rdb.B().Del().Key(UniqueKeyKey(s.prefix, *old.UniqueKey)).Build())
	}
	if old.WorkflowID != nil && *old.WorkflowID != "" {
		s.rdb.Do(ctx, s.rdb.B().Zrem().Key(JobsByWorkflowKey(s.prefix, *old.WorkflowID)).Member(jobID).Build())
	}
	if old.BatchID != nil && *old.BatchID != "" {
		s.rdb.Do(ctx, s.rdb.B().Zrem().Key(JobsByBatchKey(s.prefix, *old.BatchID)).Member(jobID).Build())
	}
}

// parseSaveJobResult splits the "old_data\nnew_version" response from single-key Lua scripts.
func parseSaveJobResult(result string) (string, uint64) {
	idx := strings.LastIndex(result, "\n")
	if idx < 0 {
		ver, _ := strconv.ParseUint(result, 10, 64)
		return "", ver
	}
	oldData := result[:idx]
	ver, _ := strconv.ParseUint(result[idx+1:], 10, 64)
	return oldData, ver
}

// hashFieldFromJSON reads a hash's "data" field, parses JSON, and extracts a named field value.
func hashFieldFromJSON(ctx context.Context, rdb rueidis.Client, key, field string) string {
	dataStr, err := rdb.Do(ctx, rdb.B().Hget().Key(key).Field("data").Build()).ToString()
	if err != nil {
		return ""
	}
	var m map[string]interface{}
	if sonic.ConfigFastest.Unmarshal([]byte(dataStr), &m) != nil {
		return ""
	}
	if v, ok := m[field]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// parseVersion extracts a uint64 version from a string, defaulting to 1.
func parseVersion(s string) uint64 {
	if v, err := strconv.ParseUint(s, 10, 64); err == nil {
		return v
	}
	return 1
}

// WorkMessageFromStreamValues constructs a WorkMessage from Redis stream field map.
func WorkMessageFromStreamValues(values map[string]string) types.WorkMessage {
	wm := types.WorkMessage{
		RunID:    values["run_id"],
		JobID:    values["job_id"],
		TaskType: types.TaskType(values["task_type"]),
	}

	// Handle payload carefully: empty string -> nil (not json.RawMessage("") which is invalid JSON).
	if p := values["payload"]; p != "" {
		wm.Payload = json.RawMessage(p)
	}

	if v := values["attempt"]; v != "" {
		n, _ := strconv.Atoi(v)
		wm.Attempt = n
	}
	if v := values["deadline"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v)
		wm.Deadline = t
	}
	if v := values["priority"]; v != "" {
		n, _ := strconv.Atoi(v)
		wm.Priority = types.Priority(n)
	}
	if v := values["dispatched_at"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v)
		wm.DispatchedAt = t
	}
	// Parse headers (JSON-encoded map).
	if h := values["headers"]; h != "" {
		var headers map[string]string
		if sonic.ConfigFastest.Unmarshal([]byte(h), &headers) == nil {
			wm.Headers = headers
		}
	}
	// Capture error field (present in DLQ entries).
	if e := values["error"]; e != "" {
		wm.Metadata = map[string]string{"error": e}
	}
	// Worker versioning.
	wm.Version = values["version"]
	wm.FiringID = values["firing_id"]
	// Concurrency keys (JSON-encoded slice).
	if ck := values["concurrency_keys"]; ck != "" {
		var keys []types.ConcurrencyKey
		if sonic.ConfigFastest.Unmarshal([]byte(ck), &keys) == nil {
			wm.ConcurrencyKeys = keys
		}
	}
	return wm
}

// ============================================================
// RedisJSON payload mirrors — best-effort writes for JSONPath search
// ============================================================

// mirrorJobPayload stores the job's Payload as a RedisJSON document for JSONPath queries.
// Uses a detached context so it never steals deadline budget from the caller.
func (s *RedisStore) mirrorJobPayload(_ context.Context, job *types.Job) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	payload := job.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	s.rdb.Do(bgCtx, s.rdb.B().JsonSet().Key(JobPayloadKey(s.prefix, job.ID)).Path("$").Value(string(payload)).Build())
}

// mirrorWorkflowPayloads stores all task payloads from a workflow definition as a JSON array.
// Uses a detached context so it never steals deadline budget from the caller.
func (s *RedisStore) mirrorWorkflowPayloads(_ context.Context, wf *types.WorkflowInstance) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	payloads := make([]json.RawMessage, 0, len(wf.Definition.Tasks))
	for _, t := range wf.Definition.Tasks {
		p := t.Payload
		if len(p) == 0 {
			p = json.RawMessage("{}")
		}
		payloads = append(payloads, p)
	}
	data, _ := sonic.ConfigFastest.Marshal(payloads)
	s.rdb.Do(bgCtx, s.rdb.B().JsonSet().Key(WorkflowPayloadKey(s.prefix, wf.ID)).Path("$").Value(string(data)).Build())
}

// mirrorBatchPayloads stores onetime + item payloads from a batch definition as a JSON document.
// Uses a detached context so it never steals deadline budget from the caller.
func (s *RedisStore) mirrorBatchPayloads(_ context.Context, batch *types.BatchInstance) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type batchPayloads struct {
		Onetime json.RawMessage   `json:"onetime"`
		Items   []json.RawMessage `json:"items"`
	}
	bp := batchPayloads{Onetime: batch.Definition.OnetimePayload}
	if len(bp.Onetime) == 0 {
		bp.Onetime = json.RawMessage("null")
	}
	for _, item := range batch.Definition.Items {
		p := item.Payload
		if len(p) == 0 {
			p = json.RawMessage("{}")
		}
		bp.Items = append(bp.Items, p)
	}
	data, _ := sonic.ConfigFastest.Marshal(bp)
	s.rdb.Do(bgCtx, s.rdb.B().JsonSet().Key(BatchPayloadKey(s.prefix, batch.ID)).Path("$").Value(string(data)).Build())
}

// ============================================================
// JSONPath search — scan entities and match payload fields via RedisJSON
// ============================================================

const jsonPathSearchChunkSize = 100

// FindJobByPayloadPath scans jobs and returns the first whose Payload matches
// the given JSONPath + value. Uses RedisJSON's JSON.GET for native path extraction.
// If taskType is non-empty, only jobs of that type are scanned (via the by_task_type index).
func (s *RedisStore) FindJobByPayloadPath(ctx context.Context, jsonPath string, value any, taskType string) (*types.Job, uint64, error) {
	fullPath := "$." + jsonPath
	valueStr := fmt.Sprintf("%v", value)

	// Choose scan index: narrow by task type if provided, otherwise scan all.
	indexKey := JobsByCreatedKey(s.prefix)
	if taskType != "" {
		indexKey = JobsByTaskTypeKey(s.prefix, taskType)
	}

	total, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(indexKey).Build()).AsInt64()
	if err != nil {
		return nil, 0, fmt.Errorf("count jobs: %w", err)
	}

	for offset := int64(0); offset < total; offset += jsonPathSearchChunkSize {
		end := offset + jsonPathSearchChunkSize - 1
		ids, err := s.rdb.Do(ctx, s.rdb.B().Zrevrange().Key(indexKey).Start(offset).Stop(end).Build()).AsStrSlice()
		if err != nil || len(ids) == 0 {
			break
		}

		// Pipeline JSON.GET for each key.
		cmds := make(rueidis.Commands, 0, len(ids))
		for _, id := range ids {
			cmds = append(cmds, s.rdb.B().JsonGet().Key(JobPayloadKey(s.prefix, id)).Path(fullPath).Build())
		}
		results := s.rdb.DoMulti(ctx, cmds...)

		for i, r := range results {
			if matchJSONGetResult(r, valueStr) {
				return s.GetJob(ctx, ids[i])
			}
		}
	}
	return nil, 0, types.ErrJobNotFound
}

// FindWorkflowByTaskPayloadPath scans all workflows and returns the first where any task's
// Payload matches the given JSONPath + value.
func (s *RedisStore) FindWorkflowByTaskPayloadPath(ctx context.Context, jsonPath string, value any) (*types.WorkflowInstance, uint64, error) {
	// JSON array stored as [task0_payload, task1_payload, ...].
	// Query all task payloads: $[*].{path}
	fullPath := "$[*]." + jsonPath
	valueStr := fmt.Sprintf("%v", value)

	total, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(WorkflowsByCreatedKey(s.prefix)).Build()).AsInt64()
	if err != nil {
		return nil, 0, fmt.Errorf("count workflows: %w", err)
	}

	for offset := int64(0); offset < total; offset += jsonPathSearchChunkSize {
		end := offset + jsonPathSearchChunkSize - 1
		ids, err := s.rdb.Do(ctx, s.rdb.B().Zrevrange().Key(WorkflowsByCreatedKey(s.prefix)).Start(offset).Stop(end).Build()).AsStrSlice()
		if err != nil || len(ids) == 0 {
			break
		}

		cmds := make(rueidis.Commands, 0, len(ids))
		for _, id := range ids {
			cmds = append(cmds, s.rdb.B().JsonGet().Key(WorkflowPayloadKey(s.prefix, id)).Path(fullPath).Build())
		}
		results := s.rdb.DoMulti(ctx, cmds...)

		for i, r := range results {
			if matchJSONGetResult(r, valueStr) {
				return s.GetWorkflow(ctx, ids[i])
			}
		}
	}
	return nil, 0, types.ErrWorkflowNotFound
}

// FindBatchByPayloadPath scans all batches and returns the first where onetime or any item
// Payload matches the given JSONPath + value.
func (s *RedisStore) FindBatchByPayloadPath(ctx context.Context, jsonPath string, value any) (*types.BatchInstance, uint64, error) {
	valueStr := fmt.Sprintf("%v", value)

	total, err := s.rdb.Do(ctx, s.rdb.B().Zcard().Key(BatchesByCreatedKey(s.prefix)).Build()).AsInt64()
	if err != nil {
		return nil, 0, fmt.Errorf("count batches: %w", err)
	}

	for offset := int64(0); offset < total; offset += jsonPathSearchChunkSize {
		end := offset + jsonPathSearchChunkSize - 1
		ids, err := s.rdb.Do(ctx, s.rdb.B().Zrevrange().Key(BatchesByCreatedKey(s.prefix)).Start(offset).Stop(end).Build()).AsStrSlice()
		if err != nil || len(ids) == 0 {
			break
		}

		// Check onetime payload: $.onetime.{path} and items: $.items[*].{path}
		cmds := make(rueidis.Commands, 0, len(ids)*2)
		for _, id := range ids {
			key := BatchPayloadKey(s.prefix, id)
			cmds = append(cmds, s.rdb.B().JsonGet().Key(key).Path("$.onetime."+jsonPath).Build())
			cmds = append(cmds, s.rdb.B().JsonGet().Key(key).Path("$.items[*]."+jsonPath).Build())
		}
		results := s.rdb.DoMulti(ctx, cmds...)

		for i := range ids {
			onetimeResult := results[i*2]
			itemsResult := results[i*2+1]
			if matchJSONGetResult(onetimeResult, valueStr) || matchJSONGetResult(itemsResult, valueStr) {
				return s.GetBatch(ctx, ids[i])
			}
		}
	}
	return nil, 0, types.ErrBatchNotFound
}

// matchJSONGetResult checks if a JSON.GET result array contains the expected value.
// JSON.GET returns a JSON array like ["value"] or [42] for scalar matches.
func matchJSONGetResult(r rueidis.RedisResult, expected string) bool {
	raw, err := r.ToString()
	if err != nil || raw == "" || raw == "[]" {
		return false
	}

	// Parse the JSON array of results.
	var results []any
	if sonic.ConfigFastest.Unmarshal([]byte(raw), &results) != nil {
		return false
	}
	for _, v := range results {
		if fmt.Sprintf("%v", v) == expected {
			return true
		}
	}
	return false
}

// --- Workflow Signal operations ---

// SendSignal appends a signal to a workflow's signal stream.
// If the signal has a DedupeKey, it prevents duplicate signals using SET NX.
func (s *RedisStore) SendSignal(ctx context.Context, signal *types.WorkflowSignal) error {
	// Dedup check: if DedupeKey is set, use SET NX to prevent duplicates.
	// TTL = SignalDedupTTL (default 30d). Must exceed max workflow lifetime.
	if signal.DedupeKey != "" {
		dedupKey := SignalDedupKey(s.prefix, signal.WorkflowID, signal.DedupeKey)
		err := s.rdb.Do(ctx, s.rdb.B().Set().Key(dedupKey).Value("1").Nx().Ex(s.cfg.SignalDedupTTL).Build()).Error()
		if err != nil {
			if rueidis.IsRedisNil(err) {
				return types.ErrSignalDuplicate
			}
			return fmt.Errorf("signal dedup check: %w", err)
		}
	}

	key := WorkflowSignalStreamKey(s.prefix, signal.WorkflowID)
	data, err := sonic.ConfigFastest.Marshal(signal)
	if err != nil {
		return fmt.Errorf("marshal signal: %w", err)
	}
	cmd := s.rdb.B().Xadd().Key(key).Id("*").FieldValue().FieldValue("data", string(data)).Build()
	return s.rdb.Do(ctx, cmd).Error()
}

// ReadSignals reads all pending signals for a workflow without deleting them.
// Each returned signal has its StreamID populated so the caller can pass
// processed signals to AckSignals after handling. This prevents signal loss
// if the caller crashes between read and processing.
func (s *RedisStore) ReadSignals(ctx context.Context, workflowID string) ([]types.WorkflowSignal, error) {
	key := WorkflowSignalStreamKey(s.prefix, workflowID)
	cmd := s.rdb.B().Xrange().Key(key).Start("-").End("+").Build()
	entries, err := s.rdb.Do(ctx, cmd).AsXRange()
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}

	signals := make([]types.WorkflowSignal, 0, len(entries))
	for _, entry := range entries {
		data, ok := entry.FieldValues["data"]
		if !ok {
			continue
		}
		var sig types.WorkflowSignal
		if err := sonic.ConfigFastest.Unmarshal([]byte(data), &sig); err != nil {
			continue
		}
		sig.StreamID = entry.ID
		signals = append(signals, sig)
	}

	return signals, nil
}

// AckSignals deletes processed signal entries from the workflow signal stream.
// Call this after successfully handling signals returned by ReadSignals.
func (s *RedisStore) AckSignals(ctx context.Context, workflowID string, streamIDs []string) error {
	if len(streamIDs) == 0 {
		return nil
	}
	key := WorkflowSignalStreamKey(s.prefix, workflowID)
	return s.rdb.Do(ctx, s.rdb.B().Xdel().Key(key).Id(streamIDs...).Build()).Error()
}

// ConsumeSignals reads and acknowledges all pending signals for a workflow.
// Deprecated: Use ReadSignals + AckSignals for crash-safe signal processing.
// This method is retained for backward compatibility but is lossy — if the
// caller crashes after this call returns, the signals are permanently lost.
func (s *RedisStore) ConsumeSignals(ctx context.Context, workflowID string) ([]types.WorkflowSignal, error) {
	s.logger.Warn().Msg("ConsumeSignals is deprecated: use ReadSignals+AckSignals for crash-safe signal processing")
	signals, err := s.ReadSignals(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(signals))
	for _, sig := range signals {
		if sig.StreamID != "" {
			ids = append(ids, sig.StreamID)
		}
	}
	if len(ids) > 0 {
		s.rdb.Do(ctx, s.rdb.B().Xdel().Key(WorkflowSignalStreamKey(s.prefix, workflowID)).Id(ids...).Build())
	}
	return signals, nil
}

// SignalStats holds signal stream statistics for a workflow.
type SignalStats struct {
	PendingCount     int64 `json:"pending_count"`
	OldestUnackedMs  int64 `json:"oldest_unacked_age_ms"`
}

// GetSignalStats returns the number of pending signals and the age of the oldest
// unacked signal for a workflow. Uses XLEN + XRANGE COUNT 1.
func (s *RedisStore) GetSignalStats(ctx context.Context, workflowID string) (*SignalStats, error) {
	key := WorkflowSignalStreamKey(s.prefix, workflowID)

	// Pipeline: XLEN + XRANGE - + COUNT 1
	xlenCmd := s.rdb.B().Xlen().Key(key).Build()
	xrangeCmd := s.rdb.B().Xrange().Key(key).Start("-").End("+").Count(1).Build()

	results := s.rdb.DoMulti(ctx, xlenCmd, xrangeCmd)

	count, err := results[0].AsInt64()
	if err != nil {
		return &SignalStats{}, nil // stream doesn't exist
	}

	stats := &SignalStats{PendingCount: count}
	entries, err := results[1].AsXRange()
	if err == nil && len(entries) > 0 {
		// Parse Redis stream ID: "{milliseconds}-{seq}"
		id := entries[0].ID
		var tsMs int64
		for i := 0; i < len(id); i++ {
			if id[i] == '-' {
				break
			}
			tsMs = tsMs*10 + int64(id[i]-'0')
		}
		stats.OldestUnackedMs = time.Now().UnixMilli() - tsMs
		if stats.OldestUnackedMs < 0 {
			stats.OldestUnackedMs = 0
		}
	}
	return stats, nil
}

// DeleteSignalStream removes the signal stream for a workflow (cleanup).
func (s *RedisStore) DeleteSignalStream(ctx context.Context, workflowID string) {
	key := WorkflowSignalStreamKey(s.prefix, workflowID)
	s.rdb.Do(ctx, s.rdb.B().Del().Key(key).Build())
}

// --- Job Audit Trail ---

const auditStreamMaxLen = 1000

// appendJobAudit records a state transition in the per-job audit stream.
func (s *RedisStore) appendJobAudit(ctx context.Context, jobID string, status types.JobStatus, lastError *string) {
	key := JobAuditKey(s.prefix, jobID)
	errVal := ""
	if lastError != nil {
		errVal = *lastError
	}
	cmd := s.rdb.B().Xadd().Key(key).Maxlen().Almost().Threshold(strconv.FormatInt(auditStreamMaxLen, 10)).Id("*").
		FieldValue().FieldValue("status", string(status)).FieldValue("error", errVal).FieldValue("ts", time.Now().Format(time.RFC3339Nano)).Build()
	s.rdb.Do(ctx, cmd)
}

// AuditEntry represents a single state transition in a job's audit trail.
type AuditEntry struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	Error     string    `json:"error,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// GetJobAuditTrail returns the state transition history for a job.
func (s *RedisStore) GetJobAuditTrail(ctx context.Context, jobID string) ([]AuditEntry, error) {
	key := JobAuditKey(s.prefix, jobID)
	entries, err := s.rdb.Do(ctx, s.rdb.B().Xrange().Key(key).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		return nil, err
	}

	result := make([]AuditEntry, 0, len(entries))
	for _, entry := range entries {
		ae := AuditEntry{
			ID:     entry.ID,
			Status: entry.FieldValues["status"],
			Error:  entry.FieldValues["error"],
		}
		if ts, err := time.Parse(time.RFC3339Nano, entry.FieldValues["ts"]); err == nil {
			ae.Timestamp = ts
		}
		result = append(result, ae)
	}
	return result, nil
}

// GetJobAuditCount returns the number of audit entries for a job (XLEN).
func (s *RedisStore) GetJobAuditCount(ctx context.Context, jobID string) (int64, error) {
	key := JobAuditKey(s.prefix, jobID)
	return s.rdb.Do(ctx, s.rdb.B().Xlen().Key(key).Build()).AsInt64()
}

// GetJobAuditCounts returns audit entry counts for multiple jobs via pipelined XLEN.
func (s *RedisStore) GetJobAuditCounts(ctx context.Context, jobIDs []string) (map[string]int64, error) {
	result := make(map[string]int64, len(jobIDs))
	if len(jobIDs) == 0 {
		return result, nil
	}
	cmds := make([]rueidis.Completed, len(jobIDs))
	for i, id := range jobIDs {
		cmds[i] = s.rdb.B().Xlen().Key(JobAuditKey(s.prefix, id)).Build()
	}
	resps := s.rdb.DoMulti(ctx, cmds...)
	for i, resp := range resps {
		n, err := resp.AsInt64()
		if err != nil {
			n = 0
		}
		result[jobIDs[i]] = n
	}
	return result, nil
}

// DeleteJobAuditTrail removes the audit stream for a job (cleanup).
func (s *RedisStore) DeleteJobAuditTrail(ctx context.Context, jobID string) {
	key := JobAuditKey(s.prefix, jobID)
	s.rdb.Do(ctx, s.rdb.B().Del().Key(key).Build())
}

// --- Per-Workflow Event Index ---

// GetWorkflowEvents returns all indexed events for a workflow, ordered by timestamp.
func (s *RedisStore) GetWorkflowEvents(ctx context.Context, workflowID string) ([]types.JobEvent, error) {
	key := WorkflowEventsKey(s.prefix, workflowID)
	members, err := s.rdb.Do(ctx, s.rdb.B().Zrangebyscore().Key(key).Min("-inf").Max("+inf").Build()).AsStrSlice()
	if err != nil {
		return nil, err
	}

	events := make([]types.JobEvent, 0, len(members))
	for _, m := range members {
		var ev types.JobEvent
		if err := sonic.ConfigFastest.Unmarshal([]byte(m), &ev); err != nil {
			continue
		}
		events = append(events, ev)
	}
	return events, nil
}

// DeleteWorkflowEvents removes the per-workflow event index (cleanup).
func (s *RedisStore) DeleteWorkflowEvents(ctx context.Context, workflowID string) {
	key := WorkflowEventsKey(s.prefix, workflowID)
	s.rdb.Do(ctx, s.rdb.B().Del().Key(key).Build())
}

// --- Node Drain ---

// SetNodeDrain sets the drain flag for a node. While draining, the worker
// stops fetching new messages but continues processing in-flight tasks.
func (s *RedisStore) SetNodeDrain(ctx context.Context, nodeID string, drain bool) error {
	key := NodeDrainKey(s.prefix, nodeID)
	if drain {
		return s.rdb.Do(ctx, s.rdb.B().Set().Key(key).Value("1").Build()).Error()
	}
	return s.rdb.Do(ctx, s.rdb.B().Del().Key(key).Build()).Error()
}

// IsNodeDraining returns true if the node is in drain mode.
func (s *RedisStore) IsNodeDraining(ctx context.Context, nodeID string) (bool, error) {
	key := NodeDrainKey(s.prefix, nodeID)
	val, err := s.rdb.Do(ctx, s.rdb.B().Get().Key(key).Build()).ToString()
	if err != nil {
		return false, nil // key doesn't exist = not draining
	}
	return val == "1", nil
}

// ListPausedJobsDueForResume returns paused jobs whose ResumeAt time has passed.
func (s *RedisStore) ListPausedJobsDueForResume(ctx context.Context, now time.Time) ([]*types.Job, error) {
	jobs, err := s.ListJobsByStatus(ctx, types.JobStatusPaused)
	if err != nil {
		return nil, err
	}
	var due []*types.Job
	for _, job := range jobs {
		if job.ResumeAt != nil && !job.ResumeAt.After(now) {
			due = append(due, job)
		}
	}
	return due, nil
}

// GetJobRevision returns only the CAS revision for a job (without full deserialization).
func (s *RedisStore) GetJobRevision(ctx context.Context, jobID string) (uint64, error) {
	result, err := s.rdb.Do(ctx, s.rdb.B().Hget().Key(JobKey(s.prefix, jobID)).Field("_version").Build()).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return 0, types.ErrJobNotFound
		}
		return 0, fmt.Errorf("get job revision: %w", err)
	}
	return parseVersion(result), nil
}

// PurgeJobData removes the payload and result data from a job while preserving
// metadata (status, timestamps, task type, etc.). This supports GDPR
// "right to erasure" by deleting personal data while maintaining audit trails.
func (s *RedisStore) PurgeJobData(ctx context.Context, jobID string) error {
	job, rev, err := s.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	if !job.Status.IsTerminal() {
		return fmt.Errorf("dureq: cannot purge data for non-terminal job %s (status: %s)", jobID, job.Status)
	}

	// Clear payload and personal data fields.
	job.Payload = nil
	job.Headers = nil
	job.UpdatedAt = time.Now()

	if _, err := s.UpdateJob(ctx, job, rev); err != nil {
		return fmt.Errorf("purge job data: %w", err)
	}

	// Also clear the mirrored JSON payload used for JSONPath search.
	s.rdb.Do(ctx, s.rdb.B().Del().Key(JobPayloadKey(s.prefix, jobID)).Build())

	// Clear result data.
	s.rdb.Do(ctx, s.rdb.B().Del().Key(ResultKey(s.prefix, jobID)).Build())

	return nil
}

// extractJSONStatus extracts the "status" field value from a JSON string
// without full unmarshal. Returns empty string if not found.
func extractJSONStatus(data string) string {
	const key = `"status":"`
	idx := strings.Index(data, key)
	if idx < 0 {
		return ""
	}
	start := idx + len(key)
	end := strings.IndexByte(data[start:], '"')
	if end < 0 {
		return ""
	}
	return data[start : start+end]
}

// --- Side-effect steps ---

// ClaimSideEffect atomically claims a side-effect step for the given run.
// Returns (result, true) if the step was already completed (cached result),
// or ("", false) if the step was newly claimed and the caller should execute.
func (s *RedisStore) ClaimSideEffect(ctx context.Context, runID, stepKey string, ttlSeconds int) (string, bool, error) {
	key := SideEffectKey(s.prefix, runID, stepKey)
	result, err := s.scriptClaimSideEffect.Exec(ctx, s.rdb,
		[]string{key},
		[]string{strconv.Itoa(ttlSeconds)},
	).ToString()
	if err != nil {
		return "", false, fmt.Errorf("claim side effect: %w", err)
	}

	switch {
	case result == "claimed":
		return "", false, nil
	case result == "pending":
		// Another caller has claimed but not yet completed. Treat as "already done"
		// to avoid duplicate execution — caller should wait or retry.
		return "", false, fmt.Errorf("side effect step %q is already being executed", stepKey)
	case strings.HasPrefix(result, "done:"):
		return result[5:], true, nil
	default:
		return "", false, fmt.Errorf("unexpected claim result: %s", result)
	}
}

// CompleteSideEffect marks a side-effect step as done and stores the result.
func (s *RedisStore) CompleteSideEffect(ctx context.Context, runID, stepKey string, result string) error {
	key := SideEffectKey(s.prefix, runID, stepKey)
	cmd := s.rdb.B().Hset().Key(key).FieldValue().FieldValue("status", "done").FieldValue("result", result).Build()
	return s.rdb.Do(ctx, cmd).Error()
}

// ============================================================
// Concurrency slot operations
// ============================================================

// AcquireConcurrencySlot tries to acquire a concurrency slot for the given run.
// Returns true if the slot was acquired, false if at capacity.
func (s *RedisStore) AcquireConcurrencySlot(ctx context.Context, concKey, runID string, maxConcurrency int) (bool, error) {
	key := ConcurrencySlotKey(s.prefix, concKey)
	now := strconv.FormatFloat(float64(time.Now().UnixMilli())/1000.0, 'f', 3, 64)
	result, err := s.scriptAcquireConcSlot.Exec(ctx, s.rdb,
		[]string{key},
		[]string{runID, strconv.Itoa(maxConcurrency), now},
	).ToInt64()
	if err != nil {
		return false, fmt.Errorf("acquire concurrency slot: %w", err)
	}
	return result == 1, nil
}

// ReleaseConcurrencySlot releases a concurrency slot held by the given run.
func (s *RedisStore) ReleaseConcurrencySlot(ctx context.Context, concKey, runID string) error {
	key := ConcurrencySlotKey(s.prefix, concKey)
	_, err := s.scriptReleaseConcSlot.Exec(ctx, s.rdb,
		[]string{key},
		[]string{runID},
	).ToInt64()
	if err != nil {
		return fmt.Errorf("release concurrency slot: %w", err)
	}
	return nil
}

// ConcurrencySlotCount returns the current number of active slots for a concurrency key.
func (s *RedisStore) ConcurrencySlotCount(ctx context.Context, concKey string) (int64, error) {
	key := ConcurrencySlotKey(s.prefix, concKey)
	return s.rdb.Do(ctx, s.rdb.B().Zcard().Key(key).Build()).ToInt64()
}
