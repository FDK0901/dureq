package scheduler

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/FDK0901/dureq/internal/cache"
	"github.com/FDK0901/dureq/internal/dynconfig"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
)

// Dispatcher is the interface the scheduler uses to dispatch work.
type Dispatcher interface {
	Dispatch(ctx context.Context, job *types.Job, attempt int) error
	// DispatchFiring reports false, without error, if the firing was already dispatched.
	DispatchFiring(ctx context.Context, job *types.Job, attempt int) (bool, error)
	DispatchToDLQ(ctx context.Context, job *types.Job, lastErr string) error
	PublishEvent(event types.JobEvent)
}

// Headers the scheduler adds to the work message of a schedule firing.
const (
	headerFiringID    = "x-dureq-firing-id"
	headerBackfill    = "x-dureq-backfill"
	headerScheduledAt = "x-dureq-scheduled-at"
)

// headerOperationKey sets the operation key of a work message.
const headerOperationKey = "x-dureq-operation-key"

// Config holds scheduler configuration.
type Config struct {
	Store        *store.RedisStore
	Dispatcher   Dispatcher
	TickInterval time.Duration
	DynConfig    *dynconfig.Manager // Optional: runtime config overrides (tick interval).
	Logger       chainedlog.Logger

	// OrphanCheckInterval controls how often orphan run detection runs.
	// Defaults to 30s. Set to 0 to disable.
	OrphanCheckInterval time.Duration
}

// Scheduler runs on the leader node and periodically checks for due jobs.
type Scheduler struct {
	store      *store.RedisStore
	cache      *cache.ScheduleCache
	dispatcher Dispatcher
	interval   time.Duration
	dynCfg     *dynconfig.Manager
	logger     chainedlog.Logger

	orphanInterval  time.Duration
	lastOrphanCheck time.Time

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	active bool
}

// New creates a new scheduler. Call Start() to begin the tick loop.
func New(cfg Config) *Scheduler {
	if cfg.TickInterval == 0 {
		cfg.TickInterval = 1 * time.Second
	}
	if cfg.OrphanCheckInterval == 0 {
		cfg.OrphanCheckInterval = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return &Scheduler{
		store:          cfg.Store,
		cache:          cache.NewScheduleCache(cfg.Store, cache.DefaultCacheConfig()),
		dispatcher:     cfg.Dispatcher,
		interval:       cfg.TickInterval,
		dynCfg:         cfg.DynConfig,
		orphanInterval: cfg.OrphanCheckInterval,
		logger:         cfg.Logger,
	}
}

// Start begins the scheduler tick loop. Safe to call multiple times (idempotent).
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active {
		return
	}

	tickCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.active = true

	go s.tickLoop(tickCtx)
	s.logger.Info().Duration("interval", s.interval).Msg("scheduler started")
}

// Stop halts the scheduler tick loop. Safe to call multiple times.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.active {
		return
	}

	s.cancel()
	<-s.done
	s.active = false
	s.logger.Info().Msg("scheduler stopped")
}

func (s *Scheduler) tickLoop(ctx context.Context) {
	defer close(s.done)

	currentInterval := s.interval
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	// Run one tick immediately on start.
	s.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
			// Check if dynamic config changed the tick interval.
			if s.dynCfg != nil {
				if snap := s.dynCfg.Get(); snap.SchedulerTickInterval > 0 && snap.SchedulerTickInterval != currentInterval {
					currentInterval = snap.SchedulerTickInterval
					ticker.Reset(currentInterval)
					s.logger.Info().String("interval", currentInterval.String()).Msg("scheduler: tick interval updated from dynamic config")
				}
			}
		}
	}
}

// tick scans for due schedules, dispatches them, and periodically checks for orphaned runs and paused jobs
// and trims the work streams.
func (s *Scheduler) tick(ctx context.Context) {
	now := time.Now()

	dueSchedules, err := s.cache.ListDueSchedules(ctx, now)
	if err != nil {
		s.logger.Error().Err(err).Msg("scheduler: failed to list due schedules")
		return
	}

	s.processDue(ctx, dueSchedules, now)

	// Orphan detection runs less frequently than the main tick.
	if s.orphanInterval > 0 && now.Sub(s.lastOrphanCheck) >= s.orphanInterval {
		s.lastOrphanCheck = now
		s.checkOrphanedRuns(ctx)
		s.resumePausedJobs(ctx, now)
		s.trimWorkStreams(ctx)
	}
}

// dueConcurrency is how many schedules that are due are processed at once.
const dueConcurrency = 32

// processDue processes schedules that are due, each of which belongs to a job of its own.
func (s *Scheduler) processDue(ctx context.Context, entries []*types.ScheduleEntry, now time.Time) {
	due := make(chan *types.ScheduleEntry)
	var wg sync.WaitGroup
	for range min(dueConcurrency, len(entries)) {
		wg.Go(func() {
			for entry := range due {
				s.processScheduleEntry(ctx, entry, now)
			}
		})
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			break // another leader goes on
		}
		due <- entry
	}
	close(due)
	wg.Wait()
}

// trimWorkStreams removes what has been processed from the work streams.
func (s *Scheduler) trimWorkStreams(ctx context.Context) {
	for _, tier := range s.store.Config().Tiers {
		n, err := s.store.TrimWorkStream(ctx, tier.Name)
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warn().String("tier", tier.Name).Err(err).Msg("scheduler: failed to trim work stream")
			}
			continue
		}
		if n > 0 {
			s.logger.Debug().String("tier", tier.Name).Int64("messages", n).Msg("scheduler: trimmed work stream")
		}
	}
}

// resumePausedJobs scans for paused jobs whose ResumeAt has passed and transitions them back to retrying.
func (s *Scheduler) resumePausedJobs(ctx context.Context, now time.Time) {
	jobs, err := s.store.ListPausedJobsDueForResume(ctx, now)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Error().Err(err).Msg("scheduler: failed to list paused jobs for auto-resume")
		}
		return
	}

	for _, job := range jobs {
		rev, err := s.store.GetJobRevision(ctx, job.ID)
		if err != nil {
			continue
		}

		job.Resume(now)

		if _, err := s.store.UpdateJob(ctx, job, rev); err != nil {
			s.logger.Warn().String("job_id", job.ID).Err(err).Msg("scheduler: failed to resume paused job")
			continue
		}

		if err := s.dispatcher.Dispatch(ctx, job, job.Attempt); err != nil {
			s.logger.Error().String("job_id", job.ID).Err(err).Msg("scheduler: failed to dispatch resumed job")
			continue
		}

		s.dispatcher.PublishEvent(types.JobEvent{
			Type:      types.EventJobResumed,
			JobID:     job.ID,
			TaskType:  job.TaskType,
			Timestamp: now,
		})

		s.logger.Info().String("job_id", job.ID).Msg("scheduler: auto-resumed paused job")
	}
}

func (s *Scheduler) processScheduleEntry(ctx context.Context, entry *types.ScheduleEntry, now time.Time) {
	// Check if the schedule has expired.
	if IsExpired(entry.Schedule, now) {
		s.expireSchedule(ctx, entry)
		return
	}

	// Load the job (via cache).
	job, rev, err := s.cache.GetJob(ctx, entry.JobID)
	if err != nil {
		s.logger.Error().String("job_id", entry.JobID).Err(err).Msg("scheduler: failed to get job")
		if err == types.ErrJobNotFound {
			s.store.DeleteSchedule(ctx, entry.JobID)
			s.cache.InvalidateSchedules()
		}
		return
	}

	// Skip if job is in a terminal state.
	if job.Status.IsTerminal() {
		s.store.DeleteSchedule(ctx, entry.JobID)
		s.cache.InvalidateSchedules()
		// Normally done when the job ended, unless that process stopped in between.
		s.releaseEndedJobUniqueKey(ctx, entry.JobID)
		return
	}

	// A paused job does not fire. What falls due meanwhile is skipped rather than
	// caught up on: resuming the job runs what it was paused on and then goes on
	// with the schedule. The cached job may be older than the pause (or the resume),
	// so it is read anew.
	if job.Status.IsPaused() {
		s.cache.InvalidateJob(entry.JobID)
		if job, rev, err = s.cache.GetJob(ctx, entry.JobID); err != nil {
			s.logger.Error().String("job_id", entry.JobID).Err(err).Msg("scheduler: failed to get paused job")
			return
		}
		if job.Status.IsPaused() {
			s.logger.Debug().String("job_id", entry.JobID).Msg("scheduler: job is paused, skipping firing")
			entry.LastProcessedAt = &now
			s.advanceSchedule(ctx, entry, now)
			return
		}
	}

	// --- Overlap policy check ---
	policy := entry.Schedule.OverlapPolicy
	if policy == "" {
		policy = types.OverlapAllowAll
	}
	if policy != types.OverlapAllowAll {
		hasActive, err := s.store.HasActiveRunForJob(ctx, entry.JobID)
		if err != nil {
			s.logger.Warn().String("job_id", entry.JobID).Err(err).Msg("scheduler: overlap check failed")
		} else if hasActive {
			switch policy {
			case types.OverlapSkip:
				s.logger.Debug().String("job_id", entry.JobID).Msg("scheduler: skipping overlapping dispatch")
				s.advanceSchedule(ctx, entry, now)
				return
			case types.OverlapBufferOne:
				s.logger.Debug().String("job_id", entry.JobID).Msg("scheduler: buffering overlapping dispatch (one)")
				s.store.SaveOverlapBuffer(ctx, entry.JobID, now)
				s.advanceSchedule(ctx, entry, now)
				return
			case types.OverlapBufferAll:
				s.logger.Debug().String("job_id", entry.JobID).Msg("scheduler: buffering overlapping dispatch (all)")
				s.store.PushOverlapBufferAll(ctx, entry.JobID, now)
				s.advanceSchedule(ctx, entry, now)
				return
			case types.OverlapReplace:
				s.logger.Info().String("job_id", entry.JobID).Msg("scheduler: cancelling active runs for REPLACE overlap")
				s.cancelActiveRunsForJob(ctx, entry.JobID)
				// NOTE: cancel is async (pub/sub signal). A short overlap window is
				// possible until the worker processes the cancellation. This is
				// best-effort by design — a synchronous wait would block the scheduler tick.
			}
		}
	}

	// --- Backfill missed firings ---
	maxBackfill := 10
	if entry.Schedule.MaxBackfillPerTick != nil {
		maxBackfill = *entry.Schedule.MaxBackfillPerTick
	}

	refTime := entry.NextRunAt
	if entry.LastProcessedAt != nil {
		refTime = *entry.LastProcessedAt
	}

	missedFirings, _ := MissedFirings(entry.Schedule, refTime, now, maxBackfill)

	dispatched := false
	if len(missedFirings) > 1 {
		s.logger.Info().String("job_id", entry.JobID).Int("missed_count", len(missedFirings)).Msg("scheduler: backfilling missed firings")
		allOK := true
		for _, firingTime := range missedFirings {
			newRev, ok := s.dispatchFiring(ctx, job, rev, entry, firingTime, firingTime, true)
			if !ok {
				allOK = false
				break
			}
			rev = newRev
			dispatched = true
		}
		if allOK {
			lastFiring := missedFirings[len(missedFirings)-1]
			entry.LastProcessedAt = &lastFiring
		}
	} else {
		// Normal single dispatch. The firing is identified by its scheduled time,
		// not the tick time, so a leader that takes over before the schedule was
		// advanced derives the same FiringID and the firing isn't dispatched twice.
		scheduledAt := entry.NextRunAt
		if scheduledAt.IsZero() {
			scheduledAt = now
		}
		_, ok := s.dispatchFiring(ctx, job, rev, entry, now, scheduledAt, false)
		if ok {
			entry.LastProcessedAt = &now
			dispatched = true
		}
	}

	s.cache.InvalidateJob(job.ID)
	if dispatched {
		s.advanceSchedule(ctx, entry, now)
	}
}

// dispatchFiring dispatches a single firing of a scheduled job. scheduledAt
// identifies the firing (FiringID); firingTime is recorded as the job's LastRunAt.
// Returns the new revision and true on success, including when the firing had
// already been dispatched, or (0, false) on failure.
func (s *Scheduler) dispatchFiring(ctx context.Context, job *types.Job, rev uint64, entry *types.ScheduleEntry, firingTime, scheduledAt time.Time, isBackfill bool) (uint64, bool) {
	prevStatus := job.Status
	prevLastRunAt := job.LastRunAt
	prevAttempt := job.Attempt

	// Firing headers belong to the work message only. Older versions stored them
	// on the job, so later non-firing dispatches (retry, resume, orphan redispatch,
	// overlap drain) carried a stale FiringID and were deduplicated away. Strip
	// them so the update below also cleans up previously stored copies.
	delete(job.Headers, headerFiringID)
	delete(job.Headers, headerBackfill)
	delete(job.Headers, headerScheduledAt)

	job.Status = types.JobStatusPending
	job.LastRunAt = &firingTime
	job.UpdatedAt = time.Now()
	if job.Schedule.Type.IsRecurring() {
		// Each firing has a retry budget of its own.
		job.Attempt = 0
	}

	newRev, err := s.store.UpdateJob(ctx, job, rev)
	if err != nil {
		s.logger.Warn().String("job_id", job.ID).Err(err).Msg("scheduler: CAS update failed")
		return 0, false
	}

	// Deterministic FiringID for schedule firing dedup: every leader that
	// processes the same firing derives the same ID.
	headers := make(map[string]string, len(job.Headers)+3)
	maps.Copy(headers, job.Headers)
	headers[headerFiringID] = fmt.Sprintf("%s:%d", job.ID, scheduledAt.UnixMilli())
	// Add backfill headers for missed firings being caught up.
	if isBackfill {
		headers[headerBackfill] = "true"
		headers[headerScheduledAt] = scheduledAt.Format(time.RFC3339Nano)
	}
	firingJob := *job
	firingJob.Headers = headers

	dispatched, err := s.dispatcher.DispatchFiring(ctx, &firingJob, job.Attempt)
	if err != nil || !dispatched {
		// Rollback: revert job status to prevent state inconsistency. If the firing
		// was already dispatched, the status must reflect that earlier run.
		job.Status = prevStatus
		job.LastRunAt = prevLastRunAt
		job.Attempt = prevAttempt
		job.UpdatedAt = time.Now()
		rbRev, rbErr := s.store.UpdateJob(ctx, job, newRev)
		if err != nil {
			s.logger.Error().String("job_id", job.ID).Err(err).Msg("scheduler: dispatch failed, rolling back status")
			if rbErr != nil {
				s.logger.Error().String("job_id", job.ID).Err(rbErr).Msg("scheduler: rollback after dispatch failure also failed")
			}
			return 0, false
		}
		s.logger.Info().String("job_id", job.ID).String("firing_id", headers[headerFiringID]).Msg("scheduler: firing already dispatched, skipping")
		if rbErr != nil {
			// The earlier run updated the job in the meantime; its state wins.
			return newRev, true
		}
		return rbRev, true
	}

	s.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventJobDispatched,
		JobID:     job.ID,
		TaskType:  job.TaskType,
		Timestamp: firingTime,
	})
	return newRev, true
}

// advanceSchedule calculates and saves the next run time for a schedule.
func (s *Scheduler) advanceSchedule(ctx context.Context, entry *types.ScheduleEntry, now time.Time) {
	nextRun, err := NextRunTime(entry.Schedule, now)
	if err != nil || nextRun.IsZero() {
		s.expireSchedule(ctx, entry)
		s.cache.InvalidateSchedules()
		s.logger.Info().String("job_id", entry.JobID).Msg("scheduler: schedule exhausted")
		return
	}

	if entry.Schedule.EndsAt != nil && nextRun.After(*entry.Schedule.EndsAt) {
		s.expireSchedule(ctx, entry)
		s.cache.InvalidateSchedules()
		s.logger.Info().String("job_id", entry.JobID).Msg("scheduler: schedule will expire before next run")
		return
	}

	entry.NextRunAt = nextRun
	if _, err := s.store.SaveSchedule(ctx, entry); err != nil {
		s.logger.Error().String("job_id", entry.JobID).Err(err).Msg("scheduler: failed to update schedule")
	}
	s.cache.InvalidateSchedules()
}

// checkOrphanedRuns detects runs whose owning node has disappeared (no heartbeat)
// or whose heartbeat has gone stale (>30s without update), and marks them as failed.
// For standalone jobs, it also auto-redispatches if the retry policy permits.
func (s *Scheduler) checkOrphanedRuns(ctx context.Context) {
	runs, err := s.store.ListRuns(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("scheduler: failed to list runs for orphan check")
		return
	}

	// Build set of live node IDs and their active run sets.
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("scheduler: failed to list nodes for orphan check")
		return
	}
	liveNodes := make(map[string]*types.NodeInfo, len(nodes))
	activeRunSet := make(map[string]struct{})
	for _, n := range nodes {
		liveNodes[n.NodeID] = n
		for _, rid := range n.ActiveRunIDs {
			activeRunSet[rid] = struct{}{}
		}
	}

	now := time.Now()
	const defaultHeartbeatStaleThreshold = 30 * time.Second

	// Pre-fetch per-job heartbeat timeouts to avoid N×GetJob calls.
	jobTimeouts := make(map[string]time.Duration)

	// Track dead nodes to clean up their drain keys (persistent since TTL was removed).
	deadNodes := make(map[string]struct{})

	for _, run := range runs {
		// Only check non-terminal runs.
		if run.Status.IsTerminal() {
			continue
		}

		// Determine heartbeat timeout for this run's job.
		staleAge, seen := jobTimeouts[run.JobID]
		if !seen {
			staleAge = defaultHeartbeatStaleThreshold
			if job, _, err := s.cache.GetJob(ctx, run.JobID); err == nil && job.HeartbeatTimeout != nil {
				staleAge = job.HeartbeatTimeout.Std()
			}
			jobTimeouts[run.JobID] = staleAge
		}

		nodeInfo, alive := liveNodes[run.NodeID]
		isOrphaned := !alive

		// Check heartbeat staleness: node is alive but run's heartbeat is stale.
		if alive && run.LastHeartbeatAt != nil {
			if now.Sub(*run.LastHeartbeatAt) > staleAge {
				// Also verify the node doesn't report this run as active.
				if _, active := activeRunSet[run.ID]; !active {
					isOrphaned = true
				}
			}
		}

		// If node is alive but hasn't been alive long enough (fresh restart), check active runs.
		if alive && run.LastHeartbeatAt == nil && nodeInfo != nil {
			if _, active := activeRunSet[run.ID]; !active {
				// Run started before the node's last restart, treat as orphaned.
				if run.StartedAt.Before(nodeInfo.StartedAt) {
					isOrphaned = true
				}
			}
		}

		if !isOrphaned {
			continue
		}

		// This run is orphaned.
		deadNodes[run.NodeID] = struct{}{}
		errMsg := "node " + run.NodeID + " crashed or became unresponsive"
		s.logger.Warn().String("run_id", run.ID).String("job_id", run.JobID).String("node_id", run.NodeID).Msg("scheduler: detected orphaned run")

		run.Status = types.RunStatusFailed
		run.Error = &errMsg
		run.FinishedAt = &now
		run.Duration = now.Sub(run.StartedAt)
		s.store.SaveRun(ctx, run)
		// Record in run history before removing from active set.
		s.store.SaveJobRun(ctx, run)

		// Load the parent job.
		job, rev, err := s.store.GetJob(ctx, run.JobID)
		if err != nil {
			s.store.DeleteRun(ctx, run.ID)
			continue
		}

		// The events are published once the run is out of the active runs and the job is
		// saved, so that whoever reads the job or its runs on one finds what it says.
		crashed := types.JobEvent{
			Type:           types.EventNodeCrashDetected,
			JobID:          run.JobID,
			RunID:          run.ID,
			NodeID:         run.NodeID,
			Error:          &errMsg,
			Timestamp:      now,
			AffectedRunIDs: []string{run.ID},
		}
		var ended *types.JobEvent
		// endedAs is the event of what has become of the job; saved says whether the job was
		// saved so, and the event carries its state then.
		endedAs := func(e types.JobEvent, saved bool) {
			if saved {
				e.SetJob(job)
			}
			ended = &e
		}

		if job.WorkflowID != nil || job.BatchID != nil {
			// Workflow/batch job: publish failure event so the orchestrator handles retry.
			saved := true
			if job.Status == types.JobStatusRunning {
				job.Status = types.JobStatusFailed
				job.LastError = &errMsg
				job.UpdatedAt = now
				_, err := s.store.UpdateJob(ctx, job, rev)
				saved = err == nil
			}

			endedAs(types.JobEvent{
				Type:      types.EventJobFailed,
				JobID:     run.JobID,
				RunID:     run.ID,
				NodeID:    run.NodeID,
				Error:     &errMsg,
				Timestamp: now,
			}, saved)
		} else {
			// Standalone job: check retry policy and auto-redispatch.
			retryPolicy := job.RetryPolicy
			if retryPolicy == nil {
				retryPolicy = types.DefaultRetryPolicy()
			}

			recurring := job.Schedule.Type.IsRecurring()
			maxAttempts := retryPolicy.MaxAttempts
			if recurring && job.DLQAfter != nil && *job.DLQAfter < maxAttempts {
				maxAttempts = *job.DLQAfter
			}
			attempt := lostAttempt(job, run, maxAttempts)
			switch {
			case job.Status == types.JobStatusDead || job.Status == types.JobStatusCancelled:
				// The job ended while this run was still going and gave up its unique
				// key, so the run is dropped rather than redispatched.

			case attempt < maxAttempts:
				// Auto-recover: redispatch.
				prevStatus, prevAttempt := job.Status, job.Attempt
				if recurring {
					// The lost run has used up an attempt, or else a firing that takes
					// its worker down would be redispatched forever.
					attempt++
				}
				job.Attempt = attempt
				job.Status = types.JobStatusPending
				job.LastError = &errMsg
				job.UpdatedAt = now
				newRev, updateErr := s.store.UpdateJob(ctx, job, rev)
				if updateErr != nil {
					s.logger.Error().String("job_id", run.JobID).Err(updateErr).Msg("scheduler: failed to update orphaned job for auto-recovery")
					s.dispatcher.PublishEvent(crashed)
					continue
				}
				// It is the same operation as the lost run, whose message may yet be
				// reclaimed: the ledger lets only one of them complete.
				recovered := *job
				if run.OperationKey != "" {
					recovered.Headers = make(map[string]string, len(job.Headers)+1)
					maps.Copy(recovered.Headers, job.Headers)
					recovered.Headers[headerOperationKey] = run.OperationKey
				}
				if dispErr := s.dispatcher.Dispatch(ctx, &recovered, job.Attempt); dispErr != nil {
					s.logger.Error().String("job_id", run.JobID).Err(dispErr).Msg("scheduler: dispatch failed for orphan recovery, rolling back")
					job.Status = prevStatus
					job.Attempt = prevAttempt
					job.UpdatedAt = time.Now()
					s.store.UpdateJob(ctx, job, newRev)
					s.dispatcher.PublishEvent(crashed)
					continue
				}

				// It does not carry the job's state: by the time it is published, the job
				// may be running again.
				endedAs(types.JobEvent{
					Type:      types.EventJobAutoRecovered,
					JobID:     run.JobID,
					RunID:     run.ID,
					NodeID:    run.NodeID,
					Error:     &errMsg,
					Attempt:   job.Attempt,
					Timestamp: now,
				}, false)

				s.logger.Info().String("job_id", run.JobID).Int("attempt", job.Attempt).Msg("scheduler: auto-recovered orphaned job")

			case s.keepsSchedule(ctx, job):
				// Retries exhausted, but the job goes on with its next firing.
				failed := *job
				failed.Attempt = attempt
				job.Status = types.JobStatusScheduled
				job.Attempt = 0
				job.LastError = &errMsg
				job.UpdatedAt = now
				_, err := s.store.UpdateJob(ctx, job, rev)
				if err != nil {
					s.logger.Error().String("job_id", run.JobID).Err(err).Msg("scheduler: failed to put job with exhausted firing back on its schedule")
				}
				s.dispatcher.DispatchToDLQ(ctx, &failed, errMsg)

				endedAs(types.JobEvent{
					Type:      types.EventJobFailed,
					JobID:     run.JobID,
					RunID:     run.ID,
					NodeID:    run.NodeID,
					TaskType:  job.TaskType,
					Attempt:   attempt,
					Error:     &errMsg,
					Timestamp: now,
				}, err == nil)

			default:
				// Retries exhausted.
				job.Status = types.JobStatusDead
				job.LastError = &errMsg
				job.UpdatedAt = now
				_, err := s.store.UpdateJob(ctx, job, rev)
				if err == nil {
					s.releaseEndedJobUniqueKey(ctx, job.ID)
				}

				endedAs(types.JobEvent{
					Type:      types.EventJobDead,
					JobID:     run.JobID,
					RunID:     run.ID,
					NodeID:    run.NodeID,
					Error:     &errMsg,
					Timestamp: now,
				}, err == nil)
			}
		}

		// Release concurrency slots held by the orphaned run.
		if len(job.ConcurrencyKeys) > 0 {
			for _, ck := range job.ConcurrencyKeys {
				if err := s.store.ReleaseConcurrencySlot(ctx, ck.Key, run.ID); err != nil {
					s.logger.Warn().String("run_id", run.ID).String("conc_key", ck.Key).Err(err).Msg("scheduler: failed to release concurrency slot for orphaned run")
				}
			}
		}

		// Clean up the orphaned run entry.
		s.store.DeleteRun(ctx, run.ID)

		s.dispatcher.PublishEvent(crashed)
		if ended != nil {
			s.dispatcher.PublishEvent(*ended)
		}
	}

	// Clean up persistent drain keys for dead nodes.
	for nodeID := range deadNodes {
		s.store.SetNodeDrain(ctx, nodeID, false)
	}
}

// lostAttempt returns the attempt an orphaned run of a standalone job was on.
//
// A recurring job can have several firings going, so there it is the run's
// attempt that counts. Versions before v0.2.5 counted a recurring job's attempts
// up over its whole life. A run with more attempts than an execution can have, or
// one that such a version started at the limit (it did not record the operation),
// goes by the failures since the job's last success instead.
func lostAttempt(job *types.Job, run *types.JobRun, maxAttempts int) int {
	if !job.Schedule.Type.IsRecurring() {
		return job.Attempt
	}
	if run.Attempt > maxAttempts || (run.Attempt == maxAttempts && run.OperationKey == "") {
		return min(run.Attempt, job.ConsecutiveErrors)
	}
	return run.Attempt
}

// keepsSchedule reports whether a job stays on its schedule when a firing is given up.
func (s *Scheduler) keepsSchedule(ctx context.Context, job *types.Job) bool {
	if !job.Schedule.KeepOnFailure || !job.Schedule.Type.IsRecurring() {
		return false
	}
	_, _, err := s.store.GetSchedule(ctx, job.ID)
	return !errors.Is(err, types.ErrScheduleNotFound)
}

// releaseEndedJobUniqueKey frees the unique key of a dead or cancelled job.
func (s *Scheduler) releaseEndedJobUniqueKey(ctx context.Context, jobID string) {
	if err := s.store.ReleaseEndedJobUniqueKey(ctx, jobID); err != nil {
		s.logger.Warn().String("job_id", jobID).Err(err).Msg("scheduler: failed to release unique key of ended job")
	}
}

// cancelActiveRunsForJob signals cancellation to all active runs of a job.
func (s *Scheduler) cancelActiveRunsForJob(ctx context.Context, jobID string) {
	runs, err := s.store.ListActiveRunsByJobID(ctx, jobID)
	if err != nil {
		s.logger.Warn().String("job_id", jobID).Err(err).Msg("scheduler: failed to list active runs for cancel")
		return
	}
	cancelCh := s.store.CancelChannel()
	for _, run := range runs {
		s.store.Client().Do(ctx, s.store.Client().B().Publish().Channel(cancelCh).Message(run.ID).Build())
	}
}

func (s *Scheduler) expireSchedule(ctx context.Context, entry *types.ScheduleEntry) {
	s.logger.Info().String("job_id", entry.JobID).Msg("scheduler: schedule expired")

	// Delete the schedule.
	s.store.DeleteSchedule(ctx, entry.JobID)

	// Mark the job as completed if it's not currently running.
	// Running jobs will be completed by the worker when execution finishes.
	job, rev, err := s.store.GetJob(ctx, entry.JobID)
	if err != nil {
		return
	}
	now := time.Now()
	if !job.Status.IsTerminal() && job.Status != types.JobStatusRunning {
		job.Status = types.JobStatusCompleted
		job.CompletedAt = &now
		job.UpdatedAt = now
		s.store.UpdateJob(ctx, job, rev)
	}

	s.dispatcher.PublishEvent(types.JobEvent{
		Type:      types.EventScheduleRemoved,
		JobID:     entry.JobID,
		Timestamp: now,
	})
}
