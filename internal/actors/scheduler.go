package actors

import (
	"context"
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq"
	"github.com/FDK0901/dureq/internal/cache"
	"github.com/FDK0901/dureq/internal/messages"
	"github.com/FDK0901/dureq/internal/scheduler"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/anthdm/hollywood/actor"
	"github.com/anthdm/hollywood/cluster"
)

const (
	schedulerTickInterval   = 1 * time.Second
	orphanCheckInterval     = 30 * time.Second
	orphanHeartbeatStaleAge = 30 * time.Second
)

// SchedulerActor is a cluster singleton that periodically checks for due
// schedules and dispatches jobs. It also detects orphaned runs whose owning
// node has disappeared or whose heartbeat has gone stale.
type SchedulerActor struct {
	store          *store.RedisStore
	cache          *cache.ScheduleCache
	cluster        *cluster.Cluster
	dispatcherPID  *actor.PID
	tickRepeater   actor.SendRepeater
	orphanRepeater actor.SendRepeater
	logger         gochainedlog.Logger
}

// NewSchedulerActor returns a Hollywood Producer that creates a SchedulerActor.
// The cluster reference is used to discover the DispatcherActor PID at runtime.
func NewSchedulerActor(s *store.RedisStore, c *cluster.Cluster, logger gochainedlog.Logger) actor.Producer {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return func() actor.Receiver {
		return &SchedulerActor{
			store:   s,
			cache:   cache.NewScheduleCache(s, cache.DefaultCacheConfig()),
			cluster: c,
			logger:  logger,
		}
	}
}

// Receive implements actor.Receiver.
func (s *SchedulerActor) Receive(ctx *actor.Context) {
	switch ctx.Message().(type) {
	case actor.Started:
		s.onStarted(ctx)
	case actor.Stopped:
		s.onStopped()
	case *pb.SingletonCheckMsg:
		ctx.Respond(&pb.SingletonCheckMsg{})
	case messages.TickMsg:
		s.handleTick(ctx)
	case messages.OrphanCheckMsg:
		s.handleOrphanCheck(ctx)
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (s *SchedulerActor) onStarted(ctx *actor.Context) {
	s.tickRepeater = ctx.SendRepeat(ctx.PID(), messages.TickMsg{}, schedulerTickInterval)
	s.orphanRepeater = ctx.SendRepeat(ctx.PID(), messages.OrphanCheckMsg{}, orphanCheckInterval)

	s.logger.Info().Duration("tick_interval", schedulerTickInterval).Duration("orphan_check_interval", orphanCheckInterval).Msg("scheduler actor started")
}

func (s *SchedulerActor) onStopped() {
	s.tickRepeater.Stop()
	s.orphanRepeater.Stop()
	s.logger.Info().Msg("scheduler actor stopped")
}

// ---------------------------------------------------------------------------
// Tick: scan due schedules and dispatch
// ---------------------------------------------------------------------------

func (s *SchedulerActor) handleTick(ctx *actor.Context) {
	now := time.Now()

	bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dueSchedules, err := s.cache.ListDueSchedules(bgCtx, now)
	if err != nil {
		s.logger.Error().Err(err).Msg("scheduler: failed to list due schedules")
		return
	}

	for _, entry := range dueSchedules {
		s.processSchedule(ctx, entry, now)
	}
}

// processSchedule handles a single due schedule entry: validates expiry,
// loads the job, updates state, dispatches, and advances the next run time.
func (s *SchedulerActor) processSchedule(ctx *actor.Context, entry *types.ScheduleEntry, now time.Time) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Check if the schedule has expired.
	if scheduler.IsExpired(entry.Schedule, now) {
		s.expireSchedule(bgCtx, entry)
		return
	}

	// Load the job (via cache).
	job, rev, err := s.cache.GetJob(bgCtx, entry.JobID)
	if err != nil {
		s.logger.Error().String("job_id", entry.JobID).Err(err).Msg("scheduler: failed to get job")
		if err == types.ErrJobNotFound {
			s.store.DeleteSchedule(bgCtx, entry.JobID)
			s.cache.InvalidateSchedules()
		}
		return
	}

	// Skip if job is in a terminal state.
	if job.Status.IsTerminal() {
		s.store.DeleteSchedule(bgCtx, entry.JobID)
		s.cache.InvalidateSchedules()
		return
	}

	// --- Overlap policy check ---
	policy := entry.Schedule.OverlapPolicy
	if policy == "" {
		policy = types.OverlapAllowAll
	}
	if policy != types.OverlapAllowAll {
		hasActive, err := s.store.HasActiveRunForJob(bgCtx, entry.JobID)
		if err != nil {
			s.logger.Warn().String("job_id", entry.JobID).Err(err).Msg("scheduler: overlap check failed")
		} else if hasActive {
			switch policy {
			case types.OverlapSkip:
				s.logger.Debug().String("job_id", entry.JobID).Msg("scheduler: skipping overlapping dispatch")
				s.advanceSchedule(bgCtx, entry, now)
				return
			case types.OverlapBufferOne:
				s.logger.Debug().String("job_id", entry.JobID).Msg("scheduler: buffering overlapping dispatch (one)")
				s.store.SaveOverlapBuffer(bgCtx, entry.JobID, now)
				s.advanceSchedule(bgCtx, entry, now)
				return
			case types.OverlapBufferAll:
				s.logger.Debug().String("job_id", entry.JobID).Msg("scheduler: buffering overlapping dispatch (all)")
				s.store.PushOverlapBufferAll(bgCtx, entry.JobID, now)
				s.advanceSchedule(bgCtx, entry, now)
				return
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

	missedFirings, _ := scheduler.MissedFirings(entry.Schedule, refTime, now, maxBackfill)

	if len(missedFirings) > 1 {
		s.logger.Info().String("job_id", entry.JobID).Int("missed_count", len(missedFirings)).Msg("scheduler: backfilling missed firings")
		currentRev := rev
		for _, firingTime := range missedFirings {
			currentRev = s.dispatchFiring(ctx, bgCtx, job, currentRev, entry, firingTime)
		}
		lastFiring := missedFirings[len(missedFirings)-1]
		entry.LastProcessedAt = &lastFiring
	} else {
		s.dispatchFiring(ctx, bgCtx, job, rev, entry, now)
		entry.LastProcessedAt = &now
	}

	s.cache.InvalidateJob(job.ID)
	s.advanceSchedule(bgCtx, entry, now)
}

// dispatchFiring dispatches a single firing of a scheduled job via the DispatcherActor.
// Returns the new CAS revision so callers can chain updates in a backfill loop.
func (s *SchedulerActor) dispatchFiring(actorCtx *actor.Context, bgCtx context.Context, job *types.Job, rev uint64, entry *types.ScheduleEntry, firingTime time.Time) uint64 {
	job.Status = types.JobStatusPending
	job.LastRunAt = &firingTime
	job.UpdatedAt = time.Now()

	newRev, err := s.store.UpdateJob(bgCtx, job, rev)
	if err != nil {
		s.logger.Warn().String("job_id", job.ID).Err(err).Msg("scheduler: CAS update failed")
		return rev
	}
	rev = newRev

	// Add backfill headers if applicable.
	if entry.Schedule.CatchupWindow != nil {
		if job.Headers == nil {
			job.Headers = make(map[string]string)
		}
		job.Headers["x-dureq-backfill"] = "true"
		job.Headers["x-dureq-scheduled-at"] = firingTime.Format(time.RFC3339Nano)
	}

	if pid := s.resolveDispatcherPID(); pid != nil {
		dispatchMsg := messages.JobToDispatchMsg(job, job.Attempt)
		actorCtx.Send(pid, dispatchMsg)
	}

	s.publishEvent(actorCtx, types.JobEvent{
		Type:      types.EventJobDispatched,
		JobID:     job.ID,
		TaskType:  job.TaskType,
		Timestamp: firingTime,
	})

	return rev
}

// advanceSchedule calculates and saves the next run time for a schedule.
func (s *SchedulerActor) advanceSchedule(bgCtx context.Context, entry *types.ScheduleEntry, now time.Time) {
	nextRun, err := scheduler.NextRunTime(entry.Schedule, now)
	if err != nil || nextRun.IsZero() {
		s.store.DeleteSchedule(bgCtx, entry.JobID)
		s.cache.InvalidateSchedules()
		s.logger.Info().String("job_id", entry.JobID).Msg("scheduler: schedule exhausted")
		return
	}

	if entry.Schedule.EndsAt != nil && nextRun.After(*entry.Schedule.EndsAt) {
		s.store.DeleteSchedule(bgCtx, entry.JobID)
		s.cache.InvalidateSchedules()
		s.logger.Info().String("job_id", entry.JobID).Msg("scheduler: schedule will expire before next run")
		return
	}

	entry.NextRunAt = nextRun
	if _, err := s.store.SaveSchedule(bgCtx, entry); err != nil {
		s.logger.Error().String("job_id", entry.JobID).Err(err).Msg("scheduler: failed to update schedule")
	}
	s.cache.InvalidateSchedules()
}

// expireSchedule marks a schedule as expired and completes the parent job.
func (s *SchedulerActor) expireSchedule(ctx context.Context, entry *types.ScheduleEntry) {
	s.logger.Info().String("job_id", entry.JobID).Msg("scheduler: schedule expired")

	s.store.DeleteSchedule(ctx, entry.JobID)

	job, rev, err := s.store.GetJob(ctx, entry.JobID)
	if err != nil {
		return
	}
	now := time.Now()
	job.Status = types.JobStatusCompleted
	job.CompletedAt = &now
	job.UpdatedAt = now
	s.store.UpdateJob(ctx, job, rev)
}

// ---------------------------------------------------------------------------
// Orphan check: detect runs stuck on dead/stale nodes
// ---------------------------------------------------------------------------

func (s *SchedulerActor) handleOrphanCheck(ctx *actor.Context) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runs, err := s.store.ListRuns(bgCtx)
	if err != nil {
		s.logger.Error().Err(err).Msg("scheduler: failed to list runs for orphan check")
		return
	}

	nodes, err := s.store.ListNodes(bgCtx)
	if err != nil {
		s.logger.Error().Err(err).Msg("scheduler: failed to list nodes for orphan check")
		return
	}

	// Build set of live nodes and their active runs.
	liveNodes := make(map[string]*types.NodeInfo, len(nodes))
	activeRunSet := make(map[string]struct{})
	for _, n := range nodes {
		liveNodes[n.NodeID] = n
		for _, rid := range n.ActiveRunIDs {
			activeRunSet[rid] = struct{}{}
		}
	}

	now := time.Now()

	// Pre-fetch per-job heartbeat timeouts to avoid N×GetJob calls.
	jobTimeouts := make(map[string]time.Duration)

	for _, run := range runs {
		if run.Status.IsTerminal() {
			continue
		}

		// Determine heartbeat timeout for this run's job.
		staleAge, seen := jobTimeouts[run.JobID]
		if !seen {
			staleAge = orphanHeartbeatStaleAge
			if job, _, err := s.cache.GetJob(bgCtx, run.JobID); err == nil && job.HeartbeatTimeout != nil {
				staleAge = job.HeartbeatTimeout.Std()
			}
			jobTimeouts[run.JobID] = staleAge
		}

		nodeInfo, alive := liveNodes[run.NodeID]
		isOrphaned := !alive

		// Node alive but heartbeat stale and run not reported as active.
		if alive && run.LastHeartbeatAt != nil {
			if now.Sub(*run.LastHeartbeatAt) > staleAge {
				if _, active := activeRunSet[run.ID]; !active {
					isOrphaned = true
				}
			}
		}

		// Node alive but run started before node restart and not in active set.
		if alive && run.LastHeartbeatAt == nil && nodeInfo != nil {
			if _, active := activeRunSet[run.ID]; !active {
				if run.StartedAt.Before(nodeInfo.StartedAt) {
					isOrphaned = true
				}
			}
		}

		if !isOrphaned {
			continue
		}

		s.handleOrphanedRun(ctx, bgCtx, run, now)
	}
}

// handleOrphanedRun marks a run as failed and either re-dispatches (standalone)
// or publishes a failure event (workflow/batch).
func (s *SchedulerActor) handleOrphanedRun(actorCtx *actor.Context, bgCtx context.Context, run *types.JobRun, now time.Time) {
	errMsg := "node " + run.NodeID + " crashed or became unresponsive"

	s.logger.Warn().String("run_id", run.ID).String("job_id", run.JobID).String("node_id", run.NodeID).Msg("scheduler: detected orphaned run")

	// Mark run as failed.
	run.Status = types.RunStatusFailed
	run.Error = &errMsg
	run.FinishedAt = &now
	run.Duration = now.Sub(run.StartedAt)
	s.store.SaveRun(bgCtx, run)

	// Load the parent job.
	job, rev, err := s.store.GetJob(bgCtx, run.JobID)
	if err != nil {
		s.store.DeleteRun(bgCtx, run.ID)
		return
	}

	// Publish crash detection event.
	s.publishEvent(actorCtx, types.JobEvent{
		Type:           types.EventNodeCrashDetected,
		JobID:          run.JobID,
		RunID:          run.ID,
		NodeID:         run.NodeID,
		Error:          &errMsg,
		Timestamp:      now,
		AffectedRunIDs: []string{run.ID},
	})

	if job.WorkflowID != nil || job.BatchID != nil {
		// Workflow/batch job: publish failure event so the orchestrator handles it.
		if job.Status == types.JobStatusRunning {
			job.Status = types.JobStatusFailed
			job.LastError = &errMsg
			job.UpdatedAt = now
			s.store.UpdateJob(bgCtx, job, rev)
		}

		s.publishEvent(actorCtx, types.JobEvent{
			Type:      types.EventJobFailed,
			JobID:     run.JobID,
			RunID:     run.ID,
			NodeID:    run.NodeID,
			Error:     &errMsg,
			Timestamp: now,
		})
	} else {
		// Standalone job: check retry policy and auto-redispatch.
		retryPolicy := job.RetryPolicy
		if retryPolicy == nil {
			retryPolicy = types.DefaultRetryPolicy()
		}

		if job.Attempt < retryPolicy.MaxAttempts {
			// Auto-recover: redispatch.
			job.Status = types.JobStatusPending
			job.LastError = &errMsg
			job.UpdatedAt = now
			s.store.UpdateJob(bgCtx, job, rev)

			if pid := s.resolveDispatcherPID(); pid != nil {
				dispatchMsg := messages.JobToDispatchMsg(job, job.Attempt)
				actorCtx.Send(pid, dispatchMsg)
			}

			s.publishEvent(actorCtx, types.JobEvent{
				Type:      types.EventJobAutoRecovered,
				JobID:     run.JobID,
				RunID:     run.ID,
				NodeID:    run.NodeID,
				Error:     &errMsg,
				Attempt:   job.Attempt,
				Timestamp: now,
			})

			s.logger.Info().String("job_id", run.JobID).Int("attempt", job.Attempt).Msg("scheduler: auto-recovered orphaned job")
		} else {
			// Retries exhausted.
			job.Status = types.JobStatusDead
			job.LastError = &errMsg
			job.UpdatedAt = now
			s.store.UpdateJob(bgCtx, job, rev)

			s.publishEvent(actorCtx, types.JobEvent{
				Type:      types.EventJobDead,
				JobID:     run.JobID,
				RunID:     run.ID,
				NodeID:    run.NodeID,
				Error:     &errMsg,
				Timestamp: now,
			})
		}
	}

	// Clean up the orphaned run entry.
	s.store.DeleteRun(bgCtx, run.ID)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// resolveDispatcherPID lazily looks up the DispatcherActor PID via the cluster.
func (s *SchedulerActor) resolveDispatcherPID() *actor.PID {
	if s.dispatcherPID != nil {
		return s.dispatcherPID
	}
	if s.cluster == nil {
		return nil
	}
	pids := s.cluster.GetActiveByKind(KindDispatcher)
	if len(pids) > 0 && pids[0] != nil {
		s.dispatcherPID = pids[0]
	}
	return s.dispatcherPID
}

// publishEvent publishes a domain event directly to the Redis store.
func (s *SchedulerActor) publishEvent(_ *actor.Context, event types.JobEvent) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.store.PublishEvent(bgCtx, event); err != nil {
		s.logger.Warn().String("type", string(event.Type)).Err(err).Msg("scheduler: failed to publish event")
	}
}
