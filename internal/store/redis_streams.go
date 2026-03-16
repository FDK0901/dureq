package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/bytedance/sonic"

	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
)

// ============================================================
// Work stream operations (per-tier Redis Streams)
// ============================================================

// EnsureStreams creates consumer groups for all configured tier streams and the DLQ stream.
// Idempotent — ignores "BUSYGROUP" errors when group already exists.
func (s *RedisStore) EnsureStreams(ctx context.Context) error {
	for _, tier := range s.cfg.Tiers {
		streamKey := WorkStreamKey(s.prefix, tier.Name)
		err := s.rdb.Do(ctx, s.rdb.B().XgroupCreate().Key(streamKey).Group(ConsumerGroup).Id("0").Mkstream().Build()).Error()
		if err != nil && !isGroupExistsErr(err) {
			return fmt.Errorf("create consumer group for tier %s: %w", tier.Name, err)
		}
	}

	// DLQ stream consumer group.
	err := s.rdb.Do(ctx, s.rdb.B().XgroupCreate().Key(DLQStreamKey(s.prefix)).Group(ConsumerGroup).Id("0").Mkstream().Build()).Error()
	if err != nil && !isGroupExistsErr(err) {
		return fmt.Errorf("create DLQ consumer group: %w", err)
	}

	// Orchestrator consumer group on events stream.
	err = s.rdb.Do(ctx, s.rdb.B().XgroupCreate().Key(EventsStreamKey(s.prefix)).Group(OrchestratorConsumerGroup).Id("0").Mkstream().Build()).Error()
	if err != nil && !isGroupExistsErr(err) {
		return fmt.Errorf("create orchestrator consumer group: %w", err)
	}

	return nil
}

// DispatchWork adds a work message to the tier-specific Redis Stream.
// Uses an atomic Lua script (dedup check + XADD + dedup mark) so that
// crash between dedup and XADD can no longer silently lose a message.
func (s *RedisStore) DispatchWork(ctx context.Context, tierName string, wm *types.WorkMessage) (string, error) {
	// FiringID dedup: best-effort pre-check on a separate slot.
	// Not the authoritative gate — the atomic script below is.
	if wm.FiringID != "" {
		firingKey := FiringDedupKey(s.prefix, wm.FiringID)
		err := s.rdb.Do(ctx, s.rdb.B().Set().Key(firingKey).Value(wm.RunID).Nx().Ex(1*time.Hour).Build()).Error()
		if err != nil {
			if rueidis.IsRedisNil(err) {
				return "", nil
			}
			return "", fmt.Errorf("firing dedup check: %w", err)
		}
	}

	// Build XADD field-value pairs as flat string slice for the Lua script.
	args := []string{
		strconv.Itoa(int(s.cfg.DedupTTL.Seconds())), // ARGV[1] = dedup TTL
		"run_id", wm.RunID,
		"job_id", wm.JobID,
		"task_type", string(wm.TaskType),
		"payload", string(wm.Payload),
		"attempt", strconv.Itoa(wm.Attempt),
		"deadline", wm.Deadline.Format(time.RFC3339Nano),
		"priority", strconv.Itoa(int(wm.Priority)),
		"dispatched_at", wm.DispatchedAt.Format(time.RFC3339Nano),
		"tier", tierName,
		"version", wm.Version,
	}
	if wm.FiringID != "" {
		args = append(args, "firing_id", wm.FiringID)
	}
	if wm.OperationKey != "" {
		args = append(args, "operation_key", wm.OperationKey)
	}
	if len(wm.Headers) > 0 {
		if hdr, err := sonic.ConfigFastest.Marshal(wm.Headers); err == nil {
			args = append(args, "headers", string(hdr))
		}
	}
	if len(wm.ConcurrencyKeys) > 0 {
		if ck, err := sonic.ConfigFastest.Marshal(wm.ConcurrencyKeys); err == nil {
			args = append(args, "concurrency_keys", string(ck))
		}
	}

	// Atomic Lua: EXISTS dedup → XADD → SET dedup. Both keys share {tierName} slot.
	streamKey := WorkStreamKey(s.prefix, tierName)
	dedupKey := StreamDedupKey(s.prefix, tierName, wm.RunID)

	msgID, err := s.scriptAtomicDispatch.Exec(ctx, s.rdb,
		[]string{streamKey, dedupKey},
		args,
	).ToString()
	if err != nil {
		return "", fmt.Errorf("atomic dispatch: %w", err)
	}
	if msgID == "DUP" {
		return "", nil // already dispatched — idempotent success
	}

	// Optional durability: wait for replication after critical dispatch write.
	if durErr := s.waitDurability(ctx); durErr != nil {
		s.logger.Warn().Err(durErr).Msg("durability wait failed after dispatch")
	}

	// Fire-and-forget push notification to wake blocked workers immediately.
	s.rdb.Do(ctx, s.rdb.B().Publish().Channel(JobNotifyChannel(s.prefix)).Message(tierName).Build())

	return msgID, nil
}

// DispatchToDLQ adds a message to the dead letter queue stream.
func (s *RedisStore) DispatchToDLQ(ctx context.Context, wm *types.WorkMessage) error {
	_, err := s.rdb.Do(ctx, s.rdb.B().Xadd().Key(DLQStreamKey(s.prefix)).
		Maxlen().Almost().Threshold(strconv.FormatInt(s.cfg.DLQStreamMaxLen, 10)).
		Id("*").FieldValue().
		FieldValue("run_id", wm.RunID).
		FieldValue("job_id", wm.JobID).
		FieldValue("task_type", string(wm.TaskType)).
		FieldValue("payload", string(wm.Payload)).
		FieldValue("attempt", strconv.Itoa(wm.Attempt)).
		FieldValue("error", metaVal(wm.Metadata, "error")).
		Build()).ToString()
	return err
}

// ============================================================
// Event operations (Pub/Sub + Stream for history)
// ============================================================

// PublishEvent publishes an event to both Pub/Sub (real-time) and the event stream (history).
func (s *RedisStore) PublishEvent(ctx context.Context, event types.JobEvent) error {
	data, err := sonic.ConfigFastest.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Slot-targeted commands (XADD, ZADD) — safe for cluster DoMulti.
	cmds := make(rueidis.Commands, 0, 2)

	// Append to durable event stream for history replay.
	cmds = append(cmds, s.rdb.B().Xadd().Key(EventsStreamKey(s.prefix)).
		Maxlen().Almost().Threshold(strconv.FormatInt(s.cfg.EventStreamMaxLen, 10)).
		Id("*").FieldValue().
		FieldValue("type", string(event.Type)).
		FieldValue("job_id", event.JobID).
		FieldValue("run_id", event.RunID).
		FieldValue("node_id", event.NodeID).
		FieldValue("data", string(data)).
		Build())

	// Per-workflow event index: index workflow-related events by workflow ID.
	// Task-level events use WorkflowID (parent), workflow-level events use JobID (which IS the workflow ID).
	if isWorkflowEventType(event.Type) {
		indexID := event.WorkflowID
		if indexID == "" {
			indexID = event.JobID
		}
		if indexID != "" {
			score := float64(event.Timestamp.UnixNano()) / 1e9
			cmds = append(cmds, s.rdb.B().Zadd().Key(WorkflowEventsKey(s.prefix, indexID)).
				ScoreMember().ScoreMember(score, string(data)).Build())
		}
	}

	for _, resp := range s.rdb.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return err
		}
	}

	// Pub/Sub (no-slot) — must be sent separately for Redis Cluster compatibility.
	s.rdb.Do(ctx, s.rdb.B().Publish().Channel(EventsPubSubChannel(s.prefix)).Message(string(data)).Build())
	if event.BatchProgress != nil {
		s.rdb.Do(ctx, s.rdb.B().Publish().Channel(EventsBatchChannel(s.prefix, event.BatchProgress.BatchID)).Message(string(data)).Build())
	}
	return nil
}

// ============================================================
// Job Notification (dureqv2 actor dispatch trigger)
// ============================================================

// PublishJobNotification publishes a lightweight notification to the
// job:notify Pub/Sub channel. The NotifierActor subscribes to this
// channel and forwards it to the DispatcherActor.
func (s *RedisStore) PublishJobNotification(ctx context.Context, jobID, taskType string, priority int) error {
	data, err := sonic.ConfigFastest.Marshal(map[string]interface{}{
		"job_id":    jobID,
		"task_type": taskType,
		"priority":  priority,
	})
	if err != nil {
		return fmt.Errorf("marshal job notification: %w", err)
	}
	return s.rdb.Do(ctx, s.rdb.B().Publish().Channel(JobNotifyChannel(s.prefix)).Message(string(data)).Build()).Error()
}

// ============================================================
// Result operations (Hash + Pub/Sub notification)
// ============================================================

// PublishResult stores a work result and notifies waiting clients.
func (s *RedisStore) PublishResult(ctx context.Context, result types.WorkResult) error {
	data, err := sonic.ConfigFastest.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	resultKey := ResultKey(s.prefix, result.JobID)

	// Slot-targeted commands.
	cmds := make(rueidis.Commands, 0, 2)
	cmds = append(cmds, s.rdb.B().Hset().Key(resultKey).FieldValue().FieldValue("data", string(data)).Build())
	cmds = append(cmds, s.rdb.B().Expire().Key(resultKey).Seconds(int64(s.cfg.ResultTTL.Seconds())).Build())

	for _, resp := range s.rdb.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return err
		}
	}

	// Pub/Sub notify (no-slot) — separate for cluster compatibility.
	s.rdb.Do(ctx, s.rdb.B().Publish().Channel(ResultNotifyChannel(s.prefix, result.JobID)).Message(string(data)).Build())
	return nil
}

// GetResult retrieves a stored work result by job ID.
func (s *RedisStore) GetResult(ctx context.Context, jobID string) (*types.WorkResult, error) {
	dataStr, err := s.rdb.Do(ctx, s.rdb.B().Hget().Key(ResultKey(s.prefix, jobID)).Field("data").Build()).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}

	var result types.WorkResult
	if err := sonic.ConfigFastest.Unmarshal([]byte(dataStr), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ExtendResultTTL extends the TTL of a stored result.
// Used to extend result lifetime for workflow-associated jobs so results
// survive for the workflow's entire execution duration.
func (s *RedisStore) ExtendResultTTL(ctx context.Context, jobID string, ttl time.Duration) error {
	return s.rdb.Do(ctx, s.rdb.B().Expire().Key(ResultKey(s.prefix, jobID)).Seconds(int64(ttl.Seconds())).Build()).Error()
}

// ============================================================
// Batched completion pipeline
// ============================================================

// CompletionBatch holds pre-computed data for a batched job completion write.
type CompletionBatch struct {
	Run            *types.JobRun
	Event          types.JobEvent
	Result         types.WorkResult
	DailyStatField string // "processed" or "failed"
	AckTierName    string // tier name for XACK (v1 only, empty for v2)
	AckMessageID   string // stream message ID for XACK (v1 only)

	// ResultTTLOverride, if > 0, overrides the default ResultTTL for this result.
	// Used by workflow tasks to extend result lifetime to match the workflow deadline.
	ResultTTLOverride time.Duration

	// OperationKey is the stable operation identifier for exactly-once ledger commit.
	// If set, CompleteRun performs a two-phase commit: Lua ledger mark (atomic commit
	// point) followed by the DoMulti bookkeeping pipeline. Empty = skip ledger (backward compat).
	OperationKey string
}

// CompleteRun performs a two-phase completion:
//
//  1. Phase 1 (atomic): If OperationKey is set, mark the operation as "done" in the
//     ledger via Lua script. This is the single atomic commit point for exactly-once (L2).
//     If the ledger returns 0 (duplicate), only XACK is performed and bookkeeping is skipped.
//  2. Phase 2 (pipeline): SaveRun + SaveJobRun + DeleteRun + IncrDailyStat +
//     PublishEvent + PublishResult + AckMessage via DoMulti.
//
// Slot-targeted commands are pipelined via DoMulti; PUBLISH (no-slot)
// commands are sent separately for Redis Cluster compatibility.
func (s *RedisStore) CompleteRun(ctx context.Context, batch *CompletionBatch) error {
	resultData, err := sonic.ConfigFastest.Marshal(batch.Result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	// Phase 1: Operation ledger commit (exactly-once gate).
	// The ledger CAS is the atomic commit point. Phase 2 always runs for
	// idempotent ops (result, history, cleanup, XACK). Non-idempotent ops
	// (stats increment, event stream append) only run on first completion.
	firstCompletion := true
	if batch.OperationKey != "" {
		isNew, err := s.CompleteOperation(ctx, batch.OperationKey, batch.Run.ID, string(resultData))
		if err != nil {
			return fmt.Errorf("operation ledger commit: %w", err)
		}
		firstCompletion = isNew
		if isNew {
			if durErr := s.waitDurability(ctx); durErr != nil {
				s.logger.Warn().Err(durErr).Msg("durability wait failed after ledger commit")
			}
		}
		// isNew=false → replay: run idempotent Phase 2 ops only (result, history, cleanup, XACK).
	}

	// Phase 2: Bookkeeping pipeline.
	// Idempotent ops (HSET, ZADD, SREM, DEL, XACK) always run.
	// Non-idempotent ops (HINCRBY stats, XADD events) only run on firstCompletion.
	runData, err := sonic.ConfigFastest.Marshal(batch.Run)
	if err != nil {
		return fmt.Errorf("marshal run: %w", err)
	}
	eventData, err := sonic.ConfigFastest.Marshal(batch.Event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	cmds := make(rueidis.Commands, 0, 24)

	// 1. SaveRun (terminal status)
	cmds = append(cmds, s.rdb.B().Hset().Key(RunKey(s.prefix, batch.Run.ID)).FieldValue().FieldValue("data", string(runData)).Build())
	cmds = append(cmds, s.rdb.B().Hincrby().Key(RunKey(s.prefix, batch.Run.ID)).Field("_version").Increment(1).Build())
	cmds = append(cmds, s.rdb.B().Srem().Key(RunsActiveKey(s.prefix)).Member(batch.Run.ID).Build())
	cmds = append(cmds, s.rdb.B().Srem().Key(RunsActiveByJobKey(s.prefix, batch.Run.JobID)).Member(batch.Run.ID).Build())
	cmds = append(cmds, s.rdb.B().Zadd().Key(RunsByJobKey(s.prefix, batch.Run.JobID)).ScoreMember().ScoreMember(float64(batch.Run.StartedAt.UnixNano()), batch.Run.ID).Build())

	// 2. SaveJobRun (history) — uses same marshaled data
	score := float64(batch.Run.StartedAt.UnixNano())
	cmds = append(cmds, s.rdb.B().Hset().Key(HistoryRunKey(s.prefix, batch.Run.ID)).FieldValue().FieldValue("data", string(runData)).Build())
	cmds = append(cmds, s.rdb.B().Zadd().Key(HistoryRunsKey(s.prefix)).ScoreMember().ScoreMember(score, batch.Run.ID).Build())
	cmds = append(cmds, s.rdb.B().Zadd().Key(HistoryRunsByJobKey(s.prefix, batch.Run.JobID)).ScoreMember().ScoreMember(score, batch.Run.ID).Build())
	cmds = append(cmds, s.rdb.B().Zadd().Key(HistoryRunsByStatusKey(s.prefix, string(batch.Run.Status))).ScoreMember().ScoreMember(score, batch.Run.ID).Build())

	// 3. DeleteRun — data already in history, clean active tracking
	cmds = append(cmds, s.rdb.B().Del().Key(RunKey(s.prefix, batch.Run.ID)).Build())
	cmds = append(cmds, s.rdb.B().Srem().Key(RunsActiveKey(s.prefix)).Member(batch.Run.ID).Build())
	cmds = append(cmds, s.rdb.B().Zrem().Key(RunsByJobKey(s.prefix, batch.Run.JobID)).Member(batch.Run.ID).Build())
	cmds = append(cmds, s.rdb.B().Srem().Key(RunsActiveByJobKey(s.prefix, batch.Run.JobID)).Member(batch.Run.ID).Build())

	// 4. IncrDailyStat — non-idempotent, skip on replay to avoid double-count.
	if firstCompletion {
		date := time.Now().Format("2006-01-02")
		statsKey := DailyStatsKey(s.prefix, date)
		cmds = append(cmds, s.rdb.B().Hincrby().Key(statsKey).Field(batch.DailyStatField).Increment(1).Build())
		cmds = append(cmds, s.rdb.B().Expire().Key(statsKey).Seconds(int64((91 * 24 * time.Hour).Seconds())).Build())
	}

	// 5. Event stream — only on first completion to avoid duplicate events.
	// Crash recovery (different worker via XAUTOCLAIM) is handled by
	// RepairCompletedOperation which has its own XADD.
	if firstCompletion {
		cmds = append(cmds, s.rdb.B().Xadd().Key(EventsStreamKey(s.prefix)).
			Maxlen().Almost().Threshold(strconv.FormatInt(s.cfg.EventStreamMaxLen, 10)).
			Id("*").FieldValue().
			FieldValue("type", string(batch.Event.Type)).
			FieldValue("job_id", batch.Event.JobID).
			FieldValue("run_id", batch.Event.RunID).
			FieldValue("node_id", batch.Event.NodeID).
			FieldValue("data", string(eventData)).
			Build())
	}

	// 6. Result hash + TTL (slot-targeted)
	resultKey := ResultKey(s.prefix, batch.Result.JobID)
	resultTTL := s.cfg.ResultTTL
	if batch.ResultTTLOverride > 0 {
		resultTTL = batch.ResultTTLOverride
	}
	cmds = append(cmds, s.rdb.B().Hset().Key(resultKey).FieldValue().FieldValue("data", string(resultData)).Build())
	cmds = append(cmds, s.rdb.B().Expire().Key(resultKey).Seconds(int64(resultTTL.Seconds())).Build())

	// 7. AckMessage (v1 only — stream message acknowledgment)
	if batch.AckTierName != "" && batch.AckMessageID != "" {
		cmds = append(cmds, s.rdb.B().Xack().Key(WorkStreamKey(s.prefix, batch.AckTierName)).Group(ConsumerGroup).Id(batch.AckMessageID).Build())
	}

	for _, resp := range s.rdb.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return fmt.Errorf("complete run pipeline: %w", err)
		}
	}

	// Pub/Sub notifications (no-slot) — only on first completion.
	// Crash recovery notifications are handled by RepairCompletedOperation.
	if firstCompletion {
		s.rdb.Do(ctx, s.rdb.B().Publish().Channel(EventsPubSubChannel(s.prefix)).Message(string(eventData)).Build())
		if batch.Event.BatchProgress != nil {
			s.rdb.Do(ctx, s.rdb.B().Publish().Channel(EventsBatchChannel(s.prefix, batch.Event.BatchProgress.BatchID)).Message(string(eventData)).Build())
		}
		s.rdb.Do(ctx, s.rdb.B().Publish().Channel(ResultNotifyChannel(s.prefix, batch.Result.JobID)).Message(string(resultData)).Build())
	}

	return nil
}

// RepairCompletedOperation stores the result from the operation ledger, ACKs the
// stream message, and publishes a result notification. Called by a worker that
// picks up a message via XAUTOCLAIM after the original worker crashed between
// ledger commit and Phase 2 completion. All ops are idempotent.
func (s *RedisStore) RepairCompletedOperation(ctx context.Context, jobID, tierName, msgID, resultJSON string) {
	resultKey := ResultKey(s.prefix, jobID)
	cmds := make(rueidis.Commands, 0, 3)
	cmds = append(cmds, s.rdb.B().Hset().Key(resultKey).FieldValue().FieldValue("data", resultJSON).Build())
	cmds = append(cmds, s.rdb.B().Expire().Key(resultKey).Seconds(int64(s.cfg.ResultTTL.Seconds())).Build())
	if tierName != "" && msgID != "" {
		cmds = append(cmds, s.rdb.B().Xack().Key(WorkStreamKey(s.prefix, tierName)).Group(ConsumerGroup).Id(msgID).Build())
	}
	for _, resp := range s.rdb.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			s.logger.Warn().Err(err).String("job_id", jobID).Msg("repair: pipeline error")
		}
	}
	// Publish completion event to the durable events stream so the orchestrator
	// can progress workflow/batch state. The data field MUST be a serialized
	// JobEvent (not WorkResult), because the orchestrator unmarshals it as JobEvent.
	repairEvent := types.JobEvent{
		Type:      types.EventJobCompleted,
		JobID:     jobID,
		Timestamp: time.Now(),
	}
	eventData, _ := sonic.ConfigFastest.Marshal(repairEvent)
	s.rdb.Do(ctx, s.rdb.B().Xadd().Key(EventsStreamKey(s.prefix)).
		Maxlen().Almost().Threshold(strconv.FormatInt(s.cfg.EventStreamMaxLen, 10)).
		Id("*").FieldValue().
		FieldValue("type", string(types.EventJobCompleted)).
		FieldValue("job_id", jobID).
		FieldValue("data", string(eventData)).
		Build())
	// Pub/Sub for real-time listeners (WebSocket, result waiters).
	s.rdb.Do(ctx, s.rdb.B().Publish().Channel(EventsPubSubChannel(s.prefix)).Message(string(eventData)).Build())
	s.rdb.Do(ctx, s.rdb.B().Publish().Channel(ResultNotifyChannel(s.prefix, jobID)).Message(resultJSON).Build())
}

// ============================================================
// Delayed retry (sorted set + Lua move)
// ============================================================

// AddDelayed adds a work message to the delayed sorted set for future re-dispatch.
func (s *RedisStore) AddDelayed(ctx context.Context, tierName string, wm *types.WorkMessage, executeAt time.Time) error {
	m := map[string]interface{}{
		"run_id":        wm.RunID,
		"job_id":        wm.JobID,
		"task_type":     string(wm.TaskType),
		"payload":       string(wm.Payload),
		"attempt":       wm.Attempt,
		"deadline":      wm.Deadline.Format(time.RFC3339Nano),
		"priority":      int(wm.Priority),
		"dispatched_at": wm.DispatchedAt.Format(time.RFC3339Nano),
		"tier":          tierName,
		"version":       wm.Version,
	}
	if wm.OperationKey != "" {
		m["operation_key"] = wm.OperationKey
	}
	if len(wm.Headers) > 0 {
		if hdr, err := sonic.ConfigFastest.Marshal(wm.Headers); err == nil {
			m["headers"] = string(hdr)
		}
	}
	if len(wm.Metadata) > 0 {
		if meta, err := sonic.ConfigFastest.Marshal(wm.Metadata); err == nil {
			m["metadata"] = string(meta)
		}
	}
	if len(wm.ConcurrencyKeys) > 0 {
		if ck, err := sonic.ConfigFastest.Marshal(wm.ConcurrencyKeys); err == nil {
			m["concurrency_keys"] = string(ck)
		}
	}
	data, err := sonic.ConfigFastest.Marshal(m)
	if err != nil {
		return err
	}

	return s.rdb.Do(ctx, s.rdb.B().Zadd().Key(DelayedKey(s.prefix, tierName)).ScoreMember().ScoreMember(float64(executeAt.UnixNano()), string(data)).Build()).Error()
}

// MoveDelayedToStream atomically moves ripe delayed messages back to the work stream.
// Returns the number of messages moved.
func (s *RedisStore) MoveDelayedToStream(ctx context.Context, tierName string, maxMove int) (int64, error) {
	now := strconv.FormatFloat(float64(time.Now().UnixNano()), 'f', 0, 64)
	result, err := s.scriptMoveDelayed.Exec(ctx, s.rdb,
		[]string{DelayedKey(s.prefix, tierName), WorkStreamKey(s.prefix, tierName)},
		[]string{now, strconv.Itoa(maxMove)},
	).AsInt64()
	if err != nil {
		return 0, err
	}

	// Wake blocked workers so they pick up the moved messages immediately.
	if result > 0 {
		s.rdb.Do(ctx, s.rdb.B().Publish().Channel(JobNotifyChannel(s.prefix)).Message(tierName).Build())
	}

	return result, nil
}

// ReenqueueWork re-adds a work message to the stream without dedup.
// Used when a worker picks up a message for a task type it doesn't handle.
func (s *RedisStore) ReenqueueWork(ctx context.Context, tierName string, wm *types.WorkMessage, redeliveries int) error {
	fv := s.rdb.B().Xadd().Key(WorkStreamKey(s.prefix, tierName)).Id("*").FieldValue().
		FieldValue("run_id", wm.RunID).
		FieldValue("job_id", wm.JobID).
		FieldValue("task_type", string(wm.TaskType)).
		FieldValue("payload", string(wm.Payload)).
		FieldValue("attempt", strconv.Itoa(wm.Attempt)).
		FieldValue("deadline", wm.Deadline.Format(time.RFC3339Nano)).
		FieldValue("priority", strconv.Itoa(int(wm.Priority))).
		FieldValue("dispatched_at", wm.DispatchedAt.Format(time.RFC3339Nano)).
		FieldValue("tier", tierName).
		FieldValue("redeliveries", strconv.Itoa(redeliveries)).
		FieldValue("version", wm.Version)

	if wm.OperationKey != "" {
		fv = fv.FieldValue("operation_key", wm.OperationKey)
	}
	if len(wm.Headers) > 0 {
		if hdr, err := sonic.ConfigFastest.Marshal(wm.Headers); err == nil {
			fv = fv.FieldValue("headers", string(hdr))
		}
	}

	_, err := s.rdb.Do(ctx, fv.Build()).ToString()

	// Wake blocked workers so they pick up the requeued message immediately.
	if err == nil {
		s.rdb.Do(ctx, s.rdb.B().Publish().Channel(JobNotifyChannel(s.prefix)).Message(tierName).Build())
	}

	return err
}

// ============================================================
// Stream acknowledgment helpers
// ============================================================

// AckMessage acknowledges a message in a tier's consumer group.
func (s *RedisStore) AckMessage(ctx context.Context, tierName, messageID string) error {
	return s.rdb.Do(ctx, s.rdb.B().Xack().Key(WorkStreamKey(s.prefix, tierName)).Group(ConsumerGroup).Id(messageID).Build()).Error()
}

// ClaimStaleMessages reclaims messages that have been pending longer than minIdleTime.
// This handles worker crashes — other nodes pick up abandoned work.
func (s *RedisStore) ClaimStaleMessages(ctx context.Context, tierName, consumerName string, minIdleTime time.Duration, count int64) ([]rueidis.XRangeEntry, error) {
	minIdleMs := strconv.FormatInt(minIdleTime.Milliseconds(), 10)
	cmd := s.rdb.B().Xautoclaim().Key(WorkStreamKey(s.prefix, tierName)).Group(ConsumerGroup).Consumer(consumerName).MinIdleTime(minIdleMs).Start("0-0").Count(count).Build()
	// XAUTOCLAIM returns [next-start-id, [[id, [f, v, ...]], ...], [deleted-ids...]]
	arr, err := s.rdb.Do(ctx, cmd).ToArray()
	if err != nil {
		return nil, err
	}
	if len(arr) < 2 {
		return nil, nil
	}
	entries, err := arr[1].AsXRange()
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// ============================================================
// Helpers
// ============================================================

func isGroupExistsErr(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}

func metaVal(m map[string]string, key string) string {
	if m == nil {
		return ""
	}
	return m[key]
}

// isWorkflowEventType returns true if the event type is workflow-related
// and should be indexed in the per-workflow event sorted set.
func isWorkflowEventType(t types.EventType) bool {
	switch t {
	case types.EventWorkflowStarted,
		types.EventWorkflowCompleted,
		types.EventWorkflowFailed,
		types.EventWorkflowCancelled,
		types.EventWorkflowTaskDispatched,
		types.EventWorkflowTaskCompleted,
		types.EventWorkflowTaskFailed,
		types.EventWorkflowTimedOut,
		types.EventWorkflowRetrying,
		types.EventWorkflowSignalReceived:
		return true
	}
	return false
}
