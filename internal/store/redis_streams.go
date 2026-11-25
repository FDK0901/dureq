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

	// Orchestrator consumer groups: on its stream, and on the events stream for events from
	// nodes before v0.3.0.
	for _, key := range []string{OrchestratorStreamKey(s.prefix), EventsStreamKey(s.prefix)} {
		err = s.rdb.Do(ctx, s.rdb.B().XgroupCreate().Key(key).Group(OrchestratorConsumerGroup).Id("0").Mkstream().Build()).Error()
		if err != nil && !isGroupExistsErr(err) {
			return fmt.Errorf("create orchestrator consumer group: %w", err)
		}
	}

	return nil
}

// firingDedupTTL bounds how long a dispatched schedule firing is remembered.
const firingDedupTTL = 1 * time.Hour

// DispatchWork adds a work message to the tier-specific Redis Stream.
// Uses an atomic Lua script (RunID/FiringID dedup check + XADD + dedup marks)
// so that a crash or error before the XADD never consumes the dedup marks
// and silently loses the message.
func (s *RedisStore) DispatchWork(ctx context.Context, tierName string, wm *types.WorkMessage) (string, error) {
	// Build XADD field-value pairs as flat string slice for the Lua script.
	args := []string{
		strconv.Itoa(int(s.cfg.DedupTTL.Seconds())), // ARGV[1] = run dedup TTL
		strconv.Itoa(int(firingDedupTTL.Seconds())), // ARGV[2] = firing dedup TTL
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

	// Atomic Lua: EXISTS dedup → XADD → SET dedup. All keys share {tierName} slot.
	keys := []string{
		WorkStreamKey(s.prefix, tierName),
		StreamDedupKey(s.prefix, tierName, wm.RunID),
	}
	if wm.FiringID != "" {
		keys = append(keys, FiringDedupKey(s.prefix, tierName, wm.FiringID))
	}

	msgID, err := s.scriptAtomicDispatch.Exec(ctx, s.rdb, keys, args).ToString()
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
	add := s.rdb.B().Xadd().Key(DLQStreamKey(s.prefix)).Id("*")
	if s.cfg.DLQStreamMaxLen > 0 {
		add = s.rdb.B().Xadd().Key(DLQStreamKey(s.prefix)).Maxlen().Almost().Threshold(strconv.FormatInt(s.cfg.DLQStreamMaxLen, 10)).Id("*")
	}
	_, err := s.rdb.Do(ctx, add.FieldValue().
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
	cmds, publish, err := s.eventCmds(event)
	if err != nil {
		return err
	}
	return s.doAndPublish(ctx, cmds, publish)
}

// StartRun stores a run that starts, and publishes the event of that.
func (s *RedisStore) StartRun(ctx context.Context, run *types.JobRun, event types.JobEvent) error {
	cmds, err := s.runCmds(run)
	if err != nil {
		return err
	}
	eventCmds, publish, err := s.eventCmds(event)
	if err != nil {
		return err
	}
	return s.doAndPublish(ctx, append(cmds, eventCmds...), publish)
}

// eventCmds returns the commands that store an event, and those that publish it.
func (s *RedisStore) eventCmds(event types.JobEvent) (cmds, publish rueidis.Commands, err error) {
	data, err := sonic.ConfigFastest.Marshal(event)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal event: %w", err)
	}

	// For the orchestrator, until it has processed the event.
	if forOrchestrator(event) {
		cmds = append(cmds, s.addOrchestratorEventCmd(string(event.Type), event.JobID, string(data)))
	}

	// Append to durable event stream for history replay.
	cmds = append(cmds, s.rdb.B().Xadd().Key(EventsStreamKey(s.prefix)).
		Maxlen().Almost().Threshold(strconv.FormatInt(s.cfg.EventStreamMaxLen, 10)).
		Id("*").FieldValue().
		FieldValue("type", string(event.Type)).
		FieldValue("job_id", event.JobID).
		FieldValue("run_id", event.RunID).
		FieldValue("node_id", event.NodeID).
		FieldValue("data", string(data)).
		FieldValue(routedField, "1").
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
	return cmds, s.publishEventCmds(event, data), nil
}

// routedField marks an event in the event stream whose writer has given the orchestrator its
// own copy, if that has to see it. Nodes before v0.3.0 do not set it.
const routedField = "routed"

// forOrchestrator reports whether the orchestrator has to see an event.
func forOrchestrator(event types.JobEvent) bool {
	return OrchestratorHandles(event.Type) && !event.Standalone
}

// OrchestratorHandles reports whether there is something for the orchestrator to do on events
// of a type.
func OrchestratorHandles(t types.EventType) bool {
	switch t {
	case types.EventJobCompleted, types.EventJobFailed, types.EventJobDead,
		types.EventWorkflowStarted, types.EventWorkflowSignalReceived, types.EventWorkflowResumed, types.EventWorkflowRetrying,
		types.EventWorkflowCompleted, types.EventWorkflowFailed, types.EventWorkflowTimedOut, types.EventWorkflowCancelled,
		types.EventBatchRetrying:
		return true
	}
	return false
}

// addOrchestratorEventCmd returns the command that gives an event to the orchestrator. It is
// not limited in length: an event stays until it has been processed.
func (s *RedisStore) addOrchestratorEventCmd(eventType, jobID, data string) rueidis.Completed {
	return s.rdb.B().Xadd().Key(OrchestratorStreamKey(s.prefix)).Id("*").FieldValue().
		FieldValue("type", eventType).
		FieldValue("job_id", jobID).
		FieldValue("data", data).
		Build()
}

// DoneOrchestratorEvents removes events that have been processed.
func (s *RedisStore) DoneOrchestratorEvents(ctx context.Context, ids ...string) {
	if len(ids) == 0 {
		return
	}
	key := OrchestratorStreamKey(s.prefix)
	s.rdb.DoMulti(ctx,
		s.rdb.B().Xack().Key(key).Group(OrchestratorConsumerGroup).Id(ids...).Build(),
		s.rdb.B().Xdel().Key(key).Id(ids...).Build())
}

// ForwardEvents gives the orchestrator its copy of events in the event stream from nodes
// before v0.3.0, which do not. It looks at up to count events and returns how many there were.
// Those that an orchestrator of such a version had taken and not finished come first.
func (s *RedisStore) ForwardEvents(ctx context.Context, consumer string, count int64) (int, error) {
	key := EventsStreamKey(s.prefix)
	read := func(id string) ([]rueidis.XRangeEntry, error) {
		streams, err := s.rdb.Do(ctx, s.rdb.B().Xreadgroup().Group(OrchestratorConsumerGroup, consumer).Count(count).Streams().Key(key).Id(id).Build()).AsXRead()
		if err != nil && !rueidis.IsRedisNil(err) {
			return nil, fmt.Errorf("forward events: %w", err)
		}
		return streams[key], nil
	}
	entries, err := read("0")
	if err == nil && len(entries) == 0 {
		entries, err = read(">")
	}
	if err != nil || len(entries) == 0 {
		return 0, err
	}

	ids := make([]string, 0, len(entries))
	var forward rueidis.Commands
	for _, e := range entries {
		ids = append(ids, e.ID)
		if e.FieldValues[routedField] == "" && OrchestratorHandles(types.EventType(e.FieldValues["type"])) {
			forward = append(forward, s.addOrchestratorEventCmd(e.FieldValues["type"], e.FieldValues["job_id"], e.FieldValues["data"]))
		}
	}
	for _, r := range s.rdb.DoMulti(ctx, forward...) {
		if err := r.Error(); err != nil {
			return 0, fmt.Errorf("forward events: %w", err)
		}
	}
	if err := s.rdb.Do(ctx, s.rdb.B().Xack().Key(key).Group(OrchestratorConsumerGroup).Id(ids...).Build()).Error(); err != nil {
		return 0, fmt.Errorf("forward events: %w", err)
	}
	return len(entries), nil
}

// publishEventCmds returns the commands that publish an event via Pub/Sub.
func (s *RedisStore) publishEventCmds(event types.JobEvent, data []byte) rueidis.Commands {
	cmds := rueidis.Commands{s.rdb.B().Publish().Channel(EventsPubSubChannel(s.prefix)).Message(string(data)).Build()}
	if event.BatchProgress != nil {
		cmds = append(cmds, s.rdb.B().Publish().Channel(EventsBatchChannel(s.prefix, event.BatchProgress.BatchID)).Message(string(data)).Build())
	}
	return cmds
}

// doAndPublish runs cmds and then publish, which are PUBLISH commands, in one round trip. With
// Redis Cluster and several cmds it takes two: the latter have no slot and cannot be mixed
// with several. It returns the first error of cmds.
func (s *RedisStore) doAndPublish(ctx context.Context, cmds, publish rueidis.Commands) error {
	n := len(cmds)
	apart := n > 1 && s.rdb.Mode() == rueidis.ClientModeCluster
	if !apart {
		cmds = append(cmds, publish...)
	}
	if len(cmds) == 0 {
		return nil
	}
	for _, r := range s.rdb.DoMulti(ctx, cmds...)[:n] {
		if err := r.Error(); err != nil {
			return err
		}
	}
	if apart && len(publish) > 0 {
		s.rdb.DoMulti(ctx, publish...)
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

	// How far CompleteRun got, if it fails and is called again.
	committed bool // this batch has marked the ledger, or may have
	stored    bool // nothing but the acknowledgement is missing
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
	var publish rueidis.Commands
	if !batch.stored {
		var err error
		if publish, err = s.storeCompletion(ctx, batch); err != nil {
			return err
		}
		batch.stored = true
	}

	// Phase 3: AckMessage, now that nothing is left to do, and Pub/Sub notifications.
	// Crash recovery notifications are handled by RepairCompletedOperation.
	if err := s.doAndPublish(ctx, s.ackCmds(batch.AckTierName, batch.AckMessageID), publish); err != nil {
		return fmt.Errorf("complete run ack: %w", err)
	}
	return nil
}

// storeCompletion is the first two phases of CompleteRun. It returns the commands that
// publish the notifications.
func (s *RedisStore) storeCompletion(ctx context.Context, batch *CompletionBatch) (publish rueidis.Commands, err error) {
	resultData, err := sonic.ConfigFastest.Marshal(batch.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}

	// Phase 1: Operation ledger commit (exactly-once gate).
	// The ledger CAS is the atomic commit point. Phase 2 always runs for
	// idempotent ops (result, history, cleanup). Non-idempotent ops
	// (stats increment, event stream append) only run on first completion (of the
	// operation, and of the job).
	firstCompletion := true
	firstOfJob := true
	if batch.OperationKey != "" {
		isNew, err := s.CompleteOperation(ctx, batch.OperationKey, batch.Run.ID, string(resultData))
		if err != nil {
			batch.committed = true // perhaps; an event too many does no harm
			return nil, fmt.Errorf("operation ledger commit: %w", err)
		}
		firstCompletion = isNew
		batch.committed = batch.committed || isNew
		firstOfJob = batch.committed || s.completedByOtherJob(ctx, batch.OperationKey, batch.Run.JobID)
		if isNew {
			if durErr := s.waitDurability(ctx); durErr != nil {
				s.logger.Warn().Err(durErr).Msg("durability wait failed after ledger commit")
			}
		}
		// isNew=false → replay: run idempotent Phase 2 ops only (result, history, cleanup).
	}

	// Phase 2: Bookkeeping pipeline.
	// Idempotent ops (HSET, ZADD, SREM, DEL) always run.
	// Non-idempotent ops (HINCRBY stats, XADD events) only run on firstCompletion.
	runData, err := sonic.ConfigFastest.Marshal(batch.Run)
	if err != nil {
		return nil, fmt.Errorf("marshal run: %w", err)
	}

	cmds := make(rueidis.Commands, 0, 24)

	// 1. SaveJobRun (history)
	cmds = append(cmds, s.historyRunCmds(batch.Run, runData)...)

	// 2. DeleteRun — data already in history, clean active tracking
	cmds = append(cmds, s.rdb.B().Del().Key(RunKey(s.prefix, batch.Run.ID)).Build())
	cmds = append(cmds, s.rdb.B().Srem().Key(RunsActiveKey(s.prefix)).Member(batch.Run.ID).Build())
	cmds = append(cmds, s.rdb.B().Zrem().Key(RunsByJobKey(s.prefix, batch.Run.JobID)).Member(batch.Run.ID).Build())
	cmds = append(cmds, s.rdb.B().Srem().Key(RunsActiveByJobKey(s.prefix, batch.Run.JobID)).Member(batch.Run.ID).Build())
	cmds = append(cmds, s.rdb.B().Del().Key(CancelFlagKey(s.prefix, batch.Run.ID)).Build())

	// 3. IncrDailyStat — non-idempotent, skip on replay to avoid double-count.
	if firstCompletion {
		date := time.Now().Format("2006-01-02")
		statsKey := DailyStatsKey(s.prefix, date)
		cmds = append(cmds, s.rdb.B().Hincrby().Key(statsKey).Field(batch.DailyStatField).Increment(1).Build())
		cmds = append(cmds, s.rdb.B().Expire().Key(statsKey).Seconds(int64((91 * 24 * time.Hour).Seconds())).Build())
	}

	// 4. Event stream — only on first completion to avoid duplicate events, but for every
	// job: whoever waits for this one does not see the event of the other.
	// Crash recovery (different worker via XAUTOCLAIM) is handled by
	// RepairCompletedOperation which has its own XADD.
	if firstOfJob {
		eventCmds, publishCmds, err := s.eventCmds(batch.Event)
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, eventCmds...)
		publish = append(publishCmds,
			s.rdb.B().Publish().Channel(ResultNotifyChannel(s.prefix, batch.Result.JobID)).Message(string(resultData)).Build())
	}

	// 5. Result hash + TTL
	resultKey := ResultKey(s.prefix, batch.Result.JobID)
	resultTTL := s.cfg.ResultTTL
	if batch.ResultTTLOverride > 0 {
		resultTTL = batch.ResultTTLOverride
	}
	cmds = append(cmds, s.rdb.B().Hset().Key(resultKey).FieldValue().FieldValue("data", string(resultData)).Build())
	cmds = append(cmds, s.rdb.B().Expire().Key(resultKey).Seconds(int64(resultTTL.Seconds())).Build())

	for _, resp := range s.rdb.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return nil, fmt.Errorf("complete run pipeline: %w", err)
		}
	}

	return publish, nil
}

// ackCmds returns the command that acknowledges a work message, if there is one.
func (s *RedisStore) ackCmds(tierName, msgID string) rueidis.Commands {
	if tierName == "" || msgID == "" {
		return nil
	}
	return rueidis.Commands{s.rdb.B().Xack().Key(WorkStreamKey(s.prefix, tierName)).Group(ConsumerGroup).Id(msgID).Build()}
}

// completedByOtherJob reports whether an operation in the ledger was completed by a job other
// than jobID: a retry that overlaps with the execution it retries is a job of its own.
func (s *RedisStore) completedByOtherJob(ctx context.Context, opKey, jobID string) bool {
	data, err := s.rdb.Do(ctx, s.rdb.B().Hget().Key(OperationLedgerKey(s.prefix, opKey)).Field("result").Build()).ToString()
	if err != nil {
		return false
	}
	var result types.WorkResult
	if err := sonic.ConfigFastest.Unmarshal([]byte(data), &result); err != nil {
		return false
	}
	return result.JobID != "" && result.JobID != jobID
}

// RepairCompletedOperation stores the result from the operation ledger, ACKs the
// stream message, and publishes a result notification. Called by a worker that
// picks up a message via XAUTOCLAIM after the original worker crashed between
// ledger commit and Phase 2 completion. All ops are idempotent.
func (s *RedisStore) RepairCompletedOperation(ctx context.Context, jobID, tierName, msgID, resultJSON string) {
	// The message is acknowledged last: until then, this is done again if it is interrupted.
	resultKey := ResultKey(s.prefix, jobID)
	cmds := rueidis.Commands{
		s.rdb.B().Hset().Key(resultKey).FieldValue().FieldValue("data", resultJSON).Build(),
		s.rdb.B().Expire().Key(resultKey).Seconds(int64(s.cfg.ResultTTL.Seconds())).Build(),
	}
	// Publish completion event to the durable events stream so the orchestrator
	// can progress workflow/batch state. The data field MUST be a serialized
	// JobEvent (not WorkResult), because the orchestrator unmarshals it as JobEvent.
	eventCmds, publish, err := s.eventCmds(types.JobEvent{
		Type:      types.EventJobCompleted,
		JobID:     jobID,
		Timestamp: time.Now(),
	})
	if err != nil {
		s.logger.Warn().Err(err).String("job_id", jobID).Msg("repair: event error")
		return
	}
	for _, resp := range s.rdb.DoMulti(ctx, append(cmds, eventCmds...)...) {
		if err := resp.Error(); err != nil {
			s.logger.Warn().Err(err).String("job_id", jobID).Msg("repair: pipeline error")
			return
		}
	}
	// Pub/Sub for real-time listeners (WebSocket, result waiters).
	publish = append(publish, s.rdb.B().Publish().Channel(ResultNotifyChannel(s.prefix, jobID)).Message(resultJSON).Build())
	if err := s.doAndPublish(ctx, s.ackCmds(tierName, msgID), publish); err != nil {
		s.logger.Warn().Err(err).String("job_id", jobID).Msg("repair: ack error")
	}
}

// ============================================================
// Delayed retry (sorted set + Lua move)
// ============================================================

// AddDelayed adds a work message to the delayed sorted set for future re-dispatch.
func (s *RedisStore) AddDelayed(ctx context.Context, tierName string, wm *types.WorkMessage, executeAt time.Time) error {
	data, err := delayedMessage(tierName, wm)
	if err != nil {
		return err
	}

	return s.rdb.Do(ctx, s.rdb.B().Zadd().Key(DelayedKey(s.prefix, tierName)).ScoreMember().ScoreMember(float64(executeAt.UnixNano()), string(data)).Build()).Error()
}

// PostponedMessage is a message that a worker has taken from a work stream and cannot process
// for now.
type PostponedMessage struct {
	Work      *types.WorkMessage
	MessageID string
	Until     time.Time
}

// Postpone moves messages from the work stream of a tier to its delayed sorted set.
func (s *RedisStore) Postpone(ctx context.Context, tierName string, msgs []PostponedMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	args := make([]string, 0, 1+3*len(msgs))
	args = append(args, ConsumerGroup)
	for _, m := range msgs {
		data, err := delayedMessage(tierName, m.Work)
		if err != nil {
			return err
		}
		args = append(args, strconv.FormatFloat(float64(m.Until.UnixNano()), 'f', 0, 64), string(data), m.MessageID)
	}
	keys := []string{DelayedKey(s.prefix, tierName), WorkStreamKey(s.prefix, tierName)}
	if err := s.scriptPostpone.Exec(ctx, s.rdb, keys, args).Error(); err != nil {
		return fmt.Errorf("postpone: %w", err)
	}
	return nil
}

// delayedMessage returns a work message as it is stored in the delayed sorted set.
func delayedMessage(tierName string, wm *types.WorkMessage) ([]byte, error) {
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
	return sonic.ConfigFastest.Marshal(m)
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
	if len(wm.ConcurrencyKeys) > 0 {
		if ck, err := sonic.ConfigFastest.Marshal(wm.ConcurrencyKeys); err == nil {
			fv = fv.FieldValue("concurrency_keys", string(ck))
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

// RefreshMessageClaim resets a pending message's idle time (XCLAIM ... JUSTID)
// so ClaimStaleMessages does not hand a still-running message to another worker.
// No-op if the message is no longer pending (already ACK'd).
func (s *RedisStore) RefreshMessageClaim(ctx context.Context, tierName, consumerName, messageID string) error {
	return s.rdb.Do(ctx, s.rdb.B().Xclaim().Key(WorkStreamKey(s.prefix, tierName)).Group(ConsumerGroup).Consumer(consumerName).MinIdleTime("0").Id(messageID).Justid().Build()).Error()
}

// QueueDepth tells how much work a tier has.
type QueueDepth struct {
	Waiting  *int64 // messages no worker has taken yet; nil if the server cannot tell (Redis before 7.0)
	InFlight int64  // messages taken by a worker and not acknowledged yet
	Delayed  int64  // messages due later, such as retries
}

// GetQueueDepth returns the depth of a tier. The length of its stream is no measure of
// that: the stream also holds the messages that have been processed.
func (s *RedisStore) GetQueueDepth(ctx context.Context, tierName string) (QueueDepth, error) {
	var depth QueueDepth
	results := s.rdb.DoMulti(ctx,
		s.rdb.B().XinfoGroups().Key(WorkStreamKey(s.prefix, tierName)).Build(),
		s.rdb.B().Zcard().Key(DelayedKey(s.prefix, tierName)).Build(),
	)
	groups, err := results[0].ToArray()
	if err != nil && strings.Contains(err.Error(), "no such key") { // nothing was ever dispatched
		groups, err, depth.Waiting = nil, nil, new(int64)
	}
	if err != nil {
		return depth, fmt.Errorf("queue depth of %s: %w", tierName, err)
	}
	if depth.Delayed, err = results[1].AsInt64(); err != nil {
		return depth, fmt.Errorf("queue depth of %s: %w", tierName, err)
	}
	for _, g := range groups {
		info, err := g.AsMap()
		if err != nil {
			return depth, fmt.Errorf("queue depth of %s: %w", tierName, err)
		}
		name := info["name"]
		if group, _ := name.ToString(); group != ConsumerGroup {
			continue
		}
		pending, lag := info["pending"], info["lag"]
		if depth.InFlight, err = pending.AsInt64(); err != nil {
			return depth, fmt.Errorf("queue depth of %s: %w", tierName, err)
		}
		if n, err := lag.AsInt64(); err == nil {
			depth.Waiting = &n
		}
	}
	return depth, nil
}

// trimRounds bounds how much TrimWorkStream does at once: Redis removes up to about
// 10,000 messages a round.
const trimRounds = 50

// TrimWorkStream removes messages from a tier's stream that every consumer group has
// taken and acknowledged, and returns how many. Acknowledging a message leaves it in the
// stream. It removes whole blocks of messages only, so that about a hundred stay.
func (s *RedisStore) TrimWorkStream(ctx context.Context, tierName string) (int64, error) {
	key := WorkStreamKey(s.prefix, tierName)
	groups, err := s.rdb.Do(ctx, s.rdb.B().XinfoGroups().Key(key).Build()).ToArray()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return 0, nil
		}
		return 0, fmt.Errorf("trim work stream of %s: %w", tierName, err)
	}

	// Up to what a group has taken, all messages but its pending ones are done. One that
	// is taken from here on comes after that.
	var oldest streamID
	for i, g := range groups {
		info, err := g.AsMap()
		if err != nil {
			return 0, fmt.Errorf("trim work stream of %s: %w", tierName, err)
		}
		nameMsg, deliveredMsg := info["name"], info["last-delivered-id"]
		name, err := nameMsg.ToString()
		if err != nil {
			return 0, fmt.Errorf("trim work stream of %s: %w", tierName, err)
		}
		delivered, err := deliveredMsg.ToString()
		if err != nil {
			return 0, fmt.Errorf("trim work stream of %s: %w", tierName, err)
		}
		needed, err := parseStreamID(delivered)
		if err != nil {
			return 0, err
		}

		// [count, smallest ID, largest ID, consumers]; the IDs are nil if there are none.
		pending, err := s.rdb.Do(ctx, s.rdb.B().Xpending().Key(key).Group(name).Build()).ToArray()
		if err != nil {
			return 0, fmt.Errorf("trim work stream of %s: %w", tierName, err)
		}
		if len(pending) > 1 && !pending[1].IsNil() {
			smallest, err := pending[1].ToString()
			if err != nil {
				return 0, fmt.Errorf("trim work stream of %s: %w", tierName, err)
			}
			id, err := parseStreamID(smallest)
			if err != nil {
				return 0, err
			}
			if id.before(needed) {
				needed = id
			}
		}
		if i == 0 || needed.before(oldest) {
			oldest = needed
		}
	}
	if len(groups) == 0 || oldest == (streamID{}) {
		return 0, nil
	}

	var trimmed int64
	for range trimRounds {
		n, err := s.rdb.Do(ctx, s.rdb.B().Xtrim().Key(key).Minid().Almost().Threshold(oldest.String()).Build()).AsInt64()
		if err != nil {
			return trimmed, fmt.Errorf("trim work stream of %s: %w", tierName, err)
		}
		if n == 0 {
			break
		}
		trimmed += n
	}
	return trimmed, nil
}

// streamID is the ID of a stream entry.
type streamID struct{ ms, seq uint64 }

func parseStreamID(id string) (streamID, error) {
	ms, seq, _ := strings.Cut(id, "-")
	var parsed streamID
	var err error
	if parsed.ms, err = strconv.ParseUint(ms, 10, 64); err != nil {
		return streamID{}, fmt.Errorf("stream ID %q: %w", id, err)
	}
	if parsed.seq, err = strconv.ParseUint(seq, 10, 64); err != nil {
		return streamID{}, fmt.Errorf("stream ID %q: %w", id, err)
	}
	return parsed, nil
}

func (id streamID) before(other streamID) bool {
	return id.ms < other.ms || id.ms == other.ms && id.seq < other.seq
}

func (id streamID) String() string { return fmt.Sprintf("%d-%d", id.ms, id.seq) }

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
