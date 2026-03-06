package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
	"github.com/bytedance/sonic"
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
func (s *RedisStore) DispatchWork(ctx context.Context, tierName string, wm *types.WorkMessage) (string, error) {
	streamKey := WorkStreamKey(s.prefix, tierName)

	// Use run ID as dedup key (prevents double-dispatch).
	dedupKey := DedupKey(s.prefix, wm.RunID)
	err := s.rdb.Do(ctx, s.rdb.B().Set().Key(dedupKey).Value("1").Nx().Ex(s.cfg.DedupTTL).Build()).Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			// Already dispatched — idempotent success.
			return "", nil
		}
		return "", fmt.Errorf("dedup check: %w", err)
	}

	fv := s.rdb.B().Xadd().Key(streamKey).Id("*").FieldValue().
		FieldValue("run_id", wm.RunID).
		FieldValue("job_id", wm.JobID).
		FieldValue("task_type", string(wm.TaskType)).
		FieldValue("payload", string(wm.Payload)).
		FieldValue("attempt", strconv.Itoa(wm.Attempt)).
		FieldValue("deadline", wm.Deadline.Format(time.RFC3339Nano)).
		FieldValue("priority", strconv.Itoa(int(wm.Priority))).
		FieldValue("dispatched_at", wm.DispatchedAt.Format(time.RFC3339Nano)).
		FieldValue("tier", tierName)

	if len(wm.Headers) > 0 {
		if hdr, err := sonic.ConfigFastest.Marshal(wm.Headers); err == nil {
			fv = fv.FieldValue("headers", string(hdr))
		}
	}

	msgID, err := s.rdb.Do(ctx, fv.Build()).ToString()
	if err != nil {
		// Rollback dedup key on failure.
		s.rdb.Do(ctx, s.rdb.B().Del().Key(dedupKey).Build())
		return "", fmt.Errorf("XADD work: %w", err)
	}
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

	cmds := make(rueidis.Commands, 0, 4)

	// Pub/Sub for real-time subscribers (orchestrator, clients).
	cmds = append(cmds, s.rdb.B().Publish().Channel(EventsPubSubChannel(s.prefix)).Message(string(data)).Build())

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

	// Batch-specific channel for batch progress subscribers.
	if event.BatchProgress != nil {
		cmds = append(cmds, s.rdb.B().Publish().Channel(EventsBatchChannel(s.prefix, event.BatchProgress.BatchID)).Message(string(data)).Build())
	}

	for _, resp := range s.rdb.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return err
		}
	}
	return nil
}

// ============================================================
// Job Notification (dureq actor dispatch trigger)
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

	cmds := make(rueidis.Commands, 0, 3)

	// Store result as hash with TTL.
	cmds = append(cmds, s.rdb.B().Hset().Key(resultKey).FieldValue().FieldValue("data", string(data)).Build())
	cmds = append(cmds, s.rdb.B().Expire().Key(resultKey).Seconds(int64(s.cfg.ResultTTL.Seconds())).Build())

	// Notify via Pub/Sub for EnqueueAndWait clients.
	cmds = append(cmds, s.rdb.B().Publish().Channel(ResultNotifyChannel(s.prefix, result.JobID)).Message(string(data)).Build())

	for _, resp := range s.rdb.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return err
		}
	}
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
}

// CompleteRun performs SaveRun + SaveJobRun + DeleteRun + IncrDailyStat +
// PublishEvent + PublishResult + AckMessage in a single Redis pipeline.
func (s *RedisStore) CompleteRun(ctx context.Context, batch *CompletionBatch) error {
	runData, err := sonic.ConfigFastest.Marshal(batch.Run)
	if err != nil {
		return fmt.Errorf("marshal run: %w", err)
	}
	eventData, err := sonic.ConfigFastest.Marshal(batch.Event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	resultData, err := sonic.ConfigFastest.Marshal(batch.Result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	cmds := make(rueidis.Commands, 0, 28)

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

	// 4. IncrDailyStat
	date := time.Now().Format("2006-01-02")
	statsKey := DailyStatsKey(s.prefix, date)
	cmds = append(cmds, s.rdb.B().Hincrby().Key(statsKey).Field(batch.DailyStatField).Increment(1).Build())
	cmds = append(cmds, s.rdb.B().Expire().Key(statsKey).Seconds(int64((91 * 24 * time.Hour).Seconds())).Build())

	// 5. PublishEvent — Pub/Sub + event stream
	cmds = append(cmds, s.rdb.B().Publish().Channel(EventsPubSubChannel(s.prefix)).Message(string(eventData)).Build())
	cmds = append(cmds, s.rdb.B().Xadd().Key(EventsStreamKey(s.prefix)).
		Maxlen().Almost().Threshold(strconv.FormatInt(s.cfg.EventStreamMaxLen, 10)).
		Id("*").FieldValue().
		FieldValue("type", string(batch.Event.Type)).
		FieldValue("job_id", batch.Event.JobID).
		FieldValue("run_id", batch.Event.RunID).
		FieldValue("node_id", batch.Event.NodeID).
		FieldValue("data", string(eventData)).
		Build())
	if batch.Event.BatchProgress != nil {
		cmds = append(cmds, s.rdb.B().Publish().Channel(EventsBatchChannel(s.prefix, batch.Event.BatchProgress.BatchID)).Message(string(eventData)).Build())
	}

	// 6. PublishResult — hash + TTL + notify
	resultKey := ResultKey(s.prefix, batch.Result.JobID)
	cmds = append(cmds, s.rdb.B().Hset().Key(resultKey).FieldValue().FieldValue("data", string(resultData)).Build())
	cmds = append(cmds, s.rdb.B().Expire().Key(resultKey).Seconds(int64(s.cfg.ResultTTL.Seconds())).Build())
	cmds = append(cmds, s.rdb.B().Publish().Channel(ResultNotifyChannel(s.prefix, batch.Result.JobID)).Message(string(resultData)).Build())

	// 7. AckMessage (v1 only — stream message acknowledgment)
	if batch.AckTierName != "" && batch.AckMessageID != "" {
		cmds = append(cmds, s.rdb.B().Xack().Key(WorkStreamKey(s.prefix, batch.AckTierName)).Group(ConsumerGroup).Id(batch.AckMessageID).Build())
	}

	for _, resp := range s.rdb.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return fmt.Errorf("complete run pipeline: %w", err)
		}
	}
	return nil
}

// ============================================================
// Delayed retry (sorted set + Lua move)
// ============================================================

// AddDelayed adds a work message to the delayed sorted set for future re-dispatch.
func (s *RedisStore) AddDelayed(ctx context.Context, tierName string, wm *types.WorkMessage, executeAt time.Time) error {
	data, err := sonic.ConfigFastest.Marshal(map[string]interface{}{
		"run_id":        wm.RunID,
		"job_id":        wm.JobID,
		"task_type":     string(wm.TaskType),
		"payload":       string(wm.Payload),
		"attempt":       wm.Attempt,
		"deadline":      wm.Deadline.Format(time.RFC3339Nano),
		"priority":      int(wm.Priority),
		"dispatched_at": wm.DispatchedAt.Format(time.RFC3339Nano),
		"tier":          tierName,
	})
	if err != nil {
		return err
	}

	return s.rdb.Do(ctx, s.rdb.B().Zadd().Key(DelayedKey(s.prefix, tierName)).ScoreMember().ScoreMember(float64(executeAt.UnixNano()), string(data)).Build()).Error()
}

// ReenqueueWork re-adds a work message to the stream without dedup.
// Used when a worker picks up a message for a task type it doesn't handle.
func (s *RedisStore) ReenqueueWork(ctx context.Context, tierName string, wm *types.WorkMessage, redeliveries int) error {
	_, err := s.rdb.Do(ctx, s.rdb.B().Xadd().Key(WorkStreamKey(s.prefix, tierName)).Id("*").FieldValue().
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
		Build()).ToString()
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
