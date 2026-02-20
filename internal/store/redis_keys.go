package store

import "fmt"

// Key prefix for all dureq Redis keys.
const defaultPrefix = "dureq"

// Key patterns for all Redis data structures.
// Jobs
func JobKey(prefix, id string) string       { return fmt.Sprintf("%s:job:%s", prefix, id) }
func JobsByCreatedKey(prefix string) string { return prefix + ":jobs:by_created" }
func JobsByStatusKey(prefix, status string) string {
	return fmt.Sprintf("%s:jobs:by_status:%s", prefix, status)
}
func JobsByTaskTypeKey(prefix, taskType string) string {
	return fmt.Sprintf("%s:jobs:by_task_type:%s", prefix, taskType)
}
func JobsByTagKey(prefix, tag string) string { return fmt.Sprintf("%s:jobs:by_tag:%s", prefix, tag) }
func JobsByWorkflowKey(prefix, wfID string) string {
	return fmt.Sprintf("%s:jobs:by_workflow:%s", prefix, wfID)
}
func JobsByBatchKey(prefix, batchID string) string {
	return fmt.Sprintf("%s:jobs:by_batch:%s", prefix, batchID)
}
func UniqueKeyKey(prefix, uniqueKey string) string {
	return fmt.Sprintf("%s:unique:%s", prefix, uniqueKey)
}

// Schedules
func ScheduleKey(prefix, jobID string) string { return fmt.Sprintf("%s:schedule:%s", prefix, jobID) }
func SchedulesDueKey(prefix string) string    { return prefix + ":schedules:due" }
func SchedulesByNextKey(prefix string) string { return prefix + ":schedules:by_next" }

// Runs
func RunKey(prefix, runID string) string { return fmt.Sprintf("%s:run:%s", prefix, runID) }
func RunsActiveKey(prefix string) string { return prefix + ":runs:active" }
func RunsByJobKey(prefix, jobID string) string {
	return fmt.Sprintf("%s:runs:by_job:%s", prefix, jobID)
}
func RunsActiveByJobKey(prefix, jobID string) string {
	return fmt.Sprintf("%s:runs:active_by_job:%s", prefix, jobID)
}

// Overlap buffer (schedule overlap policy)
func OverlapBufferedKey(prefix, jobID string) string {
	return fmt.Sprintf("%s:overlap_buffered:%s", prefix, jobID)
}
func OverlapBufferListKey(prefix, jobID string) string {
	return fmt.Sprintf("%s:overlap_buffer_list:%s", prefix, jobID)
}

// Nodes
func NodeKey(prefix, nodeID string) string { return fmt.Sprintf("%s:node:%s", prefix, nodeID) }
func NodesAllKey(prefix string) string     { return prefix + ":nodes:all" }

// Locks
func LockKey(prefix, key string) string { return fmt.Sprintf("%s:lock:%s", prefix, key) }

// Election
func ElectionLeaderKey(prefix string) string { return prefix + ":election:leader" }

// Workflows
func WorkflowKey(prefix, id string) string       { return fmt.Sprintf("%s:workflow:%s", prefix, id) }
func WorkflowsByCreatedKey(prefix string) string { return prefix + ":workflows:by_created" }
func WorkflowsByStatusKey(prefix, status string) string {
	return fmt.Sprintf("%s:workflows:by_status:%s", prefix, status)
}

// Batches
func BatchKey(prefix, id string) string        { return fmt.Sprintf("%s:batch:%s", prefix, id) }
func BatchesByCreatedKey(prefix string) string { return prefix + ":batches:by_created" }
func BatchesByStatusKey(prefix, status string) string {
	return fmt.Sprintf("%s:batches:by_status:%s", prefix, status)
}

// Batch Results
func BatchResultKey(prefix, batchID, itemID string) string {
	return fmt.Sprintf("%s:batch_result:%s:%s", prefix, batchID, itemID)
}
func BatchResultsSetKey(prefix, batchID string) string {
	return fmt.Sprintf("%s:batch_results:%s", prefix, batchID)
}

// Work Streams (per tier)
// Hash-tagged keys: {tierName} ensures delayed + work + paused co-locate
// to the same Redis Cluster slot, enabling multi-key Lua scripts.
func WorkStreamKey(prefix, tierName string) string {
	return fmt.Sprintf("%s:{%s}:work", prefix, tierName)
}
func TiersKey(prefix string) string { return prefix + ":tiers" }
func DelayedKey(prefix, tierName string) string {
	return fmt.Sprintf("%s:{%s}:delayed", prefix, tierName)
}

// DLQ
func DLQStreamKey(prefix string) string { return prefix + ":dlq" }

// Events
func EventsPubSubChannel(prefix string) string { return prefix + ":events" }
func EventsStreamKey(prefix string) string     { return prefix + ":events:stream" }
func EventsBatchChannel(prefix, batchID string) string {
	return fmt.Sprintf("%s:events:batch:%s", prefix, batchID)
}

// Results
func ResultKey(prefix, jobID string) string { return fmt.Sprintf("%s:result:%s", prefix, jobID) }
func ResultNotifyChannel(prefix, jobID string) string {
	return fmt.Sprintf("%s:result:notify:%s", prefix, jobID)
}

// History
func HistoryRunsKey(prefix string) string { return prefix + ":history:runs" }
func HistoryRunKey(prefix, runID string) string {
	return fmt.Sprintf("%s:history:run:%s", prefix, runID)
}
func HistoryRunsByJobKey(prefix, jobID string) string {
	return fmt.Sprintf("%s:history:runs:by_job:%s", prefix, jobID)
}
func HistoryRunsByStatusKey(prefix, status string) string {
	return fmt.Sprintf("%s:history:runs:by_status:%s", prefix, status)
}
func HistoryEventsKey(prefix string) string { return prefix + ":history:events" }
func HistoryEventKey(prefix, eventID string) string {
	return fmt.Sprintf("%s:history:event:%s", prefix, eventID)
}
func HistoryEventsByJobKey(prefix, jobID string) string {
	return fmt.Sprintf("%s:history:events:by_job:%s", prefix, jobID)
}
func HistoryEventsByTypeKey(prefix, eventType string) string {
	return fmt.Sprintf("%s:history:events:by_type:%s", prefix, eventType)
}

// Consumer group names
const ConsumerGroup = "dureq_workers"
const OrchestratorConsumerGroup = "dureq_orchestrator"

// Deduplication
func DedupKey(prefix, runID string) string { return fmt.Sprintf("%s:dedup:%s", prefix, runID) }

// Queue Pause (hash-tagged with tier for cluster slot co-location)
func QueuePausedKey(prefix, tierName string) string {
	return fmt.Sprintf("%s:{%s}:paused", prefix, tierName)
}

// Daily Stats
func DailyStatsKey(prefix, date string) string { return fmt.Sprintf("%s:stats:daily:%s", prefix, date) }

// Job Notification Pub/Sub (dureq actor dispatch)
func JobNotifyChannel(prefix string) string { return prefix + ":job:notify" }

// Task Cancellation Pub/Sub
func CancelChannelKey(prefix string) string { return prefix + ":cancel" }

// JSON Payload mirrors (RedisJSON) — used for JSONPath-based search.
func JobPayloadKey(prefix, id string) string { return fmt.Sprintf("%s:jparam:job:%s", prefix, id) }
func WorkflowPayloadKey(prefix, id string) string {
	return fmt.Sprintf("%s:jparam:workflow:%s", prefix, id)
}
func BatchPayloadKey(prefix, id string) string { return fmt.Sprintf("%s:jparam:batch:%s", prefix, id) }
