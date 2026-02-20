package messages

import (
	"encoding/json"
	"time"

	pb "github.com/FDK0901/dureq/gen/dureq"
	"github.com/FDK0901/dureq/pkg/types"
)

// JobToDispatchMsg converts a domain Job to a DispatchJobMsg for sending
// from the SchedulerActor/NotifierActor to the DispatcherActor.
func JobToDispatchMsg(job *types.Job, attempt int) *pb.DispatchJobMsg {
	msg := &pb.DispatchJobMsg{
		JobId:              job.ID,
		TaskType:           string(job.TaskType),
		Payload:            []byte(job.Payload),
		Attempt:            int32(attempt),
		DispatchedAtUnixMs: time.Now().UnixMilli(),
	}

	if job.Priority != nil {
		msg.Priority = int32(*job.Priority)
	}

	if job.Headers != nil {
		msg.Headers = job.Headers
	}

	if job.Schedule.EndsAt != nil {
		msg.DeadlineUnixMs = job.Schedule.EndsAt.UnixMilli()
	}

	if job.WorkflowID != nil {
		msg.WorkflowId = *job.WorkflowID
	}
	if job.WorkflowTask != nil {
		msg.WorkflowTask = *job.WorkflowTask
	}

	if job.BatchID != nil {
		msg.BatchId = *job.BatchID
	}
	if job.BatchItem != nil {
		msg.BatchItem = *job.BatchItem
	}
	if job.BatchRole != nil {
		msg.BatchRole = *job.BatchRole
	}

	if job.ScheduleToStartTimeout != nil {
		msg.ScheduleToStartTimeoutMs = int64(job.ScheduleToStartTimeout.Std() / time.Millisecond)
	}

	return msg
}

// DispatchToExecuteMsg converts a DispatchJobMsg to an ExecuteJobMsg,
// adding the assigned runID for execution tracking.
func DispatchToExecuteMsg(msg *pb.DispatchJobMsg, runID string) *pb.ExecuteJobMsg {
	return &pb.ExecuteJobMsg{
		RunId:                    runID,
		JobId:                    msg.JobId,
		TaskType:                 msg.TaskType,
		Payload:                  msg.Payload,
		Attempt:                  msg.Attempt,
		Priority:                 msg.Priority,
		Headers:                  msg.Headers,
		DeadlineUnixMs:           msg.DeadlineUnixMs,
		DispatchedAtUnixMs:       msg.DispatchedAtUnixMs,
		WorkflowId:               msg.WorkflowId,
		WorkflowTask:             msg.WorkflowTask,
		BatchId:                  msg.BatchId,
		BatchItem:                msg.BatchItem,
		BatchRole:                msg.BatchRole,
		ScheduleToStartTimeoutMs: msg.ScheduleToStartTimeoutMs,
	}
}

// WorkerDoneToJobResult converts a local WorkerDoneMsg to a proto JobResultMsg
// for sending across node boundaries.
func WorkerDoneToJobResult(msg *WorkerDoneMsg, nodeID string) *pb.JobResultMsg {
	return &pb.JobResultMsg{
		RunId:            msg.RunID,
		JobId:            msg.JobID,
		TaskType:         msg.TaskType,
		Success:          msg.Success,
		Error:            msg.Error,
		Output:           []byte(msg.Output),
		Attempt:          int32(msg.Attempt),
		NodeId:           nodeID,
		StartedAtUnixMs:  msg.StartedAt.UnixMilli(),
		FinishedAtUnixMs: msg.FinishedAt.UnixMilli(),
		WorkflowId:       msg.WorkflowID,
		BatchId:          msg.BatchID,
	}
}

// JobEventToDomainEvent converts a domain JobEvent to a proto DomainEventMsg.
func JobEventToDomainEvent(event types.JobEvent) *pb.DomainEventMsg {
	msg := &pb.DomainEventMsg{
		Type:            string(event.Type),
		JobId:           event.JobID,
		RunId:           event.RunID,
		NodeId:          event.NodeID,
		TaskType:        string(event.TaskType),
		Attempt:         int32(event.Attempt),
		TimestampUnixMs: event.Timestamp.UnixMilli(),
	}

	if event.Error != nil {
		msg.Error = *event.Error
	}

	if event.BatchProgress != nil {
		msg.BatchId = event.BatchProgress.BatchID
		msg.BatchDone = int32(event.BatchProgress.Done)
		msg.BatchTotal = int32(event.BatchProgress.Total)
		msg.BatchFailed = int32(event.BatchProgress.Failed)
	}

	return msg
}

// DomainEventToJobEvent converts a proto DomainEventMsg back to a domain JobEvent.
func DomainEventToJobEvent(msg *pb.DomainEventMsg) types.JobEvent {
	event := types.JobEvent{
		Type:      types.EventType(msg.Type),
		JobID:     msg.JobId,
		RunID:     msg.RunId,
		NodeID:    msg.NodeId,
		TaskType:  types.TaskType(msg.TaskType),
		Attempt:   int(msg.Attempt),
		Timestamp: time.UnixMilli(msg.TimestampUnixMs),
	}

	if msg.Error != "" {
		errStr := msg.Error
		event.Error = &errStr
	}

	if msg.BatchId != "" {
		event.BatchProgress = &types.BatchProgress{
			BatchID: msg.BatchId,
			Done:    int(msg.BatchDone),
			Total:   int(msg.BatchTotal),
			Failed:  int(msg.BatchFailed),
		}
	}

	return event
}

// ExecuteMsgToWorkContext extracts execution context fields from a proto ExecuteJobMsg.
func ExecuteMsgToWorkContext(msg *pb.ExecuteJobMsg) (jobID, runID, taskType string, payload json.RawMessage, attempt int, priority int, headers map[string]string, deadline time.Time) {
	jobID = msg.JobId
	runID = msg.RunId
	taskType = msg.TaskType
	payload = json.RawMessage(msg.Payload)
	attempt = int(msg.Attempt)
	priority = int(msg.Priority)
	headers = msg.Headers
	if msg.DeadlineUnixMs > 0 {
		deadline = time.UnixMilli(msg.DeadlineUnixMs)
	}
	return
}
