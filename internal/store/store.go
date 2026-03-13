package store

import (
	"context"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

// Store defines the core persistence interface for dureq.
// RedisStore is the primary implementation; this interface enables
// testing with in-memory stores and future backend alternatives.
type Store interface {
	// --- Job CRUD ---
	SaveJob(ctx context.Context, job *types.Job) (uint64, error)
	CreateJob(ctx context.Context, job *types.Job) (uint64, error)
	GetJob(ctx context.Context, jobID string) (*types.Job, uint64, error)
	UpdateJob(ctx context.Context, job *types.Job, expectedRev uint64) (uint64, error)
	DeleteJob(ctx context.Context, jobID string) error
	ListJobs(ctx context.Context) ([]*types.Job, error)
	ListJobsByStatus(ctx context.Context, status types.JobStatus) ([]*types.Job, error)
	ListPausedJobsDueForResume(ctx context.Context, now time.Time) ([]*types.Job, error)
	GetJobRevision(ctx context.Context, jobID string) (uint64, error)

	// --- Schedule ---
	SaveSchedule(ctx context.Context, entry *types.ScheduleEntry) (uint64, error)
	GetSchedule(ctx context.Context, jobID string) (*types.ScheduleEntry, uint64, error)
	DeleteSchedule(ctx context.Context, jobID string) error
	ListDueSchedules(ctx context.Context, cutoff time.Time) ([]*types.ScheduleEntry, error)

	// --- Run ---
	SaveRun(ctx context.Context, run *types.JobRun) (uint64, error)
	GetRun(ctx context.Context, runID string) (*types.JobRun, uint64, error)
	DeleteRun(ctx context.Context, runID string) error
	ListRuns(ctx context.Context) ([]*types.JobRun, error)
	ListActiveRunsByJobID(ctx context.Context, jobID string) ([]*types.JobRun, error)
	HasActiveRunForJob(ctx context.Context, jobID string) (bool, error)
	SaveJobRun(ctx context.Context, run *types.JobRun) error

	// --- Workflow ---
	SaveWorkflow(ctx context.Context, wf *types.WorkflowInstance) (uint64, error)
	GetWorkflow(ctx context.Context, id string) (*types.WorkflowInstance, uint64, error)
	UpdateWorkflow(ctx context.Context, wf *types.WorkflowInstance, expectedRev uint64) (uint64, error)

	// --- Batch ---
	SaveBatch(ctx context.Context, batch *types.BatchInstance) (uint64, error)
	GetBatch(ctx context.Context, batchID string) (*types.BatchInstance, uint64, error)
	UpdateBatch(ctx context.Context, batch *types.BatchInstance, expectedRev uint64) (uint64, error)

	// --- Node ---
	SaveNode(ctx context.Context, node *types.NodeInfo) (uint64, error)
	ListNodes(ctx context.Context) ([]*types.NodeInfo, error)

	// --- Events ---
	PublishEvent(ctx context.Context, event types.JobEvent) error
	PublishResult(ctx context.Context, result types.WorkResult) error
	GetResult(ctx context.Context, jobID string) (*types.WorkResult, error)

	// --- Work dispatch ---
	DispatchWork(ctx context.Context, tierName string, msg *types.WorkMessage) (string, error)
	AckMessage(ctx context.Context, tierName string, msgID string) error
	ReenqueueWork(ctx context.Context, tierName string, msg *types.WorkMessage, redeliveries int) error
	AddDelayed(ctx context.Context, tierName string, msg *types.WorkMessage, executeAt time.Time) error

	// --- Overlap ---
	SaveOverlapBuffer(ctx context.Context, jobID string, nextRunAt time.Time) error
	PopOverlapBuffer(ctx context.Context, jobID string) (time.Time, bool, error)
	PushOverlapBufferAll(ctx context.Context, jobID string, nextRunAt time.Time) error
	PopOverlapBufferAll(ctx context.Context, jobID string) (time.Time, bool, error)

	// --- Unique keys ---
	CheckUniqueKey(ctx context.Context, uniqueKey string) (string, bool, error)
	SetUniqueKey(ctx context.Context, uniqueKey, jobID string) error
	DeleteUniqueKey(ctx context.Context, uniqueKey string) error

	// --- Stats ---
	IncrDailyStat(ctx context.Context, field string)

	// --- GDPR ---
	PurgeJobData(ctx context.Context, jobID string) error
}

// Compile-time check that RedisStore implements Store.
var _ Store = (*RedisStore)(nil)

// Compile-time check that RedisStore implements SideEffectStore.
var _ types.SideEffectStore = (*RedisStore)(nil)
