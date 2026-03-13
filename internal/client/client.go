package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/FDK0901/dureq/internal/scheduler"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/internal/workflow"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/bytedance/sonic"
	"github.com/redis/rueidis"
	"github.com/rs/xid"
)

// Client is the dureq client SDK for enqueuing jobs and querying status.
// It connects directly to Redis — no server-side request/reply needed.
type Client struct {
	store *store.RedisStore
	rdb   rueidis.Client
	owned bool // true if we created the redis client
}

// Option configures the client.
type Option func(*clientConfig)

type clientConfig struct {
	redisURL      string
	redisUsername string
	redisPassword string
	redisDB       int
	redisPoolSize int
	tlsConfig     interface{} // *tls.Config
	keyPrefix     string
	tiers         []store.TierConfig
	rdb           rueidis.Client    // pre-existing client
	clusterAddrs  []string          // Redis Cluster addresses
	redisStore    *store.RedisStore // pre-existing store
}

// WithRedisURL sets the Redis server URL.
func WithRedisURL(url string) Option {
	return func(c *clientConfig) { c.redisURL = url }
}

// WithRedisUsername sets the Redis ACL username (Redis 6+).
func WithRedisUsername(username string) Option {
	return func(c *clientConfig) { c.redisUsername = username }
}

// WithRedisPassword sets the Redis password.
func WithRedisPassword(password string) Option {
	return func(c *clientConfig) { c.redisPassword = password }
}

// WithRedisDB sets the Redis database number.
func WithRedisDB(db int) Option {
	return func(c *clientConfig) { c.redisDB = db }
}

// WithRedisPoolSize sets the Redis connection pool size.
func WithRedisPoolSize(n int) Option {
	return func(c *clientConfig) { c.redisPoolSize = n }
}

// WithKeyPrefix sets the key prefix for multi-tenant isolation.
func WithKeyPrefix(prefix string) Option {
	return func(c *clientConfig) { c.keyPrefix = prefix }
}

// WithPriorityTiers sets the priority tier configuration.
func WithPriorityTiers(tiers []store.TierConfig) Option {
	return func(c *clientConfig) { c.tiers = tiers }
}

// WithRedisClient uses a pre-existing rueidis client.
func WithRedisClient(rdb rueidis.Client) Option {
	return func(c *clientConfig) { c.rdb = rdb }
}

// WithClusterAddrs sets Redis Cluster node addresses for cluster mode.
func WithClusterAddrs(addrs []string) Option {
	return func(c *clientConfig) { c.clusterAddrs = addrs }
}

// WithStore uses a pre-existing RedisStore (for in-process use with the server).
func WithStore(s *store.RedisStore) Option {
	return func(c *clientConfig) { c.redisStore = s }
}

// New creates a new dureq client.
func New(opts ...Option) (*Client, error) {
	cfg := &clientConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	// If a pre-existing store is provided, use it directly.
	if cfg.redisStore != nil {
		return &Client{
			store: cfg.redisStore,
			rdb:   cfg.redisStore.Client(),
			owned: false,
		}, nil
	}

	// Create Redis client.
	var rdb rueidis.Client
	owned := false
	if cfg.rdb != nil {
		rdb = cfg.rdb
	} else if len(cfg.clusterAddrs) > 0 {
		opt := rueidis.ClientOption{
			InitAddress:      cfg.clusterAddrs,
			Username:         cfg.redisUsername,
			Password:         cfg.redisPassword,
			BlockingPoolSize: max(cfg.redisPoolSize, 10),
		}
		var err error
		rdb, err = rueidis.NewClient(opt)
		if err != nil {
			return nil, fmt.Errorf("redis cluster connect: %w", err)
		}
		owned = true
	} else if cfg.redisURL != "" {
		u, err := url.Parse(cfg.redisURL)
		if err != nil {
			return nil, fmt.Errorf("parse redis URL: %w", err)
		}
		addr := u.Host
		if addr == "" {
			addr = "localhost:6379"
		}
		opt := rueidis.ClientOption{
			InitAddress:      []string{addr},
			SelectDB:         cfg.redisDB,
			BlockingPoolSize: max(cfg.redisPoolSize, 10),
		}
		if cfg.redisUsername != "" {
			opt.Username = cfg.redisUsername
		} else if u.User != nil {
			opt.Username = u.User.Username()
		}
		if cfg.redisPassword != "" {
			opt.Password = cfg.redisPassword
		} else if u.User != nil {
			if p, ok := u.User.Password(); ok {
				opt.Password = p
			}
		}
		if u.Scheme == "rediss" {
			opt.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		rdb, err = rueidis.NewClient(opt)
		if err != nil {
			return nil, fmt.Errorf("redis connect: %w", err)
		}
		owned = true
	} else {
		return nil, fmt.Errorf("Redis URL, Redis client, or RedisStore is required")
	}

	// Ping to verify connectivity.
	if err := rdb.Do(context.Background(), rdb.B().Ping().Build()).Error(); err != nil {
		if owned {
			rdb.Close()
		}
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	// Create store.
	storeCfg := store.RedisStoreConfig{
		KeyPrefix: cfg.keyPrefix,
		Tiers:     cfg.tiers,
	}

	s, err := store.NewRedisStore(rdb, storeCfg, nil)
	if err != nil {
		if owned {
			rdb.Close()
		}
		return nil, fmt.Errorf("create store: %w", err)
	}

	return &Client{
		store: s,
		rdb:   rdb,
		owned: owned,
	}, nil
}

// Store returns the underlying RedisStore.
func (c *Client) Store() *store.RedisStore { return c.store }

// --- Job operations ---

// EnqueueRequest describes a job to enqueue.
type EnqueueRequest struct {
	TaskType         types.TaskType     `json:"task_type"`
	Payload          json.RawMessage    `json:"payload"`
	Schedule         types.Schedule     `json:"schedule"`
	RetryPolicy      *types.RetryPolicy `json:"retry_policy,omitempty"`
	Tags             []string           `json:"tags,omitempty"`
	UniqueKey        *string            `json:"unique_key,omitempty"`
	DLQAfter         *int               `json:"dlq_after,omitempty"`
	Priority         *types.Priority    `json:"priority,omitempty"`
	HeartbeatTimeout *types.Duration    `json:"heartbeat_timeout,omitempty"`
	// RequestID enables idempotent enqueue: if the same RequestID is sent again
	// within 5 minutes, the cached response is returned instead of creating a duplicate job.
	RequestID *string `json:"request_id,omitempty"`
}

// Enqueue submits a job for immediate execution.
func (c *Client) Enqueue(ctx context.Context, taskType types.TaskType, payload any) (*types.Job, error) {
	payloadBytes, err := sonic.ConfigFastest.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	return c.enqueue(ctx, &EnqueueRequest{
		TaskType: taskType,
		Payload:  payloadBytes,
		Schedule: types.Schedule{Type: types.ScheduleImmediate},
	})
}

// EnqueueScheduled submits a job with a schedule.
func (c *Client) EnqueueScheduled(ctx context.Context, req *EnqueueRequest) (*types.Job, error) {
	return c.enqueue(ctx, req)
}

// EnqueueGroup adds a task to a named group for aggregated processing.
// The group is flushed automatically by the server's aggregation processor
// based on the configured GracePeriod, MaxDelay, or MaxSize.
// Returns the current group size after adding the message.
func (c *Client) EnqueueGroup(ctx context.Context, opt types.EnqueueGroupOption) (int64, error) {
	msg := types.GroupMessage{
		JobID:    xid.New().String(),
		TaskType: opt.TaskType,
		Payload:  opt.Payload,
		AddedAt:  time.Now(),
	}
	return c.store.AddToGroup(ctx, opt.Group, msg)
}

func (c *Client) enqueue(ctx context.Context, req *EnqueueRequest) (*types.Job, error) {
	// Request ID deduplication: return cached job if the same request was already processed.
	if req.RequestID != nil && *req.RequestID != "" {
		dedupKey := store.RequestDedupKey(c.store.Prefix(), *req.RequestID)
		cachedJobID, err := c.rdb.Do(ctx, c.rdb.B().Get().Key(dedupKey).Build()).ToString()
		if err == nil && cachedJobID != "" {
			// Already processed — return the cached job.
			job, _, err := c.store.GetJob(ctx, cachedJobID)
			if err == nil {
				return job, nil
			}
		}
	}

	now := time.Now()
	job := &types.Job{
		ID:               xid.New().String(),
		TaskType:         req.TaskType,
		Payload:          req.Payload,
		Schedule:         req.Schedule,
		RetryPolicy:      req.RetryPolicy,
		Tags:             req.Tags,
		UniqueKey:        req.UniqueKey,
		DLQAfter:         req.DLQAfter,
		Priority:         req.Priority,
		HeartbeatTimeout: req.HeartbeatTimeout,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	// Handle unique key deduplication.
	if job.UniqueKey != nil && *job.UniqueKey != "" {
		if err := c.store.SetUniqueKey(ctx, *job.UniqueKey, job.ID); err != nil {
			return nil, fmt.Errorf("unique key: %w", err)
		}
	}

	// Determine initial status based on schedule type.
	if job.Schedule.Type == types.ScheduleImmediate {
		job.Status = types.JobStatusPending
	} else {
		job.Status = types.JobStatusScheduled
	}

	// Create the job in Redis.
	if _, err := c.store.CreateJob(ctx, job); err != nil {
		// Clean up unique key on failure.
		if job.UniqueKey != nil && *job.UniqueKey != "" {
			c.store.DeleteUniqueKey(ctx, *job.UniqueKey)
		}
		return nil, fmt.Errorf("create job: %w", err)
	}

	// Handle dispatch or scheduling.
	switch job.Schedule.Type {
	case types.ScheduleImmediate:
		if err := c.dispatchJob(ctx, job); err != nil {
			// Clean up orphaned job on dispatch failure.
			c.store.DeleteJob(ctx, job.ID)
			if job.UniqueKey != nil && *job.UniqueKey != "" {
				c.store.DeleteUniqueKey(ctx, *job.UniqueKey)
			}
			return nil, fmt.Errorf("dispatch: %w", err)
		}
		c.publishEvent(ctx, types.JobEvent{
			Type:      types.EventJobDispatched,
			JobID:     job.ID,
			TaskType:  job.TaskType,
			Timestamp: now,
		})

	default:
		// Cron, interval, one-time schedules.
		nextRun, err := scheduler.NextRunTime(job.Schedule, now)
		if err != nil {
			c.store.DeleteJob(ctx, job.ID)
			if job.UniqueKey != nil && *job.UniqueKey != "" {
				c.store.DeleteUniqueKey(ctx, *job.UniqueKey)
			}
			return nil, fmt.Errorf("calculate next run: %w", err)
		}
		entry := &types.ScheduleEntry{
			JobID:     job.ID,
			Schedule:  job.Schedule,
			NextRunAt: nextRun,
		}
		if _, err := c.store.SaveSchedule(ctx, entry); err != nil {
			c.store.DeleteJob(ctx, job.ID)
			if job.UniqueKey != nil && *job.UniqueKey != "" {
				c.store.DeleteUniqueKey(ctx, *job.UniqueKey)
			}
			return nil, fmt.Errorf("save schedule: %w", err)
		}
		c.publishEvent(ctx, types.JobEvent{
			Type:      types.EventScheduleCreated,
			JobID:     job.ID,
			TaskType:  job.TaskType,
			Timestamp: now,
		})
	}

	// Cache request ID → job ID for idempotent retries.
	if req.RequestID != nil && *req.RequestID != "" {
		dedupKey := store.RequestDedupKey(c.store.Prefix(), *req.RequestID)
		c.rdb.Do(ctx, c.rdb.B().Set().Key(dedupKey).Value(job.ID).Ex(c.store.Config().RequestDedupTTL).Build())
	}

	return job, nil
}

// Cancel cancels a job by ID.
func (c *Client) Cancel(ctx context.Context, jobID string) error {
	job, rev, err := c.store.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job: %w", err)
	}

	now := time.Now()
	job.Status = types.JobStatusCancelled
	job.UpdatedAt = now

	if _, err := c.store.UpdateJob(ctx, job, rev); err != nil {
		return fmt.Errorf("update job: %w", err)
	}

	c.store.DeleteSchedule(ctx, jobID)
	c.signalCancelActiveRuns(ctx, jobID)
	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventJobCancelled,
		JobID:     jobID,
		Timestamp: now,
	})
	return nil
}

// CancelByUniqueKey cancels a job identified by its unique key.
func (c *Client) CancelByUniqueKey(ctx context.Context, uniqueKey string) error {
	jobID, exists, err := c.store.CheckUniqueKey(ctx, uniqueKey)
	if err != nil {
		return fmt.Errorf("check unique key: %w", err)
	}
	if !exists {
		return nil // no job with this key — nothing to cancel
	}
	if err := c.Cancel(ctx, jobID); err != nil {
		return err
	}
	c.store.DeleteUniqueKey(ctx, uniqueKey)
	return nil
}

// Retry re-enqueues a failed or dead job for another attempt.
func (c *Client) Retry(ctx context.Context, jobID string) error {
	job, rev, err := c.store.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job: %w", err)
	}

	if !job.Status.IsRetryable() {
		return fmt.Errorf("job %s is %s, cannot retry (only failed/dead/cancelled)", jobID, job.Status)
	}

	now := time.Now()
	prevStatus := job.Status
	job.Status = types.JobStatusPending
	job.Attempt = 0
	job.LastError = nil
	job.UpdatedAt = now

	newRev, err := c.store.UpdateJob(ctx, job, rev)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}

	if err := c.dispatchJob(ctx, job); err != nil {
		// Rollback: restore previous status.
		job.Status = prevStatus
		job.UpdatedAt = time.Now()
		c.store.UpdateJob(ctx, job, newRev)
		return fmt.Errorf("dispatch: %w", err)
	}

	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventJobRetrying,
		JobID:     jobID,
		Timestamp: now,
	})
	return nil
}

// ResumeJob manually resumes a paused job. The job transitions back to retrying
// and is immediately dispatched for execution.
func (c *Client) ResumeJob(ctx context.Context, jobID string) error {
	job, rev, err := c.store.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job: %w", err)
	}

	if job.Status != types.JobStatusPaused {
		return fmt.Errorf("job %s is in state %s, not paused", jobID, job.Status)
	}

	now := time.Now()
	job.Status = types.JobStatusRetrying
	job.PausedAt = nil
	job.ResumeAt = nil
	job.ConsecutiveErrors = 0
	job.UpdatedAt = now

	newRev, err := c.store.UpdateJob(ctx, job, rev)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}

	if err := c.dispatchJob(ctx, job); err != nil {
		job.Status = types.JobStatusPaused
		job.UpdatedAt = time.Now()
		c.store.UpdateJob(ctx, job, newRev)
		return fmt.Errorf("dispatch: %w", err)
	}

	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventJobResumed,
		JobID:     jobID,
		Timestamp: now,
	})
	return nil
}

// Status queries the current status of a job.
func (c *Client) Status(ctx context.Context, jobID string) (*types.Job, error) {
	job, _, err := c.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// GetJob retrieves a job by ID.
func (c *Client) GetJob(ctx context.Context, jobID string) (*types.Job, error) {
	job, _, err := c.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// GetJobByParamJSONPath finds the first job whose Payload matches the given JSONPath + value.
// Uses RedisJSON's native JSONPath for efficient field extraction.
// path is a dot-separated field path (e.g. "user_id", "order.type").
func (c *Client) GetJobByParamJSONPath(ctx context.Context, path string, value any) (*types.Job, error) {
	return c.GetJobByParamJSONPathWithTaskType(ctx, path, value, "")
}

// GetJobByParamJSONPathWithTaskType finds the first job of the given task type whose Payload
// matches the given JSONPath + value. Only scans jobs of that task type, so it is
// significantly faster than GetJobByParamJSONPath when the task type is known.
func (c *Client) GetJobByParamJSONPathWithTaskType(ctx context.Context, path string, value any, taskType string) (*types.Job, error) {
	job, _, err := c.store.FindJobByPayloadPath(ctx, path, value, taskType)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// GetWorkflowByParamJSONPath finds the first workflow where any task's Payload
// matches the given JSONPath + value.
func (c *Client) GetWorkflowByParamJSONPath(ctx context.Context, path string, value any) (*types.WorkflowInstance, error) {
	wf, _, err := c.store.FindWorkflowByTaskPayloadPath(ctx, path, value)
	if err != nil {
		return nil, err
	}
	return wf, nil
}

// GetBatchByParamJSONPath finds the first batch where the onetime payload or any item
// payload matches the given JSONPath + value.
func (c *Client) GetBatchByParamJSONPath(ctx context.Context, path string, value any) (*types.BatchInstance, error) {
	batch, _, err := c.store.FindBatchByPayloadPath(ctx, path, value)
	if err != nil {
		return nil, err
	}
	return batch, nil
}

// --- Workflow operations ---

// EnqueueWorkflow submits a workflow definition for execution.
func (c *Client) EnqueueWorkflow(ctx context.Context, def types.WorkflowDefinition, input any) (*types.WorkflowInstance, error) {
	if err := workflow.ValidateDAG(&def); err != nil {
		return nil, err
	}

	now := time.Now()

	// Initialize task states.
	tasks := make(map[string]types.WorkflowTaskState, len(def.Tasks))
	for _, t := range def.Tasks {
		tasks[t.Name] = types.WorkflowTaskState{
			Status: types.JobStatusPending,
		}
	}

	wf := &types.WorkflowInstance{
		ID:           xid.New().String(),
		WorkflowName: def.Name,
		Definition:   def,
		Status:       types.WorkflowStatusRunning,
		Tasks:        tasks,
		Attempt:      1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	// Initialize reference counting for dependency resolution.
	workflow.InitPendingDeps(wf)

	// Set deadline if execution timeout is configured.
	if def.ExecutionTimeout != nil {
		deadline := now.Add(def.ExecutionTimeout.Std())
		wf.Deadline = &deadline
	}

	// Save workflow instance.
	rev, err := c.store.SaveWorkflow(ctx, wf)
	if err != nil {
		return nil, fmt.Errorf("save workflow: %w", err)
	}

	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventWorkflowStarted,
		JobID:     wf.ID,
		Timestamp: now,
	})

	// Dispatch root tasks (no dependencies).
	roots := rootTasks(&def)
	for _, taskName := range roots {
		c.dispatchWorkflowTask(ctx, wf, taskName, &now)
	}

	// Save again after dispatch updates task states.
	// Use CAS to avoid overwriting orchestrator state if a root task already completed.
	c.store.UpdateWorkflow(ctx, wf, rev)

	return wf, nil
}

// BackfillSchedule manually triggers all missed firings for a scheduled job
// within the given time range. Each firing is dispatched as a separate execution.
// Returns the number of firings dispatched.
func (c *Client) BackfillSchedule(ctx context.Context, jobID string, startTime, endTime time.Time) (int, error) {
	job, _, err := c.store.GetJob(ctx, jobID)
	if err != nil {
		return 0, fmt.Errorf("get job: %w", err)
	}

	firings, err := scheduler.MissedFirings(job.Schedule, startTime, endTime, 100)
	if err != nil {
		return 0, fmt.Errorf("calculate firings: %w", err)
	}

	dispatched := 0
	for range firings {
		if err := c.dispatchJob(ctx, job); err != nil {
			break
		}
		dispatched++
	}

	return dispatched, nil
}

// SignalWorkflow sends an asynchronous signal to a running workflow.
// The signal is appended to a per-workflow signal stream. User code
// reads signals via ReadSignals() + AckSignals() (at-least-once delivery);
// the deprecated ConsumeSignals() provides at-most-once delivery.
// The orchestrator does not consume signals directly.
//
// Returns ErrWorkflowTerminal if the workflow has already completed,
// failed, or been cancelled.
func (c *Client) SignalWorkflow(ctx context.Context, workflowID string, signalName string, payload any, opts ...types.SignalOption) error {
	// Terminal guard: reject signals to workflows that can no longer process them.
	wf, _, err := c.store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get workflow for signal: %w", err)
	}
	if wf.Status.IsTerminal() {
		return types.ErrWorkflowTerminal
	}

	var payloadBytes json.RawMessage
	if payload != nil {
		var err error
		payloadBytes, err = sonic.ConfigFastest.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal signal payload: %w", err)
		}
	}

	signal := &types.WorkflowSignal{
		Name:       signalName,
		Payload:    payloadBytes,
		SentAt:     time.Now(),
		WorkflowID: workflowID,
	}

	// Apply options.
	for _, opt := range opts {
		opt(signal)
	}

	if err := c.store.SendSignal(ctx, signal); err != nil {
		return fmt.Errorf("send signal: %w", err)
	}

	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventWorkflowSignalReceived,
		JobID:     workflowID,
		Timestamp: signal.SentAt,
	})

	return nil
}

// GetWorkflow retrieves the current state of a workflow instance.
func (c *Client) GetWorkflow(ctx context.Context, workflowID string) (*types.WorkflowInstance, error) {
	wf, _, err := c.store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	return wf, nil
}

// CancelWorkflow cancels a running workflow and all its non-terminal tasks.
func (c *Client) CancelWorkflow(ctx context.Context, workflowID string) error {
	const maxCASRetries = 5
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		wf, rev, err := c.store.GetWorkflow(ctx, workflowID)
		if err != nil {
			return err
		}

		now := time.Now()
		wf.Status = types.WorkflowStatusCancelled
		wf.CompletedAt = &now
		wf.UpdatedAt = now

		for name, state := range wf.Tasks {
			if !state.Status.IsTerminal() {
				state.Status = types.JobStatusCancelled
				state.FinishedAt = &now
				wf.Tasks[name] = state

				if state.JobID != "" {
					if childJob, childRev, err := c.store.GetJob(ctx, state.JobID); err == nil {
						if !childJob.Status.IsTerminal() {
							childJob.Status = types.JobStatusCancelled
							childJob.UpdatedAt = now
							c.store.UpdateJob(ctx, childJob, childRev)
						}
					}
					c.signalCancelActiveRuns(ctx, state.JobID)
				}
				// Recursively cancel child workflows spawned by this task.
				if state.ChildWorkflowID != "" {
					c.cancelChildWorkflow(ctx, state.ChildWorkflowID)
				}
			}
		}

		if _, err := c.store.UpdateWorkflow(ctx, wf, rev); err != nil {
			if attempt < maxCASRetries-1 {
				continue // CAS conflict — retry with fresh state
			}
			return err
		}

		c.publishEvent(ctx, types.JobEvent{
			Type:      types.EventWorkflowCancelled,
			JobID:     workflowID,
			Timestamp: now,
		})
		return nil
	}
	return fmt.Errorf("cancel workflow: max CAS retries exceeded")
}

// RetryWorkflow retries a failed workflow. Only failed/dead/cancelled tasks are
// reset — completed tasks are preserved so work resumes from the failure point.
func (c *Client) RetryWorkflow(ctx context.Context, workflowID string) (*types.WorkflowInstance, error) {
	wf, rev, err := c.store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	if !wf.Status.IsRetryable() {
		return nil, fmt.Errorf("cannot retry: workflow is %s (only failed/cancelled)", wf.Status)
	}

	now := time.Now()
	wf.Attempt++
	wf.Status = types.WorkflowStatusRunning
	wf.UpdatedAt = now
	wf.CompletedAt = nil

	// Reset only failed/dead/cancelled tasks — keep completed tasks.
	for name, state := range wf.Tasks {
		if state.Status == types.JobStatusFailed || state.Status == types.JobStatusDead || state.Status == types.JobStatusCancelled {
			state.Status = types.JobStatusPending
			state.JobID = ""
			state.Error = nil
			state.StartedAt = nil
			state.FinishedAt = nil
			wf.Tasks[name] = state
		}
	}

	newRev, err := c.store.UpdateWorkflow(ctx, wf, rev)
	if err != nil {
		return nil, err
	}

	// Dispatch tasks whose dependencies are all completed.
	ready := readyTasks(wf)
	for _, taskName := range ready {
		c.dispatchWorkflowTask(ctx, wf, taskName, &now)
	}
	// Use CAS to avoid overwriting orchestrator state.
	c.store.UpdateWorkflow(ctx, wf, newRev)

	return wf, nil
}

// --- Batch operations ---

// EnqueueBatch submits a batch definition for processing.
func (c *Client) EnqueueBatch(ctx context.Context, def types.BatchDefinition) (*types.BatchInstance, error) {
	now := time.Now()

	itemStates := make(map[string]types.BatchItemState, len(def.Items))
	for _, item := range def.Items {
		itemStates[item.ID] = types.BatchItemState{
			ItemID: item.ID,
			Status: types.JobStatusPending,
		}
	}

	batch := &types.BatchInstance{
		ID:           xid.New().String(),
		Name:         def.Name,
		Definition:   def,
		Status:       types.WorkflowStatusRunning,
		ItemStates:   itemStates,
		TotalItems:   len(def.Items),
		PendingItems: len(def.Items),
		Attempt:      1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	// Set deadline if execution timeout is configured.
	if def.ExecutionTimeout != nil {
		deadline := now.Add(def.ExecutionTimeout.Std())
		batch.Deadline = &deadline
	}

	// Save batch instance.
	batchRev, err := c.store.SaveBatch(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("save batch: %w", err)
	}

	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventBatchStarted,
		JobID:     batch.ID,
		Timestamp: now,
	})

	// If onetime task is defined, dispatch it first.
	if def.OnetimeTaskType != nil && *def.OnetimeTaskType != "" {
		c.dispatchBatchOnetime(ctx, batch, &now)
		c.store.UpdateBatch(ctx, batch, batchRev)
	} else {
		// Dispatch first chunk directly.
		c.dispatchBatchChunk(ctx, batch, &now)
		c.store.UpdateBatch(ctx, batch, batchRev)
	}

	return batch, nil
}

// GetBatch retrieves the current state of a batch instance.
func (c *Client) GetBatch(ctx context.Context, batchID string) (*types.BatchInstance, error) {
	batch, _, err := c.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	return batch, nil
}

// GetBatchResults retrieves all item results for a batch.
func (c *Client) GetBatchResults(ctx context.Context, batchID string) ([]*types.BatchItemResult, error) {
	return c.store.ListBatchItemResults(ctx, batchID)
}

// CancelBatch cancels a running batch.
func (c *Client) CancelBatch(ctx context.Context, batchID string) error {
	const maxCASRetries = 5
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		batch, rev, err := c.store.GetBatch(ctx, batchID)
		if err != nil {
			return err
		}

		now := time.Now()
		batch.Status = types.WorkflowStatusCancelled
		batch.CompletedAt = &now
		batch.UpdatedAt = now

		// Cancel in-flight child jobs.
		for itemID, state := range batch.ItemStates {
			if !state.Status.IsTerminal() {
				state.Status = types.JobStatusCancelled
				state.FinishedAt = &now
				batch.ItemStates[itemID] = state

				if state.JobID != "" {
					if childJob, childRev, err := c.store.GetJob(ctx, state.JobID); err == nil {
						if !childJob.Status.IsTerminal() {
							childJob.Status = types.JobStatusCancelled
							childJob.UpdatedAt = now
							c.store.UpdateJob(ctx, childJob, childRev)
						}
					}
					c.signalCancelActiveRuns(ctx, state.JobID)
				}
			}
		}
		// Cancel onetime task if still running.
		if batch.OnetimeState != nil && !batch.OnetimeState.Status.IsTerminal() {
			if batch.OnetimeState.JobID != "" {
				if childJob, childRev, err := c.store.GetJob(ctx, batch.OnetimeState.JobID); err == nil {
					if !childJob.Status.IsTerminal() {
						childJob.Status = types.JobStatusCancelled
						childJob.UpdatedAt = now
						c.store.UpdateJob(ctx, childJob, childRev)
					}
				}
				c.signalCancelActiveRuns(ctx, batch.OnetimeState.JobID)
			}
			batch.OnetimeState.Status = types.JobStatusCancelled
			batch.OnetimeState.FinishedAt = &now
		}

		if _, err := c.store.UpdateBatch(ctx, batch, rev); err != nil {
			if attempt < maxCASRetries-1 {
				continue // CAS conflict — retry with fresh state
			}
			return err
		}

		c.publishEvent(ctx, types.JobEvent{
			Type:      types.EventBatchCancelled,
			JobID:     batchID,
			Timestamp: now,
		})
		return nil
	}
	return fmt.Errorf("cancel batch: max CAS retries exceeded")
}

// RetryBatch retries a failed batch. If retryFailedOnly is true, only failed items are re-processed.
func (c *Client) RetryBatch(ctx context.Context, batchID string, retryFailedOnly bool) (*types.BatchInstance, error) {
	batch, rev, err := c.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if !batch.Status.IsRetryable() {
		return nil, fmt.Errorf("cannot retry: batch is %s (only failed/cancelled)", batch.Status)
	}

	now := time.Now()
	batch.Attempt++
	batch.Status = types.WorkflowStatusRunning
	batch.UpdatedAt = now
	batch.CompletedAt = nil

	batch.FailedItems = 0
	batch.RunningItems = 0
	batch.PendingItems = 0
	batch.CompletedItems = 0

	for id, state := range batch.ItemStates {
		if retryFailedOnly && state.Status != types.JobStatusFailed {
			if state.Status == types.JobStatusCompleted {
				batch.CompletedItems++
			}
			continue
		}
		state.Status = types.JobStatusPending
		state.JobID = ""
		state.Error = nil
		state.StartedAt = nil
		state.FinishedAt = nil
		batch.ItemStates[id] = state
		batch.PendingItems++
	}
	batch.NextChunkIndex = 0

	newBatchRev, err := c.store.UpdateBatch(ctx, batch, rev)
	if err != nil {
		return nil, err
	}

	// Re-dispatch: onetime first if needed, otherwise chunks.
	needsOnetime := batch.Definition.OnetimeTaskType != nil && *batch.Definition.OnetimeTaskType != "" &&
		(batch.OnetimeState == nil || batch.OnetimeState.Status != types.JobStatusCompleted)
	if needsOnetime {
		batch.OnetimeState = nil
		c.dispatchBatchOnetime(ctx, batch, &now)
	} else {
		c.dispatchBatchChunk(ctx, batch, &now)
	}
	c.store.UpdateBatch(ctx, batch, newBatchRev)

	return batch, nil
}

// SubscribeBatchProgress subscribes to batch progress events.
func (c *Client) SubscribeBatchProgress(ctx context.Context, batchID string) (<-chan types.BatchProgress, error) {
	ch := make(chan types.BatchProgress, 64)

	channel := store.EventsBatchChannel(c.store.Prefix(), batchID)
	subscribeCmd := c.rdb.B().Subscribe().Channel(channel).Build()

	go func() {
		defer close(ch)

		_ = c.rdb.Receive(ctx, subscribeCmd, func(msg rueidis.PubSubMessage) {
			var event types.JobEvent
			if err := sonic.ConfigFastest.Unmarshal([]byte(msg.Message), &event); err != nil {
				return
			}
			if event.BatchProgress == nil {
				return
			}
			select {
			case ch <- *event.BatchProgress:
			default:
			}
		})
	}()

	return ch, nil
}

// EnqueueAndWait submits a job for immediate execution and waits for the result.
// It subscribes to the result channel BEFORE enqueuing to prevent a race condition
// where a fast job completes before the subscription is active.
func (c *Client) EnqueueAndWait(ctx context.Context, taskType types.TaskType, payload any, timeout time.Duration) (*types.WorkResult, error) {
	// Pre-generate a job ID so we can subscribe before enqueuing.
	jobID := xid.New().String()

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	channel := store.ResultNotifyChannel(c.store.Prefix(), jobID)
	subscribeCmd := c.rdb.B().Subscribe().Channel(channel).Build()

	// 1. Start subscription BEFORE enqueue to close the race window.
	resultOut := make(chan *types.WorkResult, 1)
	errOut := make(chan error, 1)

	go func() {
		err := c.rdb.Receive(waitCtx, subscribeCmd, func(msg rueidis.PubSubMessage) {
			var result types.WorkResult
			if err := sonic.ConfigFastest.Unmarshal([]byte(msg.Message), &result); err != nil {
				return
			}
			if result.JobID == jobID {
				select {
				case resultOut <- &result:
				default:
				}
			}
		})
		if err != nil {
			select {
			case errOut <- err:
			default:
			}
		}
	}()

	// 2. Enqueue the job with the pre-generated ID.
	job, err := c.enqueueWithID(ctx, jobID, taskType, payload)
	if err != nil {
		return nil, err
	}

	// 3. Poll once after enqueue to catch results that arrived before
	//    the subscription became active.
	if result, err := c.store.GetResult(waitCtx, job.ID); err == nil && result != nil {
		return result, nil
	}

	select {
	case result := <-resultOut:
		return result, nil
	case err := <-errOut:
		return nil, fmt.Errorf("wait for result: %w", err)
	case <-waitCtx.Done():
		return nil, fmt.Errorf("wait for result: %w", waitCtx.Err())
	}
}

// enqueueWithID creates and dispatches an immediate job with a pre-generated ID.
func (c *Client) enqueueWithID(ctx context.Context, jobID string, taskType types.TaskType, payload any) (*types.Job, error) {
	payloadBytes, err := sonic.ConfigFastest.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	now := time.Now()
	job := &types.Job{
		ID:        jobID,
		TaskType:  taskType,
		Payload:   payloadBytes,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		Status:    types.JobStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if _, err := c.store.CreateJob(ctx, job); err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	if err := c.dispatchJob(ctx, job); err != nil {
		c.store.DeleteJob(ctx, job.ID)
		return nil, fmt.Errorf("dispatch: %w", err)
	}

	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventJobDispatched,
		JobID:     job.ID,
		TaskType:  job.TaskType,
		Timestamp: now,
	})

	return job, nil
}

// DrainNode puts a node into drain mode. The node stops fetching new messages
// but continues processing in-flight tasks. Call with drain=false to resume.
func (c *Client) DrainNode(ctx context.Context, nodeID string, drain bool) error {
	return c.store.SetNodeDrain(ctx, nodeID, drain)
}

// Close closes the client connection (only if we created the Redis client).
func (c *Client) Close() {
	if c.owned && c.rdb != nil {
		c.rdb.Close()
	}
}

// --- Internal helpers ---

func (c *Client) dispatchJob(ctx context.Context, job *types.Job) error {
	priority := types.PriorityNormal
	if job.Priority != nil {
		priority = *job.Priority
	}

	now := time.Now()
	msg := &types.WorkMessage{
		RunID:           xid.New().String(),
		JobID:           job.ID,
		TaskType:        job.TaskType,
		Payload:         job.Payload,
		Attempt:         job.Attempt,
		Priority:        priority,
		DispatchedAt:    now,
		ConcurrencyKeys: job.ConcurrencyKeys,
	}

	tierName := resolveTier(c.store.Config().Tiers, priority)
	_, err := c.store.DispatchWork(ctx, tierName, msg)
	return err
}

func (c *Client) publishEvent(ctx context.Context, event types.JobEvent) {
	c.store.PublishEvent(ctx, event)
}

func (c *Client) signalCancelActiveRuns(ctx context.Context, jobID string) {
	runs, _ := c.store.ListActiveRunsByJobID(ctx, jobID)
	for _, run := range runs {
		// Dual-write: durable flag (SET EX) + Pub/Sub fast path.
		// RequestCancel handles both; fall back to Pub/Sub only on error.
		if err := c.store.RequestCancel(ctx, run.ID); err != nil {
			c.rdb.Do(ctx, c.rdb.B().Publish().Channel(c.store.CancelChannel()).Message(run.ID).Build())
		}
	}
}

// cancelChildWorkflow recursively cancels a child workflow and all its tasks.
func (c *Client) cancelChildWorkflow(ctx context.Context, childWfID string) {
	childWf, rev, err := c.store.GetWorkflow(ctx, childWfID)
	if err != nil || childWf.Status.IsTerminal() {
		return
	}

	now := time.Now()
	childWf.Status = types.WorkflowStatusCancelled
	childWf.CompletedAt = &now
	childWf.UpdatedAt = now

	for name, state := range childWf.Tasks {
		if !state.Status.IsTerminal() {
			if state.JobID != "" {
				c.signalCancelActiveRuns(ctx, state.JobID)
			}
			if state.ChildWorkflowID != "" {
				c.cancelChildWorkflow(ctx, state.ChildWorkflowID)
			}
			state.Status = types.JobStatusCancelled
			state.FinishedAt = &now
			childWf.Tasks[name] = state
		}
	}

	c.store.UpdateWorkflow(ctx, childWf, rev)
}

func (c *Client) dispatchWorkflowTask(ctx context.Context, wf *types.WorkflowInstance, taskName string, now *time.Time) {
	var taskDef *types.WorkflowTask
	for i := range wf.Definition.Tasks {
		if wf.Definition.Tasks[i].Name == taskName {
			taskDef = &wf.Definition.Tasks[i]
			break
		}
	}
	if taskDef == nil {
		return
	}

	job := &types.Job{
		ID:           xid.New().String(),
		TaskType:     taskDef.TaskType,
		Payload:      taskDef.Payload,
		Schedule:     types.Schedule{Type: types.ScheduleImmediate},
		Status:       types.JobStatusPending,
		Priority:     wf.Definition.DefaultPriority,
		WorkflowID:   &wf.ID,
		WorkflowTask: &taskName,
		CreatedAt:    *now,
		UpdatedAt:    *now,
	}

	if _, err := c.store.CreateJob(ctx, job); err != nil {
		return
	}
	if err := c.dispatchJob(ctx, job); err != nil {
		c.store.DeleteJob(ctx, job.ID)
		return
	}

	if state, ok := wf.Tasks[taskName]; ok {
		state.JobID = job.ID
		state.Status = types.JobStatusRunning
		state.StartedAt = now
		wf.Tasks[taskName] = state
	}
}

func (c *Client) dispatchBatchOnetime(ctx context.Context, batch *types.BatchInstance, now *time.Time) {
	job := &types.Job{
		ID:        xid.New().String(),
		TaskType:  *batch.Definition.OnetimeTaskType,
		Payload:   batch.Definition.OnetimePayload,
		Schedule:  types.Schedule{Type: types.ScheduleImmediate},
		Status:    types.JobStatusPending,
		Priority:  batch.Definition.DefaultPriority,
		BatchID:   &batch.ID,
		BatchRole: strPtr("onetime"),
		CreatedAt: *now,
		UpdatedAt: *now,
	}

	if _, err := c.store.CreateJob(ctx, job); err != nil {
		return
	}
	if err := c.dispatchJob(ctx, job); err != nil {
		c.store.DeleteJob(ctx, job.ID)
		return
	}

	batch.OnetimeState = &types.BatchOnetimeState{
		JobID:     job.ID,
		Status:    types.JobStatusRunning,
		StartedAt: now,
	}
}

func (c *Client) dispatchBatchChunk(ctx context.Context, batch *types.BatchInstance, now *time.Time) {
	chunkSize := batch.Definition.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 100
	}

	items := batch.Definition.Items
	dispatched := 0

	for batch.NextChunkIndex < len(items) && dispatched < chunkSize && batch.RunningItems < chunkSize {
		item := items[batch.NextChunkIndex]

		if state, ok := batch.ItemStates[item.ID]; ok && state.Status != types.JobStatusPending {
			batch.NextChunkIndex++
			continue
		}

		job := &types.Job{
			ID:          xid.New().String(),
			TaskType:    batch.Definition.ItemTaskType,
			Payload:     item.Payload,
			Schedule:    types.Schedule{Type: types.ScheduleImmediate},
			Status:      types.JobStatusPending,
			Priority:    batch.Definition.DefaultPriority,
			BatchID:     &batch.ID,
			BatchItem:   &item.ID,
			BatchRole:   strPtr("item"),
			RetryPolicy: batch.Definition.ItemRetryPolicy,
			CreatedAt:   *now,
			UpdatedAt:   *now,
		}

		if _, err := c.store.CreateJob(ctx, job); err != nil {
			break
		}
		if err := c.dispatchJob(ctx, job); err != nil {
			c.store.DeleteJob(ctx, job.ID)
			break
		}

		batch.ItemStates[item.ID] = types.BatchItemState{
			ItemID:    item.ID,
			JobID:     job.ID,
			Status:    types.JobStatusRunning,
			StartedAt: now,
		}
		batch.NextChunkIndex++
		batch.RunningItems++
		batch.PendingItems--
		dispatched++
	}
}

// resolveTier maps a numeric priority to a configured tier name.
func resolveTier(tiers []store.TierConfig, priority types.Priority) string {
	if len(tiers) == 0 {
		return "normal"
	}
	if len(tiers) == 1 {
		return tiers[0].Name
	}

	p := int(priority)
	if p <= 0 {
		p = 5
	}
	if p > 10 {
		p = 10
	}

	bucketSize := 10 / len(tiers)
	if bucketSize == 0 {
		bucketSize = 1
	}

	idx := (10 - p) / bucketSize
	if idx >= len(tiers) {
		idx = len(tiers) - 1
	}
	return tiers[idx].Name
}

// rootTasks returns task names that have no dependencies (DAG roots).
func rootTasks(def *types.WorkflowDefinition) []string {
	var roots []string
	for _, t := range def.Tasks {
		if len(t.DependsOn) == 0 {
			roots = append(roots, t.Name)
		}
	}
	return roots
}

// readyTasks returns pending task names whose dependencies are all completed.
func readyTasks(wf *types.WorkflowInstance) []string {
	taskDefs := make(map[string]types.WorkflowTask, len(wf.Definition.Tasks))
	for _, t := range wf.Definition.Tasks {
		taskDefs[t.Name] = t
	}
	var ready []string
	for name, state := range wf.Tasks {
		if state.Status != types.JobStatusPending {
			continue
		}
		def, ok := taskDefs[name]
		if !ok {
			continue
		}
		allDepsDone := true
		for _, dep := range def.DependsOn {
			depState, exists := wf.Tasks[dep]
			if !exists || depState.Status != types.JobStatusCompleted {
				allDepsDone = false
				break
			}
		}
		if allDepsDone {
			ready = append(ready, name)
		}
	}
	return ready
}

func strPtr(s string) *string { return &s }
