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
// Unlike v1 which dispatches via Redis Streams (XADD), v2 publishes a
// lightweight Pub/Sub notification that the NotifierActor forwards to the
// DispatcherActor for push-based dispatch.
type Client struct {
	store *store.RedisStore
	rdb   rueidis.Client
	owned bool // true if we created the redis client
}

// Option configures the client.
type Option func(*clientConfig)

type clientConfig struct {
	redisURL      string
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

// WithRedisClient uses a pre-existing Redis client.
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

	if cfg.redisStore != nil {
		return &Client{
			store: cfg.redisStore,
			rdb:   cfg.redisStore.Client(),
			owned: false,
		}, nil
	}

	var rdb rueidis.Client
	owned := false
	if cfg.rdb != nil {
		rdb = cfg.rdb
	} else if len(cfg.clusterAddrs) > 0 {
		opt := rueidis.ClientOption{
			InitAddress:      cfg.clusterAddrs,
			BlockingPoolSize: max(cfg.redisPoolSize, 10),
		}
		if cfg.redisPassword != "" {
			opt.Password = cfg.redisPassword
		}
		var err error
		rdb, err = rueidis.NewClient(opt)
		if err != nil {
			return nil, fmt.Errorf("redis cluster client: %w", err)
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
			return nil, fmt.Errorf("redis client: %w", err)
		}
		owned = true
	} else {
		return nil, fmt.Errorf("Redis URL, Redis client, or RedisStore is required")
	}

	ctx := context.Background()
	if err := rdb.Do(ctx, rdb.B().Ping().Build()).Error(); err != nil {
		if owned {
			rdb.Close()
		}
		return nil, fmt.Errorf("redis ping: %w", err)
	}

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

func (c *Client) enqueue(ctx context.Context, req *EnqueueRequest) (*types.Job, error) {
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

	if job.UniqueKey != nil && *job.UniqueKey != "" {
		if err := c.store.SetUniqueKey(ctx, *job.UniqueKey, job.ID); err != nil {
			return nil, fmt.Errorf("unique key: %w", err)
		}
	}

	if job.Schedule.Type == types.ScheduleImmediate {
		job.Status = types.JobStatusPending
	} else {
		job.Status = types.JobStatusScheduled
	}

	if _, err := c.store.CreateJob(ctx, job); err != nil {
		if job.UniqueKey != nil && *job.UniqueKey != "" {
			c.store.DeleteUniqueKey(ctx, *job.UniqueKey)
		}
		return nil, fmt.Errorf("create job: %w", err)
	}

	// Publish job.enqueued so real-time subscribers (WS, UI) see the new job immediately.
	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventJobEnqueued,
		JobID:     job.ID,
		TaskType:  job.TaskType,
		Timestamp: now,
	})

	switch job.Schedule.Type {
	case types.ScheduleImmediate:
		if err := c.dispatchJob(ctx, job); err != nil {
			c.rollbackEnqueue(ctx, job)
			return nil, fmt.Errorf("dispatch: %w", err)
		}
		c.publishEvent(ctx, types.JobEvent{
			Type:      types.EventJobDispatched,
			JobID:     job.ID,
			TaskType:  job.TaskType,
			Timestamp: now,
		})

	default:
		nextRun, err := scheduler.NextRunTime(job.Schedule, now)
		if err != nil {
			c.rollbackEnqueue(ctx, job)
			return nil, fmt.Errorf("calculate next run: %w", err)
		}
		entry := &types.ScheduleEntry{
			JobID:     job.ID,
			Schedule:  job.Schedule,
			NextRunAt: nextRun,
		}
		if _, err := c.store.SaveSchedule(ctx, entry); err != nil {
			c.rollbackEnqueue(ctx, job)
			return nil, fmt.Errorf("save schedule: %w", err)
		}
		c.publishEvent(ctx, types.JobEvent{
			Type:      types.EventScheduleCreated,
			JobID:     job.ID,
			TaskType:  job.TaskType,
			Timestamp: now,
		})
	}

	return job, nil
}

// Cancel cancels a job by ID. This updates the job status and removes its
// schedule, but does NOT signal in-flight worker runs via Pub/Sub. To cancel
// active runs as well, use the monitor API's CancelJob endpoint instead.
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
	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventJobCancelled,
		JobID:     jobID,
		Timestamp: now,
	})
	return nil
}

// Retry re-enqueues a failed or dead job for another attempt.
func (c *Client) Retry(ctx context.Context, jobID string) error {
	job, rev, err := c.store.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job: %w", err)
	}
	if job.Status == types.JobStatusRunning {
		return fmt.Errorf("job is still running; cancel it first")
	}

	// Cancel any in-flight runs from the previous attempt.
	c.store.SignalCancelActiveRuns(ctx, jobID)

	now := time.Now()
	job.Status = types.JobStatusPending
	job.Attempt = 0
	job.LastError = nil
	job.UpdatedAt = now

	if _, err := c.store.UpdateJob(ctx, job, rev); err != nil {
		return fmt.Errorf("update job: %w", err)
	}

	if err := c.dispatchJob(ctx, job); err != nil {
		return fmt.Errorf("dispatch: %w", err)
	}

	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventJobRetrying,
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

// --- Workflow operations ---

// EnqueueWorkflow submits a workflow definition for execution.
func (c *Client) EnqueueWorkflow(ctx context.Context, def types.WorkflowDefinition, input any) (*types.WorkflowInstance, error) {
	if err := workflow.ValidateDAG(&def); err != nil {
		return nil, fmt.Errorf("validate workflow: %w", err)
	}

	now := time.Now()

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

	if def.ExecutionTimeout != nil {
		deadline := now.Add(def.ExecutionTimeout.Std())
		wf.Deadline = &deadline
	}

	rev, err := c.store.SaveWorkflow(ctx, wf)
	if err != nil {
		return nil, fmt.Errorf("save workflow: %w", err)
	}

	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventWorkflowStarted,
		JobID:     wf.ID,
		Timestamp: now,
	})

	roots := rootTasks(&def)
	for _, taskName := range roots {
		if err := c.dispatchWorkflowTask(ctx, wf, taskName, &now); err != nil {
			// Clean up already-dispatched child jobs before deleting parent.
			c.cancelDispatchedWorkflowChildren(ctx, wf)
			c.store.DeleteWorkflow(ctx, wf.ID)
			return nil, fmt.Errorf("dispatch root task: %w", err)
		}
	}

	if _, err := c.store.UpdateWorkflow(ctx, wf, rev); err != nil {
		// Children already dispatched — cancel them since we can't persist parent state.
		c.cancelDispatchedWorkflowChildren(ctx, wf)
		c.store.DeleteWorkflow(ctx, wf.ID)
		return nil, fmt.Errorf("update workflow after dispatch: %w", err)
	}

	return wf, nil
}

// GetWorkflow retrieves the current state of a workflow instance.
func (c *Client) GetWorkflow(ctx context.Context, workflowID string) (*types.WorkflowInstance, error) {
	wf, _, err := c.store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	return wf, nil
}

// --- Batch operations ---

// EnqueueBatch submits a batch definition for processing.
func (c *Client) EnqueueBatch(ctx context.Context, def types.BatchDefinition) (*types.BatchInstance, error) {
	if len(def.Items) == 0 && (def.OnetimeTaskType == nil || *def.OnetimeTaskType == "") {
		return nil, fmt.Errorf("batch has no items and no onetime task type")
	}

	// Validate no duplicate item IDs — duplicates would cause TotalItems
	// to exceed len(itemStates), making completion impossible.
	seen := make(map[string]struct{}, len(def.Items))
	for _, item := range def.Items {
		if _, dup := seen[item.ID]; dup {
			return nil, fmt.Errorf("duplicate batch item ID: %q", item.ID)
		}
		seen[item.ID] = struct{}{}
	}

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

	if def.ExecutionTimeout != nil {
		deadline := now.Add(def.ExecutionTimeout.Std())
		batch.Deadline = &deadline
	}

	batchRev, err := c.store.SaveBatch(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("save batch: %w", err)
	}

	c.publishEvent(ctx, types.JobEvent{
		Type:      types.EventBatchStarted,
		JobID:     batch.ID,
		Timestamp: now,
	})

	if def.OnetimeTaskType != nil && *def.OnetimeTaskType != "" {
		if err := c.dispatchBatchOnetime(ctx, batch, &now); err != nil {
			c.store.DeleteBatch(ctx, batch.ID)
			return nil, fmt.Errorf("dispatch batch onetime: %w", err)
		}
	} else {
		if err := c.dispatchBatchChunk(ctx, batch, &now); err != nil {
			c.cancelDispatchedBatchChildren(ctx, batch)
			c.store.DeleteBatch(ctx, batch.ID)
			return nil, fmt.Errorf("dispatch batch chunk: %w", err)
		}
	}

	if _, err := c.store.UpdateBatch(ctx, batch, batchRev); err != nil {
		// Children already dispatched — cancel them since we can't persist parent state.
		c.cancelDispatchedBatchChildren(ctx, batch)
		c.store.DeleteBatch(ctx, batch.ID)
		return nil, fmt.Errorf("update batch after dispatch: %w", err)
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

// --- Wait operations ---

// EnqueueAndWait submits a job for immediate execution and waits for the result.
//
// To avoid a subscribe-handshake race, the Pub/Sub subscription is established
// BEFORE the job is enqueued. This guarantees that any result notification
// published after enqueue is captured by the subscription.
func (c *Client) EnqueueAndWait(ctx context.Context, taskType types.TaskType, payload any, timeout time.Duration) (*types.WorkResult, error) {
	// Pre-generate a job ID so we can subscribe before enqueuing.
	jobID := xid.New().String()

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resultCh := store.ResultNotifyChannel(c.store.Prefix(), jobID)
	subscribeCmd := c.rdb.B().Subscribe().Channel(resultCh).Build()

	// 1. Start subscription BEFORE enqueue to close the race window.
	resultOut := make(chan *types.WorkResult, 1)
	errOut := make(chan error, 1)
	subReady := make(chan struct{}, 1)

	go func() {
		err := c.rdb.Receive(waitCtx, subscribeCmd, func(msg rueidis.PubSubMessage) {
			// Signal readiness on first callback invocation (subscription confirmation).
			select {
			case subReady <- struct{}{}:
			default:
			}

			var result types.WorkResult
			if err := sonic.ConfigFastest.UnmarshalFromString(msg.Message, &result); err != nil {
				return
			}
			if result.JobID == jobID {
				select {
				case resultOut <- &result:
				default:
				}
			}
		})
		// Signal readiness even on error so we don't block forever.
		select {
		case subReady <- struct{}{}:
		default:
		}
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
	//    the subscription became active (covers the edge case where the
	//    Receive goroutine hasn't started processing yet).
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

// enqueueWithID enqueues a job using a pre-generated ID (used by EnqueueAndWait).
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
		c.rollbackEnqueue(ctx, job)
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

// GetBatchResults retrieves all item results for a completed batch.
func (c *Client) GetBatchResults(ctx context.Context, batchID string) ([]*types.BatchItemResult, error) {
	return c.store.ListBatchItemResults(ctx, batchID)
}

// Close closes the client connection (only if we created the Redis client).
func (c *Client) Close() {
	if c.owned && c.rdb != nil {
		c.rdb.Close()
	}
}

// --- Internal helpers ---

// rollbackEnqueue cleans up a job and its unique key on enqueue failure.
func (c *Client) rollbackEnqueue(ctx context.Context, job *types.Job) {
	c.store.DeleteJob(ctx, job.ID)
	if job.UniqueKey != nil && *job.UniqueKey != "" {
		c.store.DeleteUniqueKey(ctx, *job.UniqueKey)
	}
}

// dispatchJob publishes a notification to the job:notify Pub/Sub channel.
// The NotifierActor on each server node subscribes to this channel and
// forwards it to the DispatcherActor for push-based dispatch.
func (c *Client) dispatchJob(ctx context.Context, job *types.Job) error {
	priority := int(types.PriorityNormal)
	if job.Priority != nil {
		priority = int(*job.Priority)
	}
	return c.store.PublishJobNotification(ctx, job.ID, string(job.TaskType), priority)
}

func (c *Client) publishEvent(ctx context.Context, event types.JobEvent) {
	c.store.PublishEvent(ctx, event)
}

func (c *Client) dispatchWorkflowTask(ctx context.Context, wf *types.WorkflowInstance, taskName string, now *time.Time) error {
	var taskDef *types.WorkflowTask
	for i := range wf.Definition.Tasks {
		if wf.Definition.Tasks[i].Name == taskName {
			taskDef = &wf.Definition.Tasks[i]
			break
		}
	}
	if taskDef == nil {
		return fmt.Errorf("task definition not found: %s", taskName)
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
		return fmt.Errorf("create job for task %s: %w", taskName, err)
	}
	if err := c.dispatchJob(ctx, job); err != nil {
		c.store.DeleteJob(ctx, job.ID)
		return fmt.Errorf("dispatch task %s: %w", taskName, err)
	}

	if state, ok := wf.Tasks[taskName]; ok {
		state.JobID = job.ID
		state.Status = types.JobStatusRunning
		state.StartedAt = now
		wf.Tasks[taskName] = state
	}
	return nil
}

func (c *Client) dispatchBatchOnetime(ctx context.Context, batch *types.BatchInstance, now *time.Time) error {
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
		return fmt.Errorf("create onetime job: %w", err)
	}
	if err := c.dispatchJob(ctx, job); err != nil {
		c.store.DeleteJob(ctx, job.ID)
		return fmt.Errorf("dispatch onetime job: %w", err)
	}

	batch.OnetimeState = &types.BatchOnetimeState{
		JobID:     job.ID,
		Status:    types.JobStatusRunning,
		StartedAt: now,
	}
	return nil
}

func (c *Client) dispatchBatchChunk(ctx context.Context, batch *types.BatchInstance, now *time.Time) error {
	chunkSize := batch.Definition.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 100
	}

	items := batch.Definition.Items
	dispatched := 0
	var firstErr error

	for batch.NextChunkIndex < len(items) && dispatched < chunkSize && batch.RunningItems < chunkSize {
		item := items[batch.NextChunkIndex]
		batch.NextChunkIndex++

		if state, ok := batch.ItemStates[item.ID]; ok && state.Status != types.JobStatusPending {
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
			if firstErr == nil {
				firstErr = fmt.Errorf("create batch item job %s: %w", item.ID, err)
			}
			errStr := err.Error()
			batch.ItemStates[item.ID] = types.BatchItemState{
				ItemID:     item.ID,
				Status:     types.JobStatusFailed,
				Error:      &errStr,
				FinishedAt: now,
			}
			batch.FailedItems++
			batch.PendingItems--
			continue
		}
		if err := c.dispatchJob(ctx, job); err != nil {
			c.store.DeleteJob(ctx, job.ID)
			if firstErr == nil {
				firstErr = fmt.Errorf("dispatch batch item %s: %w", item.ID, err)
			}
			errStr := err.Error()
			batch.ItemStates[item.ID] = types.BatchItemState{
				ItemID:     item.ID,
				Status:     types.JobStatusFailed,
				Error:      &errStr,
				FinishedAt: now,
			}
			batch.FailedItems++
			batch.PendingItems--
			continue
		}

		batch.ItemStates[item.ID] = types.BatchItemState{
			ItemID:    item.ID,
			JobID:     job.ID,
			Status:    types.JobStatusRunning,
			StartedAt: now,
		}
		batch.RunningItems++
		batch.PendingItems--
		dispatched++
	}

	if dispatched == 0 && firstErr != nil {
		return firstErr
	}
	return nil
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

// cancelDispatchedWorkflowChildren cancels and deletes child jobs that were
// already dispatched for workflow tasks. Called during rollback when a later
// root task dispatch or final parent update fails.
func (c *Client) cancelDispatchedWorkflowChildren(ctx context.Context, wf *types.WorkflowInstance) {
	now := time.Now()
	for _, state := range wf.Tasks {
		if state.JobID == "" {
			continue
		}
		if childJob, childRev, err := c.store.GetJob(ctx, state.JobID); err == nil {
			if !childJob.Status.IsTerminal() {
				childJob.Status = types.JobStatusCancelled
				childJob.UpdatedAt = now
				c.store.UpdateJob(ctx, childJob, childRev)
				c.store.SignalCancelActiveRuns(ctx, state.JobID)
			}
		}
	}
}

// cancelDispatchedBatchChildren cancels and deletes child jobs that were
// already dispatched for batch items. Called during rollback when a later
// item dispatch or final parent update fails.
func (c *Client) cancelDispatchedBatchChildren(ctx context.Context, batch *types.BatchInstance) {
	now := time.Now()
	for _, state := range batch.ItemStates {
		if state.JobID == "" {
			continue
		}
		if childJob, childRev, err := c.store.GetJob(ctx, state.JobID); err == nil {
			if !childJob.Status.IsTerminal() {
				childJob.Status = types.JobStatusCancelled
				childJob.UpdatedAt = now
				c.store.UpdateJob(ctx, childJob, childRev)
				c.store.SignalCancelActiveRuns(ctx, state.JobID)
			}
		}
	}
	if batch.OnetimeState != nil && batch.OnetimeState.JobID != "" {
		if childJob, childRev, err := c.store.GetJob(ctx, batch.OnetimeState.JobID); err == nil {
			if !childJob.Status.IsTerminal() {
				childJob.Status = types.JobStatusCancelled
				childJob.UpdatedAt = now
				c.store.UpdateJob(ctx, childJob, childRev)
				c.store.SignalCancelActiveRuns(ctx, batch.OnetimeState.JobID)
			}
		}
	}
}

func strPtr(s string) *string { return &s }
