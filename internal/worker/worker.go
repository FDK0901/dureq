package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FDK0901/dureq/internal/dispatcher"
	"github.com/FDK0901/dureq/internal/dynconfig"
	"github.com/FDK0901/dureq/internal/lock"
	"github.com/FDK0901/dureq/internal/ratelimit"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/redis/rueidis"
)

// HandlerRegistry is the interface for looking up handlers by task type.
type HandlerRegistry interface {
	Get(taskType types.TaskType) (*types.HandlerDefinition, bool)
	TaskTypes() []string
}

// Config holds worker configuration.
type Config struct {
	NodeID            string
	Store             *store.RedisStore
	Registry          HandlerRegistry
	Disp              *dispatcher.Dispatcher
	Locker            *lock.Locker
	MaxConcurrency    int
	ShutdownTimeout   time.Duration // Grace period before aborting in-flight tasks. Default: 30s.
	GlobalMiddlewares []types.MiddlewareFunc
	DynConfig         *dynconfig.Manager // Optional: runtime config overrides (timeout, maxAttempts).
	Logger            chainedlog.Logger
}

// streamMessage wraps a parsed work message with the stream metadata needed for ack/delay.
type streamMessage struct {
	Work         types.WorkMessage
	StreamMsgID  string
	TierName     string
	Redeliveries int

	standalone bool // the job is not part of a workflow or batch
	completed  bool // CompleteRun has been called
}

// maxRedeliveries is the limit before a message with no handler is sent to DLQ.
const maxRedeliveries = 50

// Worker pulls work messages from Redis Streams and executes them
// using a panjf2000/ants goroutine pool.
type Worker struct {
	nodeID            string
	store             *store.RedisStore
	registry          HandlerRegistry
	disp              *dispatcher.Dispatcher
	locker            *lock.Locker
	pool              *taskPool                    // global pool for tasks without dedicated concurrency
	subpools          map[types.TaskType]*taskPool // per-TaskType pools
	shared            bool                         // some task types have no pool of their own
	freed             chan struct{}                // signals that a pool has room again
	globalMiddlewares []types.MiddlewareFunc
	dynCfg            *dynconfig.Manager
	tiers             []store.TierConfig
	logger            chainedlog.Logger

	rateLimiter     *ratelimit.Limiter
	activeRuns      sync.Map // runID → struct{} for heartbeat/crash detection
	cancelations    sync.Map // runID → context.CancelFunc for per-task cancellation
	quit            chan struct{}
	abort           chan struct{}
	shutdownTimeout time.Duration
	stopOnce        sync.Once
	syncer          *SyncRetrier

	// Executions since the worker was created.
	submitted atomic.Uint64
	completed atomic.Uint64
	failed    atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a new worker.
func New(cfg Config) (*Worker, error) {
	if cfg.Logger == nil {
		cfg.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	if cfg.MaxConcurrency == 0 {
		cfg.MaxConcurrency = 100
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}

	freed := make(chan struct{}, 1)
	pool, err := newTaskPool(cfg.MaxConcurrency, freed)
	if err != nil {
		return nil, err
	}

	// Create per-TaskType subpools for handlers that specify Concurrency > 0.
	shared := false
	subpools := make(map[types.TaskType]*taskPool)
	for _, tt := range cfg.Registry.TaskTypes() {
		def, ok := cfg.Registry.Get(types.TaskType(tt))
		if !ok || def.Concurrency <= 0 {
			shared = true
			continue
		}
		sp, err := newTaskPool(def.Concurrency, freed)
		if err != nil {
			return nil, err
		}
		subpools[types.TaskType(tt)] = sp
		cfg.Logger.Info().String("task_type", tt).Int("concurrency", def.Concurrency).Msg("worker: created subpool")
	}

	return &Worker{
		nodeID:            cfg.NodeID,
		store:             cfg.Store,
		registry:          cfg.Registry,
		disp:              cfg.Disp,
		locker:            cfg.Locker,
		pool:              pool,
		subpools:          subpools,
		shared:            shared,
		freed:             freed,
		globalMiddlewares: cfg.GlobalMiddlewares,
		dynCfg:            cfg.DynConfig,
		tiers:             cfg.Store.Config().Tiers,
		rateLimiter:       ratelimit.New(cfg.Store.Client(), cfg.Store.Prefix()),
		logger:            cfg.Logger,
		quit:              make(chan struct{}),
		abort:             make(chan struct{}),
		shutdownTimeout:   cfg.ShutdownTimeout,
	}, nil
}

// Start begins consuming work messages from Redis Streams.
// Creates XREADGROUP consumers for each tier and runs a weighted fetch loop.
func (w *Worker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.syncer = NewSyncRetrier(w.ctx, w.logger)

	go w.priorityFetchLoop()
	go w.claimStaleLoop()
	go w.refreshLoop()
	go w.cancelSubscribeLoop()

	w.logger.Info().String("node_id", w.nodeID).Int("pool_capacity", w.pool.pool.Cap()).Int("tiers", len(w.tiers)).Msg("worker started with priority consumers")
	return nil
}

// Stop gracefully stops the worker using a two-phase shutdown:
//  1. Close quit channel → fetch loop stops accepting new messages.
//  2. After ShutdownTimeout → close abort channel → in-flight handlers
//     detect the signal and requeue their messages for other workers.
//  3. ReleaseTimeout waits for all ants pool workers to return.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		close(w.quit)
		w.returnHeld()

		time.AfterFunc(w.shutdownTimeout, func() {
			close(w.abort)
		})

		releaseTimeout := w.shutdownTimeout + 5*time.Second
		if w.pool != nil {
			w.pool.pool.ReleaseTimeout(releaseTimeout)
		}
		for _, sp := range w.subpools {
			sp.pool.ReleaseTimeout(releaseTimeout)
		}

		if w.cancel != nil {
			w.cancel()
		}
	})
}

// Stats returns current worker pool statistics.
func (w *Worker) Stats() *types.PoolStats {
	if w.pool == nil {
		return nil
	}
	return &types.PoolStats{
		RunningWorkers: w.pool.running(),
		IdleWorkers:    w.pool.free(),
		MaxConcurrency: w.pool.pool.Cap(),
		TotalSubmitted: w.submitted.Load(),
		TotalCompleted: w.completed.Load(),
		TotalFailed:    w.failed.Load(),
	}
}

// SyncerStats returns the current SyncRetrier stats for the monitoring API.
func (w *Worker) SyncerStats() *SyncRetryStats {
	if w.syncer == nil {
		return nil
	}
	stats := w.syncer.Stats()
	return &stats
}

// ActiveRunIDs returns the list of currently executing run IDs on this worker.
func (w *Worker) ActiveRunIDs() []string {
	var ids []string
	w.activeRuns.Range(func(key, _ any) bool {
		ids = append(ids, key.(string))
		return true
	})
	return ids
}

// availableCapacity returns how many messages the global pool and all per-TaskType subpools
// take. The value is a best-effort estimate (racy) but sufficient for throttling XREADGROUP
// batch sizes.
func (w *Worker) availableCapacity() int {
	avail := 0
	if w.shared {
		avail = w.pool.free()
	}
	for _, sp := range w.subpools {
		avail += sp.room()
	}
	return avail
}

const (
	// pollInterval is how often an idle worker looks for work without being notified of any.
	pollInterval = 500 * time.Millisecond
	// gateInterval is how often a busy worker checks whether it is draining and which
	// queues are paused.
	gateInterval = 50 * time.Millisecond
	// A worker that is notified of work which another node has taken by the time it looks
	// waits a little before it looks again, and the longer the more often that happens in a
	// row: all nodes are notified of every job.
	minMissDelay = 1 * time.Millisecond
	maxMissDelay = 16 * time.Millisecond
	// returnTimeout bounds how long a worker that is stopping takes to return messages.
	returnTimeout = 5 * time.Second
)

// priorityFetchLoop polls tier streams in weight-descending order via XREADGROUP.
// Higher-weight tiers are polled first and with larger batch sizes.
// The batch size is dynamically capped by pool capacity to avoid dequeuing
// more messages than can be immediately processed (backpressure).
//
// A tier is read without waiting for messages, or an empty one would hold up the others.
// When there is nothing to do, a Pub/Sub subscriber on JobNotifyChannel wakes the loop as
// soon as work is dispatched (reduces dispatch-to-start latency from ~500ms to near-zero).
// When there is no room, the end of a running message does.
func (w *Worker) priorityFetchLoop() {
	// Start push notification subscriber.
	notify := make(chan struct{}, 1)
	go w.jobNotifySubscriber(notify)

	var (
		gateAt    time.Time
		draining  bool
		paused    []bool // by tier
		missDelay time.Duration
	)
	for {
		select {
		case <-w.quit:
			return
		default:
		}

		if time.Since(gateAt) >= gateInterval {
			draining, paused = w.store.FetchGate(w.ctx, w.nodeID)
			gateAt = time.Now()
		}
		// Skip all tiers if this node is draining.
		if draining {
			if !w.pause(1 * time.Second) {
				return
			}
			continue
		}

		fetched, full, poll := 0, false, pollInterval
		for i, tier := range w.tiers {
			// Skip paused queues.
			if paused[i] {
				continue
			}
			avail := w.availableCapacity()
			if avail == 0 {
				full = true
				break
			}
			n, retryIn := w.fetchTier(tier, min(tier.FetchBatch, avail))
			fetched += n
			if retryIn > 0 {
				poll = min(poll, retryIn)
			}
		}

		switch {
		case fetched > 0:
			missDelay = 0
		case full:
			// Wait for a running message to end.
			select {
			case <-w.freed:
			case <-time.After(pollInterval):
			case <-w.quit:
				return
			}
		default:
			// Wait for either a push notification or a fallback poll interval.
			select {
			case <-notify:
				if missDelay > 0 && !w.pause(missDelay) {
					return
				}
				missDelay = min(max(2*missDelay, minMissDelay), maxMissDelay)
			case <-time.After(poll):
				missDelay = 0
			case <-w.quit:
				return
			}
		}
	}
}

// pause waits, and reports false if the worker is to quit.
func (w *Worker) pause(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-w.quit:
		return false
	}
}

// fetchTier takes up to count messages off a tier, has them processed or put back, and returns
// how many there were. If the rate limit of the tier does not allow any, it returns when to try
// again.
func (w *Worker) fetchTier(tier store.TierConfig, count int) (fetched int, retryIn time.Duration) {
	used := 0 // of the rate limit
	// Check rate limit if configured.
	if tier.RateLimit > 0 {
		burst := tier.RateBurst
		if burst <= 0 {
			burst = int(math.Ceil(tier.RateLimit))
			if burst < 1 {
				burst = 1
			}
		}
		count = min(count, burst) // more than that is never allowed
		limit := ratelimit.Config{
			MaxTokens:  burst,
			RefillRate: tier.RateLimit,
		}
		allowed, err := w.rateLimiter.Allow(w.ctx, tier.Name, count, limit)
		if err != nil {
			w.logger.Warn().Err(err).String("tier", tier.Name).Msg("worker: rate limit check failed")
		}
		if !allowed {
			return 0, time.Duration(float64(count) / tier.RateLimit * float64(time.Second))
		}
		defer func() {
			if used < count {
				if err := w.rateLimiter.Return(w.ctx, tier.Name, count-used, limit); err != nil && w.ctx.Err() == nil {
					w.logger.Warn().Err(err).String("tier", tier.Name).Msg("worker: rate limit return failed")
				}
			}
		}()
	}

	streamKey := store.WorkStreamKey(w.store.Prefix(), tier.Name)
	cmd := w.store.Client().B().Xreadgroup().Group(store.ConsumerGroup, w.nodeID).Count(int64(count)).Streams().Key(streamKey).Id(">").Build()
	xStreams, err := w.store.Client().Do(w.ctx, cmd).AsXRead()
	if err != nil {
		if !rueidis.IsRedisNil(err) && w.ctx.Err() == nil {
			w.logger.Warn().Err(err).String("tier", tier.Name).Msg("worker: XREADGROUP error")
		}
		return 0, 0
	}

	var sms []*streamMessage
	for _, entries := range xStreams {
		for _, entry := range entries {
			sms = append(sms, parseStreamMessage(entry, tier.Name))
		}
	}
	fetched = len(sms)
	used = fetched - w.process(tier.Name, sms)
	return fetched, 0
}

// process has messages of a tier processed. Those of a task type with a limit that has enough
// of them are put back, so that they do not hold up the others, and process returns how many
// there were.
func (w *Worker) process(tierName string, sms []*streamMessage) (putBack int) {
	tooMany := make(map[*taskPool][]*streamMessage)
	for _, sm := range sms {
		run := func() { w.processMessage(sm) }
		pool, limited := w.subpools[sm.Work.TaskType]
		if !limited {
			if err := w.pool.submit(run); err != nil {
				w.logger.Error().Err(err).Msg("worker: pool submit failed")
				// Don't ack — XAUTOCLAIM will reclaim it later.
			}
		} else if !pool.offer(sm, run) {
			tooMany[pool] = append(tooMany[pool], sm)
		}
	}
	for pool, sms := range tooMany {
		if err := w.postpone(w.ctx, tierName, sms, pool.returnTimes(len(sms))); err != nil {
			if w.ctx.Err() == nil {
				w.logger.Error().Err(err).String("tier", tierName).Int("messages", len(sms)).Msg("worker: failed to put back messages, waiting for room")
			}
			for _, sm := range sms {
				pool.submit(func() { w.processMessage(sm) })
			}
			continue
		}
		putBack += len(sms)
	}
	return putBack
}

// postpone moves messages to the delayed set of their tier.
func (w *Worker) postpone(ctx context.Context, tierName string, sms []*streamMessage, until []time.Time) error {
	msgs := make([]store.PostponedMessage, len(sms))
	for i, sm := range sms {
		msgs[i] = store.PostponedMessage{Work: &sm.Work, MessageID: sm.StreamMsgID, Until: until[i]}
	}
	return w.store.Postpone(ctx, tierName, msgs)
}

// refreshLoop keeps other nodes from taking over messages that are kept at hand.
func (w *Worker) refreshLoop() {
	for w.pause(claimRefreshInterval) {
		for _, sp := range w.subpools {
			for _, sm := range sp.held() {
				if err := w.store.RefreshMessageClaim(w.ctx, sm.TierName, w.nodeID, sm.StreamMsgID); err != nil && w.ctx.Err() == nil {
					w.logger.Warn().String("run_id", sm.Work.RunID).Err(err).Msg("worker: stream claim refresh failed")
				}
			}
		}
	}
}

// returnHeld gives other nodes the messages that are kept at hand.
func (w *Worker) returnHeld() {
	byTier := make(map[string][]*streamMessage)
	for _, sp := range w.subpools {
		for _, sm := range sp.close() {
			byTier[sm.TierName] = append(byTier[sm.TierName], sm)
		}
	}
	for tierName, sms := range byTier {
		w.returnMessages(tierName, sms)
	}
}

// returnMessages gives other nodes messages that this one does not process, as it is stopping.
func (w *Worker) returnMessages(tierName string, sms []*streamMessage) {
	if len(sms) == 0 {
		return
	}
	now := make([]time.Time, len(sms))
	for i := range now {
		now[i] = time.Now()
	}
	ctx, cancel := context.WithTimeout(context.Background(), returnTimeout)
	defer cancel()
	if err := w.postpone(ctx, tierName, sms, now); err != nil {
		w.logger.Error().Err(err).String("tier", tierName).Int("messages", len(sms)).Msg("worker: failed to return messages, leaving them for XAUTOCLAIM")
	}
}

// jobNotifySubscriber subscribes to the JobNotifyChannel and sends a wake-up
// signal on the notify channel whenever a new job is dispatched.
func (w *Worker) jobNotifySubscriber(notify chan<- struct{}) {
	ch := store.JobNotifyChannel(w.store.Prefix())
	cmd := w.store.Client().B().Subscribe().Channel(ch).Build()

	err := w.store.Client().Receive(w.ctx, cmd, func(msg rueidis.PubSubMessage) {
		select {
		case notify <- struct{}{}:
		default:
		}
	})
	if err != nil && w.ctx.Err() == nil {
		w.logger.Warn().Err(err).Msg("worker: job notify subscriber ended")
	}
}

const (
	// staleClaimMinIdle is how long a stream entry must sit idle in the PEL
	// before claimStaleLoop reclaims it from its (presumably crashed) owner.
	staleClaimMinIdle = 5 * time.Minute
	// claimRefreshInterval is how often a running handler resets its entry's
	// idle time. Must stay well below staleClaimMinIdle.
	claimRefreshInterval = 30 * time.Second
)

// claimStaleLoop periodically reclaims unacknowledged messages from crashed workers.
func (w *Worker) claimStaleLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.quit:
			return
		case <-ticker.C:
			for _, tier := range w.tiers {
				msgs, err := w.store.ClaimStaleMessages(w.ctx, tier.Name, w.nodeID, staleClaimMinIdle, 10)
				if err != nil {
					if w.ctx.Err() == nil {
						w.logger.Warn().Err(err).String("tier", tier.Name).Msg("worker: XAUTOCLAIM error")
					}
					continue
				}
				sms := make([]*streamMessage, len(msgs))
				for i, xmsg := range msgs {
					sms[i] = parseStreamMessage(xmsg, tier.Name)
				}
				w.process(tier.Name, sms)
				if len(msgs) > 0 {
					w.logger.Info().String("tier", tier.Name).Int("claimed", len(msgs)).Msg("worker: reclaimed stale messages")
				}
			}
		}
	}
}

// parseStreamMessage constructs a streamMessage from a rueidis XRangeEntry.
func parseStreamMessage(entry rueidis.XRangeEntry, tierName string) *streamMessage {
	sm := &streamMessage{
		Work:        store.WorkMessageFromStreamValues(entry.FieldValues),
		StreamMsgID: entry.ID,
		TierName:    tierName,
	}
	if s, ok := entry.FieldValues["redeliveries"]; ok {
		n, _ := strconv.Atoi(s)
		sm.Redeliveries = n
	}
	return sm
}

// runHeartbeat periodically updates the run's LastHeartbeatAt field while the job executes.
// Also refreshes the operation ledger TTL to prevent expiry during long-running jobs,
// and the stream entry's idle time so claimStaleLoop doesn't reclaim a live run.
func (w *Worker) runHeartbeat(ctx context.Context, run *types.JobRun, mu *sync.Mutex, operationKey string, sm *streamMessage) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	var lastClaimRefresh time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			mu.Lock()
			run.LastHeartbeatAt = &now
			snapshot := *run
			mu.Unlock()
			w.store.SaveRun(w.ctx, &snapshot)
			// Refresh operation ledger TTL to prevent expiry during long-running jobs.
			if operationKey != "" {
				w.store.RefreshOperationLedger(w.ctx, operationKey)
			}
			if now.Sub(lastClaimRefresh) >= claimRefreshInterval {
				if err := w.store.RefreshMessageClaim(w.ctx, sm.TierName, w.nodeID, sm.StreamMsgID); err != nil {
					w.logger.Warn().String("run_id", run.ID).Err(err).Msg("worker: stream claim refresh failed")
				} else {
					lastClaimRefresh = now
				}
			}
		}
	}
}

// executeResult carries the outcome of a handler execution from the
// nested goroutine back to the monitoring select.
type executeResult struct {
	err    error
	output json.RawMessage
}

// processMessage handles a single work message from a Redis Stream.
// The handler is executed in an isolated goroutine while the ants pool
// worker monitors abort, context cancellation, and normal completion
// via select.
func (w *Worker) processMessage(sm *streamMessage) {
	work := sm.Work

	if work.JobID == "" || work.RunID == "" {
		w.logger.Error().Msg("worker: invalid work message (missing job_id or run_id)")
		w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID)
		return
	}

	job, rev, jobErr := w.store.GetJob(w.ctx, work.JobID)
	sm.standalone = jobErr == nil && job.WorkflowID == nil && job.BatchID == nil

	// What is still queued of a cancelled job does not run any more. Workflow and
	// batch jobs are left to the orchestrator, which waits for their outcome.
	if sm.standalone && job.Status == types.JobStatusCancelled {
		w.logger.Info().String("job_id", work.JobID).String("run_id", work.RunID).Msg("worker: job is cancelled, dropping message")
		w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID)
		return
	}

	// ScheduleToStart timeout check.
	if !work.DispatchedAt.IsZero() {
		if jobErr == nil && job.ScheduleToStartTimeout != nil {
			waited := time.Since(work.DispatchedAt)
			if waited > job.ScheduleToStartTimeout.Std() {
				w.logger.Warn().String("job_id", work.JobID).String("run_id", work.RunID).String("waited", waited.String()).Msg("worker: schedule-to-start timeout exceeded")

				now := time.Now()
				errStr := types.ErrScheduleToStartTimeout.Error()
				if !w.staysEnded(job) {
					job.Status = types.JobStatusFailed
					job.LastError = &errStr
					job.UpdatedAt = now
					if _, err := w.store.UpdateJob(w.ctx, job, rev); err != nil {
						w.logger.Error().String("job_id", work.JobID).Err(err).Msg("worker: schedule-to-start timeout: failed to update job")
					}
				}

				w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID)

				w.disp.PublishEvent(types.JobEvent{
					Type:      types.EventJobScheduleToStartTimeout,
					JobID:     work.JobID,
					RunID:     work.RunID,
					NodeID:    w.nodeID,
					TaskType:  work.TaskType,
					Error:     &errStr,
					Timestamp: now,
				})
				return
			}
		}
	}

	// Look up handler.
	def, ok := w.registry.Get(work.TaskType)
	if !ok {
		// Requeue/DLQ BEFORE ACK to prevent message loss if requeue fails.
		if sm.Redeliveries >= maxRedeliveries {
			if w.clusterHasTaskType(work.TaskType) {
				if err := w.store.ReenqueueWork(w.ctx, sm.TierName, &work, 0); err != nil {
					w.logger.Error().Err(err).String("job_id", work.JobID).Msg("worker: requeue failed, leaving for XAUTOCLAIM")
					return
				}
				w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID)
				return
			}
			w.logger.Warn().String("task_type", string(work.TaskType)).Int("redeliveries", sm.Redeliveries).Msg("worker: no handler in cluster, sending to DLQ")
			w.disp.DispatchToDLQ(w.ctx, &types.Job{ID: work.JobID, TaskType: work.TaskType, Payload: work.Payload}, "no handler registered for task type: "+string(work.TaskType))
			w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID)
			return
		}
		if err := w.store.ReenqueueWork(w.ctx, sm.TierName, &work, sm.Redeliveries+1); err != nil {
			w.logger.Error().Err(err).String("job_id", work.JobID).Msg("worker: requeue failed, leaving for XAUTOCLAIM")
			return
		}
		w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID)
		return
	}

	// --- Worker version check ---
	// If the handler has a version and the message was dispatched with a different version,
	// re-enqueue the message so a matching worker can pick it up.
	if def.Version != "" && work.Version != "" && def.Version != work.Version {
		w.logger.Debug().
			String("job_id", work.JobID).
			String("handler_version", def.Version).
			String("msg_version", work.Version).
			Msg("worker: version mismatch, re-enqueuing")
		// Requeue BEFORE ACK — only ACK if requeue succeeds.
		if err := w.store.ReenqueueWork(w.ctx, sm.TierName, &work, sm.Redeliveries+1); err != nil {
			w.logger.Error().Err(err).String("job_id", work.JobID).Msg("worker: requeue failed, leaving for XAUTOCLAIM")
			return
		}
		w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID)
		return
	}

	// --- Operation ledger check (exactly-once L2) ---
	// If this operation was already completed by a previous attempt,
	// skip execution entirely and just ACK the message.
	if work.OperationKey != "" {
		ledgerResult, err := w.store.CheckAndClaimOperation(w.ctx, work.OperationKey, work.RunID)
		if err != nil {
			w.logger.Warn().String("op_key", work.OperationKey).Err(err).Msg("worker: operation ledger check failed, proceeding without L2 guard")
		} else if strings.HasPrefix(ledgerResult, "DONE:") {
			w.logger.Info().String("op_key", work.OperationKey).String("run_id", work.RunID).Msg("worker: operation already completed, running repair + ACK")
			// Repair: ensure result is stored, message is ACK'd, and result notification
			// is published — even if the original worker crashed after ledger commit.
			resultJSON := ledgerResult[5:]
			w.store.RepairCompletedOperation(w.ctx, work.JobID, sm.TierName, sm.StreamMsgID, resultJSON)
			// Best-effort job status repair (idempotent via CAS).
			go func() {
				job, rev, getErr := w.store.GetJob(w.ctx, work.JobID)
				if getErr != nil || job.Status.IsTerminal() {
					return
				}
				now := time.Now()
				_, _, schedErr := w.store.GetSchedule(w.ctx, work.JobID)
				if schedErr == nil {
					job.Status = types.JobStatusScheduled
				} else {
					job.Status = types.JobStatusCompleted
					job.CompletedAt = &now
				}
				job.UpdatedAt = now
				if job.Schedule.Type.IsRecurring() {
					job.Attempt = 0
				}
				w.store.UpdateJob(w.ctx, job, rev)
			}()
			return
		}
	}

	// --- Per-run lock (exactly-once guard) ---
	// Prevents duplicate execution when XAUTOCLAIM reclaims a message
	// while the original worker's handler is still running.
	lockOwner := fmt.Sprintf("%s:%d", w.nodeID, time.Now().UnixNano())
	rl, cancelFlag, lockErr := w.locker.LockAndDo(w.ctx, "run:"+work.RunID, lockOwner, w.store.CancelRequestedCmd(work.RunID))
	if lockErr != nil {
		if errors.Is(lockErr, types.ErrLockFailed) {
			// Don't ack: this may be the very entry the lock holder is executing,
			// reclaimed while it still runs. ACKing would drop it from the PEL and
			// lose the run if the holder then crashes. The holder ACKs on completion;
			// otherwise XAUTOCLAIM redelivers it once the lock expires.
			w.logger.Info().String("run_id", work.RunID).Msg("worker: run already locked by another worker, leaving pending")
			return
		}
		w.logger.Warn().String("run_id", work.RunID).Err(lockErr).Msg("worker: lock acquisition error")
		// Don't ack — XAUTOCLAIM will retry later.
		return
	}
	defer rl.Unlock(w.ctx)

	// --- Concurrency key slot acquisition ---
	// If the job has concurrency keys, acquire a slot for each.
	// If any slot is unavailable, re-enqueue with a short delay.
	if len(work.ConcurrencyKeys) > 0 {
		var acquired []types.ConcurrencyKey
		var blocked bool
		for _, ck := range work.ConcurrencyKeys {
			ok, err := w.store.AcquireConcurrencySlot(w.ctx, ck.Key, work.RunID, ck.MaxConcurrency)
			if err != nil {
				w.logger.Warn().String("run_id", work.RunID).String("conc_key", ck.Key).Err(err).Msg("worker: concurrency slot acquire error")
				blocked = true
				break
			}
			if !ok {
				w.logger.Debug().String("run_id", work.RunID).String("conc_key", ck.Key).Int("max", ck.MaxConcurrency).Msg("worker: concurrency key at capacity, re-enqueuing")
				blocked = true
				break
			}
			acquired = append(acquired, ck)
		}
		if blocked {
			// Release any slots we acquired before the blocking one.
			for _, ck := range acquired {
				w.store.ReleaseConcurrencySlot(w.ctx, ck.Key, work.RunID)
			}
			// Re-enqueue with 1s delay — only ACK if AddDelayed succeeds.
			if err := w.store.AddDelayed(w.ctx, sm.TierName, &work, time.Now().Add(1*time.Second)); err != nil {
				w.logger.Error().Err(err).String("job_id", work.JobID).Msg("worker: delayed requeue failed, leaving for XAUTOCLAIM")
				return
			}
			w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID)
			return
		}
		// Ensure all slots are released when the handler finishes.
		defer func() {
			for _, ck := range work.ConcurrencyKeys {
				w.store.ReleaseConcurrencySlot(w.ctx, ck.Key, work.RunID)
			}
		}()
	}

	// --- Durable cancel check (before handler start) ---
	// If a cancel was requested while this message was pending (e.g., worker was offline),
	// skip execution immediately.
	if n, _ := cancelFlag.AsInt64(); n > 0 {
		w.logger.Info().String("run_id", work.RunID).Msg("worker: cancel flag set before handler start, skipping")
		w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID)
		w.store.ClearCancelFlag(w.ctx, work.RunID)
		return
	}
	// Clean up cancel flag when the run finishes (regardless of outcome). CompleteRun has
	// done so by then.
	defer func() {
		if !sm.completed {
			w.store.ClearCancelFlag(w.ctx, work.RunID)
		}
	}()

	now := time.Now()

	// Track active run for heartbeat/crash detection.
	w.activeRuns.Store(work.RunID, struct{}{})
	defer w.activeRuns.Delete(work.RunID)

	// Record the run.
	run := &types.JobRun{
		ID:        work.RunID,
		JobID:     work.JobID,
		NodeID:    w.nodeID,
		Status:    types.RunStatusRunning,
		Attempt:   work.Attempt,
		StartedAt: now,

		OperationKey: work.OperationKey,
	}
	w.disp.StartRun(w.ctx, run, types.JobEvent{
		Type:      types.EventJobStarted,
		JobID:     work.JobID,
		RunID:     work.RunID,
		NodeID:    w.nodeID,
		TaskType:  work.TaskType,
		Attempt:   work.Attempt,
		Timestamp: now,
	})

	w.submitted.Add(1)
	defer func() {
		switch run.Status {
		case types.RunStatusSucceeded:
			w.completed.Add(1)
		case types.RunStatusFailed:
			w.failed.Add(1)
		}
	}()

	// Start heartbeat goroutine for this run.
	var runMu sync.Mutex
	var hbWg sync.WaitGroup
	hbCtx, hbCancel := context.WithCancel(w.ctx)
	defer hbCancel()
	hbWg.Add(1)
	go func() {
		defer hbWg.Done()
		w.runHeartbeat(hbCtx, run, &runMu, work.OperationKey, sm)
	}()

	// Set up execution context with deadline.
	// Dynamic config overrides take precedence over static handler config.
	timeout := def.Timeout
	if w.dynCfg != nil {
		if ho, ok := w.dynCfg.GetHandler(string(work.TaskType)); ok && ho.Timeout > 0 {
			timeout = ho.Timeout
		}
	}
	execCtx := w.ctx
	var execCancel context.CancelFunc
	if timeout > 0 {
		execCtx, execCancel = context.WithTimeout(w.ctx, timeout)
	} else if !work.Deadline.IsZero() {
		execCtx, execCancel = context.WithDeadline(w.ctx, work.Deadline)
	}
	if execCancel == nil {
		execCtx, execCancel = context.WithCancel(execCtx)
	}
	defer execCancel()

	// Register cancel func for per-task cancellation (Phase 5).
	w.cancelations.Store(work.RunID, execCancel)
	defer w.cancelations.Delete(work.RunID)

	// If auto-extend loses the run lock, cancel the handler immediately.
	rl.SetOnLost(func() {
		w.logger.Warn().String("run_id", work.RunID).Msg("worker: run lock lost, cancelling handler")
		execCancel()
	})

	// Durable cancel flag polling (fallback for missed Pub/Sub signals).
	// Polls every 10s alongside the heartbeat lifecycle.
	go func() {
		pollTicker := time.NewTicker(10 * time.Second)
		defer pollTicker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-pollTicker.C:
				if cancelled, _ := w.store.IsCancelRequested(w.ctx, work.RunID); cancelled {
					w.logger.Info().String("run_id", work.RunID).Msg("worker: cancel flag detected via polling")
					execCancel()
					return
				}
			}
		}
	}()

	// Inject metadata into the execution context.
	execCtx = types.WithJobID(execCtx, work.JobID)
	execCtx = types.WithRunID(execCtx, work.RunID)
	execCtx = types.WithAttempt(execCtx, work.Attempt)
	execCtx = types.WithTaskType(execCtx, work.TaskType)
	execCtx = types.WithPriority(execCtx, work.Priority)
	execCtx = types.WithOperationKey(execCtx, work.OperationKey)
	execCtx = types.WithNodeID(execCtx, w.nodeID)
	execCtx = types.WithSideEffectStore(execCtx, w.store)
	execCtx = types.WithSideEffectTTL(execCtx, int(w.store.Config().SideEffectTTL.Seconds()))
	execCtx = types.WithOperationEffectStore(execCtx, w.store)
	if work.Headers != nil {
		execCtx = types.WithHeaders(execCtx, work.Headers)
	}
	if def.RetryPolicy != nil {
		execCtx = types.WithMaxRetry(execCtx, def.RetryPolicy.MaxAttempts)
	}

	// Inject progress reporter for ReportProgress(ctx, data) API.
	progressFn := func(_ context.Context, data json.RawMessage) error {
		hbNow := time.Now()
		runMu.Lock()
		run.Progress = data
		run.LastHeartbeatAt = &hbNow
		snapshot := *run
		runMu.Unlock()
		_, err := w.store.SaveRun(w.ctx, &snapshot)
		return err
	}
	execCtx = types.WithProgressReporter(execCtx, progressFn)

	// Execute handler in an isolated goroutine.
	resCh := make(chan executeResult, 1)
	go func() {
		var execErr error
		var output json.RawMessage
		defer func() {
			if r := recover(); r != nil {
				execErr = &types.PanicError{Value: r, Stacktrace: string(debug.Stack())}
			}
			resCh <- executeResult{err: execErr, output: output}
		}()
		if def.HandlerWithResult != nil {
			h := def.HandlerWithResult
			for i := len(def.Middlewares) - 1; i >= 0; i-- {
				h = types.AdaptMiddleware(def.Middlewares[i])(h)
			}
			for i := len(w.globalMiddlewares) - 1; i >= 0; i-- {
				h = types.AdaptMiddleware(w.globalMiddlewares[i])(h)
			}
			output, execErr = h(execCtx, work.Payload)
		} else {
			h := def.Handler
			if len(def.Middlewares) > 0 {
				h = types.ChainMiddleware(h, def.Middlewares...)
			}
			if len(w.globalMiddlewares) > 0 {
				h = types.ChainMiddleware(h, w.globalMiddlewares...)
			}
			execErr = h(execCtx, work.Payload)
		}
	}()

	// Monitor multiple signals while the handler runs.
	select {
	case <-w.abort:
		// Graceful shutdown: cancel the handler and requeue the message
		// so another worker can pick it up immediately.
		execCancel()
		hbCancel() // Stop heartbeat and wait for goroutine to fully exit before writing terminal run fields.
		hbWg.Wait()
		bgCtx := context.Background()
		syncRetrySave(w.syncer, func() error { return w.store.ReenqueueWork(bgCtx, sm.TierName, &sm.Work, sm.Redeliveries) }, fmt.Sprintf("requeue %s", work.RunID))
		syncRetryAck(w.syncer, func() error { return w.store.AckMessage(bgCtx, sm.TierName, sm.StreamMsgID) }, sm.TierName, sm.StreamMsgID)
		w.logger.Info().String("job_id", work.JobID).String("run_id", work.RunID).Msg("worker: requeued on shutdown abort")

		finishedAt := time.Now()
		run.FinishedAt = &finishedAt
		run.Duration = finishedAt.Sub(now)
		run.Status = types.RunStatusFailed
		abortErr := "worker shutdown: task requeued"
		run.Error = &abortErr
		syncRetrySave(w.syncer, func() error { _, err := w.store.SaveRun(bgCtx, run); return err }, fmt.Sprintf("save run %s", run.ID))
		syncRetrySave(w.syncer, func() error { return w.store.SaveJobRun(bgCtx, run) }, fmt.Sprintf("save job run %s", run.ID))
		syncRetrySave(w.syncer, func() error { return w.store.DeleteRun(bgCtx, run.ID) }, fmt.Sprintf("delete run %s", run.ID))
		return

	case <-execCtx.Done():
		// Context cancelled: timeout, deadline, or per-task cancellation.
		// Wait briefly for the handler goroutine to notice and return.
		hbCancel() // Stop heartbeat and wait for goroutine to fully exit before writing terminal run fields.
		hbWg.Wait()
		select {
		case res := <-resCh:
			w.finishProcessMessage(sm, work, run, now, res)
		case <-time.After(5 * time.Second):
			finishedAt := time.Now()
			run.FinishedAt = &finishedAt
			run.Duration = finishedAt.Sub(now)
			run.Status = types.RunStatusFailed
			ctxErr := execCtx.Err().Error()
			run.Error = &ctxErr
			syncRetrySave(w.syncer, func() error { _, err := w.store.SaveRun(w.ctx, run); return err }, fmt.Sprintf("save run %s", run.ID))
			w.onFailure(sm, work, run, execCtx.Err())
			syncRetrySave(w.syncer, func() error { return w.store.SaveJobRun(w.ctx, run) }, fmt.Sprintf("save job run %s", run.ID))
			syncRetrySave(w.syncer, func() error { return w.store.DeleteRun(w.ctx, run.ID) }, fmt.Sprintf("delete run %s", run.ID))
		}
		return

	case res := <-resCh:
		// Normal completion (success or handler error).
		hbCancel() // Stop heartbeat and wait for goroutine to fully exit before writing terminal run fields.
		hbWg.Wait()
		w.finishProcessMessage(sm, work, run, now, res)
	}
}

// finishProcessMessage completes the run bookkeeping after handler execution.
func (w *Worker) finishProcessMessage(sm *streamMessage, work types.WorkMessage, run *types.JobRun, startedAt time.Time, res executeResult) {
	finishedAt := time.Now()
	run.FinishedAt = &finishedAt
	run.Duration = finishedAt.Sub(startedAt)

	if res.err == nil {
		run.Status = types.RunStatusSucceeded
		w.onSuccess(sm, work, run, res.output)
		return
	}

	// Check for control flow errors before treating as failure.
	ctrlClass := types.ClassifyControlFlow(res.err)
	switch ctrlClass {
	case types.ErrorClassSkip:
		// Skip: ack message without changing job status.
		run.Status = types.RunStatusSucceeded
		syncRetrySave(w.syncer, func() error { _, err := w.store.SaveRun(w.ctx, run); return err }, fmt.Sprintf("save run %s", run.ID))
		syncRetryAck(w.syncer, func() error { return w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID) }, sm.TierName, sm.StreamMsgID)
		syncRetrySave(w.syncer, func() error { return w.store.SaveJobRun(w.ctx, run) }, fmt.Sprintf("save job run %s", run.ID))
		syncRetrySave(w.syncer, func() error { return w.store.DeleteRun(w.ctx, run.ID) }, fmt.Sprintf("delete run %s", run.ID))
		w.logger.Info().String("job_id", work.JobID).String("run_id", work.RunID).Msg("worker: handler requested skip")
		return

	case types.ErrorClassRepeat:
		// Repeat: re-enqueue for immediate re-execution.
		// Requeue BEFORE ACK — if requeue fails, leave original for XAUTOCLAIM.
		run.Status = types.RunStatusSucceeded
		syncRetrySave(w.syncer, func() error { _, err := w.store.SaveRun(w.ctx, run); return err }, fmt.Sprintf("save run %s", run.ID))
		var repeatErr *types.RepeatError
		if errors.As(res.err, &repeatErr) && repeatErr.Delay > 0 {
			retryWork := work
			syncRetrySave(w.syncer, func() error {
				return w.store.AddDelayed(w.ctx, sm.TierName, &retryWork, time.Now().Add(repeatErr.Delay))
			}, fmt.Sprintf("add delayed repeat %s", work.JobID))
		} else {
			syncRetrySave(w.syncer, func() error { return w.store.ReenqueueWork(w.ctx, sm.TierName, &work, 0) }, fmt.Sprintf("requeue repeat %s", work.JobID))
		}
		syncRetryAck(w.syncer, func() error { return w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID) }, sm.TierName, sm.StreamMsgID)
		syncRetrySave(w.syncer, func() error { return w.store.SaveJobRun(w.ctx, run) }, fmt.Sprintf("save job run %s", run.ID))
		syncRetrySave(w.syncer, func() error { return w.store.DeleteRun(w.ctx, run.ID) }, fmt.Sprintf("delete run %s", run.ID))
		w.logger.Info().String("job_id", work.JobID).String("run_id", work.RunID).Msg("worker: handler requested repeat")
		return

	case types.ErrorClassPause:
		// Pause: transition job to paused state.
		run.Status = types.RunStatusFailed
		errStr := res.err.Error()
		run.Error = &errStr
		syncRetrySave(w.syncer, func() error { _, err := w.store.SaveRun(w.ctx, run); return err }, fmt.Sprintf("save run %s", run.ID))
		syncRetryAck(w.syncer, func() error { return w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID) }, sm.TierName, sm.StreamMsgID)
		syncRetrySave(w.syncer, func() error { return w.store.SaveJobRun(w.ctx, run) }, fmt.Sprintf("save job run %s", run.ID))
		syncRetrySave(w.syncer, func() error { return w.store.DeleteRun(w.ctx, run.ID) }, fmt.Sprintf("delete run %s", run.ID))
		w.pauseJob(work, res.err)
		return

	case types.ErrorClassContinueAsNew:
		// Continue-as-new: treat as success but embed the new input in the result
		// so the orchestrator can spawn a replacement workflow.
		run.Status = types.RunStatusSucceeded
		var contInput json.RawMessage
		var contErr *types.ContinueAsNewError
		if errors.As(res.err, &contErr) {
			contInput = contErr.Input
		}
		w.onSuccessContinueAsNew(sm, work, run, contInput)
		return
	}

	// Standard failure path.
	errStr := res.err.Error()
	run.Status = types.RunStatusFailed
	run.Error = &errStr
	syncRetrySave(w.syncer, func() error { _, err := w.store.SaveRun(w.ctx, run); return err }, fmt.Sprintf("save run %s", run.ID))
	w.onFailure(sm, work, run, res.err)
	syncRetrySave(w.syncer, func() error { return w.store.SaveJobRun(w.ctx, run) }, fmt.Sprintf("save job run %s", run.ID))
	syncRetrySave(w.syncer, func() error { return w.store.DeleteRun(w.ctx, run.ID) }, fmt.Sprintf("delete run %s", run.ID))
}

// pauseJob transitions a job to paused status.
func (w *Worker) pauseJob(work types.WorkMessage, execErr error) {
	job, rev, err := w.store.GetJob(w.ctx, work.JobID)
	if err != nil {
		w.logger.Error().String("job_id", work.JobID).Err(err).Msg("worker: failed to get job for pause")
		return
	}
	if w.staysEnded(job) {
		return
	}

	now := time.Now()
	errStr := execErr.Error()
	job.RunCount++
	job.LastError = &errStr

	var retryAfter time.Duration
	var pauseErr *types.PauseError
	if errors.As(execErr, &pauseErr) {
		retryAfter = pauseErr.RetryAfter
	}
	job.Pause(now, retryAfter)

	syncRetrySave(w.syncer, func() error { _, err := w.store.UpdateJob(w.ctx, job, rev); return err }, fmt.Sprintf("pause job %s", work.JobID))

	w.disp.PublishEvent(types.JobEvent{
		Type:      types.EventJobPaused,
		JobID:     work.JobID,
		RunID:     work.RunID,
		NodeID:    w.nodeID,
		TaskType:  work.TaskType,
		Attempt:   work.Attempt,
		Error:     &errStr,
		Timestamp: now,
	})
}

func (w *Worker) onSuccessContinueAsNew(sm *streamMessage, work types.WorkMessage, run *types.JobRun, contInput json.RawMessage) {
	batch := &store.CompletionBatch{
		Run: run,
		Event: types.JobEvent{
			Type:       types.EventJobCompleted,
			JobID:      work.JobID,
			RunID:      work.RunID,
			NodeID:     w.nodeID,
			TaskType:   work.TaskType,
			Attempt:    work.Attempt,
			Timestamp:  time.Now(),
			Standalone: sm.standalone,
		},
		Result: types.WorkResult{
			RunID:              work.RunID,
			JobID:              work.JobID,
			Success:            true,
			ContinueAsNewInput: contInput,
		},
		DailyStatField: "processed",
		AckTierName:    sm.TierName,
		AckMessageID:   sm.StreamMsgID,
		OperationKey:   work.OperationKey,
	}

	syncRetrySave(w.syncer, func() error { return w.store.CompleteRun(w.ctx, batch) }, fmt.Sprintf("complete run %s (continue-as-new)", run.ID))
	sm.completed = true

	// Retryable GetJob + UpdateJob as a unit to prevent stuck jobs.
	var hasWorkflow bool
	syncRetrySave(w.syncer, func() error {
		job, rev, err := w.store.GetJob(w.ctx, work.JobID)
		if err != nil {
			return fmt.Errorf("get job: %w", err)
		}
		if w.staysEnded(job) {
			return nil
		}
		now := time.Now()
		job.LastRunAt = &now
		job.Attempt = attemptAfterSuccess(job, work)
		job.RunCount++
		job.LastError = nil
		job.ConsecutiveErrors = 0
		job.Status = types.JobStatusCompleted
		job.CompletedAt = &now
		job.UpdatedAt = now
		if _, err := w.store.UpdateJob(w.ctx, job, rev); err != nil {
			return fmt.Errorf("update job: %w", err)
		}
		hasWorkflow = job.WorkflowID != nil
		return nil
	}, fmt.Sprintf("update job %s (continue-as-new)", work.JobID))

	if hasWorkflow {
		w.store.ExtendResultTTL(w.ctx, work.JobID, w.store.Config().WorkflowResultTTL)
	}

	w.logger.Info().String("job_id", work.JobID).String("run_id", work.RunID).Msg("worker: handler requested continue-as-new")
}

func (w *Worker) onSuccess(sm *streamMessage, work types.WorkMessage, run *types.JobRun, output json.RawMessage) {
	// Batched completion: SaveRun + SaveJobRun + DeleteRun + IncrDailyStat +
	// PublishEvent + PublishResult + AckMessage in a single Redis pipeline.
	batch := &store.CompletionBatch{
		Run: run,
		Event: types.JobEvent{
			Type:       types.EventJobCompleted,
			JobID:      work.JobID,
			RunID:      work.RunID,
			NodeID:     w.nodeID,
			TaskType:   work.TaskType,
			Attempt:    work.Attempt,
			Timestamp:  time.Now(),
			Standalone: sm.standalone,
		},
		Result: types.WorkResult{
			RunID:   work.RunID,
			JobID:   work.JobID,
			Success: true,
			Output:  output,
		},
		DailyStatField: "processed",
		AckTierName:    sm.TierName,
		AckMessageID:   sm.StreamMsgID,
		OperationKey:   work.OperationKey,
	}

	syncRetrySave(w.syncer, func() error { return w.store.CompleteRun(w.ctx, batch) }, fmt.Sprintf("complete run %s", run.ID))
	sm.completed = true

	// Update job state as a retryable unit. GetJob + UpdateJob are wrapped
	// together so a transient GetJob failure doesn't leave the job stuck in
	// "running" with no active run (which orphan recovery can't find).
	var jobForPostUpdate *types.Job
	syncRetrySave(w.syncer, func() error {
		job, rev, scheduled, err := w.store.GetJobAndIfScheduled(w.ctx, work.JobID)
		if err != nil {
			return fmt.Errorf("get job: %w", err)
		}
		if w.staysEnded(job) {
			w.logger.Info().String("job_id", work.JobID).String("run_id", work.RunID).String("status", string(job.Status)).Msg("worker: job ended while this run was going, leaving its status")
			return nil
		}
		now := time.Now()
		job.LastRunAt = &now
		job.Attempt = attemptAfterSuccess(job, work)
		job.RunCount++
		job.LastError = nil
		job.ConsecutiveErrors = 0
		job.UpdatedAt = now

		if scheduled {
			job.Status = types.JobStatusScheduled
		} else {
			job.Status = types.JobStatusCompleted
			job.CompletedAt = &now
		}
		if _, err := w.store.UpdateJob(w.ctx, job, rev); err != nil {
			return fmt.Errorf("update job: %w", err)
		}
		jobForPostUpdate = job
		return nil
	}, fmt.Sprintf("update job %s after success", work.JobID))

	// Extend result TTL for workflow-associated jobs so results survive
	// for the entire workflow execution, not just the default ResultTTL.
	if jobForPostUpdate != nil && jobForPostUpdate.WorkflowID != nil {
		w.store.ExtendResultTTL(w.ctx, work.JobID, w.store.Config().WorkflowResultTTL)
	}

	// Drain overlap buffer if the job has a buffer policy.
	if jobForPostUpdate != nil && hasOverlapBuffer(jobForPostUpdate) {
		go w.drainOverlapBuffer(work.JobID, jobForPostUpdate)
	}
}

func hasOverlapBuffer(job *types.Job) bool {
	return job.Schedule.OverlapPolicy == types.OverlapBufferOne || job.Schedule.OverlapPolicy == types.OverlapBufferAll
}

// drainOverlapBuffer dispatches a buffered job after a run completes.
func (w *Worker) drainOverlapBuffer(jobID string, job *types.Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// If another run is active, that one drains the buffer when it is done.
	if hasActive, _ := w.store.HasActiveRunForJob(ctx, jobID); hasActive {
		return
	}

	var found bool
	var err error

	switch job.Schedule.OverlapPolicy {
	case types.OverlapBufferOne:
		_, found, err = w.store.PopOverlapBuffer(ctx, jobID)
	case types.OverlapBufferAll:
		_, found, err = w.store.PopOverlapBufferAll(ctx, jobID)
	default:
		return
	}

	if err != nil || !found {
		return
	}

	if err := w.disp.Dispatch(ctx, job, job.Attempt); err != nil {
		w.logger.Error().Err(err).String("job_id", jobID).Msg("drainOverlapBuffer: dispatch failed")
	}
}

func (w *Worker) onFailure(sm *streamMessage, work types.WorkMessage, run *types.JobRun, execErr error) {
	w.store.IncrDailyStat(w.ctx, "failed")

	errStr := execErr.Error()
	errClass := types.ClassifyError(execErr)

	w.logger.Warn().String("job_id", work.JobID).String("run_id", work.RunID).Int("attempt", work.Attempt).String("error_class", types.GetErrorClassString(errClass)).String("error", errStr).Msg("worker: job execution failed")

	// Load job for retry policy check.
	job, rev, err := w.store.GetJob(w.ctx, work.JobID)
	if err != nil {
		w.logger.Error().String("job_id", work.JobID).Err(err).Msg("worker: failed to get job for failure update")
		// Don't ack — leave for XAUTOCLAIM.
		return
	}

	if w.staysEnded(job) {
		// The job was cancelled or went dead for good while this run was going. A
		// retry would bring it back to life.
		w.logger.Info().String("job_id", work.JobID).String("run_id", work.RunID).String("status", string(job.Status)).Msg("worker: job ended while this run was going, not retrying")
		syncRetryAck(w.syncer, func() error { return w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID) }, sm.TierName, sm.StreamMsgID)
		w.disp.PublishEvent(types.JobEvent{
			Type:       types.EventJobFailed,
			JobID:      work.JobID,
			RunID:      work.RunID,
			NodeID:     w.nodeID,
			TaskType:   work.TaskType,
			Attempt:    work.Attempt,
			Error:      &errStr,
			Timestamp:  time.Now(),
			Standalone: sm.standalone,
		})
		return
	}

	retryPolicy := job.RetryPolicy
	if retryPolicy == nil {
		// Fallback to handler-level retry policy before using defaults.
		if def, ok := w.registry.Get(work.TaskType); ok && def.RetryPolicy != nil {
			retryPolicy = def.RetryPolicy
		} else {
			retryPolicy = types.DefaultRetryPolicy()
		}
	}

	// Check NonRetryableErrors list: if the error message contains any of the
	// configured substrings, treat it as non-retryable.
	if len(retryPolicy.NonRetryableErrors) > 0 {
		for _, nre := range retryPolicy.NonRetryableErrors {
			if nre != "" && strings.Contains(errStr, nre) {
				errClass = types.ErrorClassNonRetryable
				break
			}
		}
	}

	maxAttempts := retryPolicy.MaxAttempts
	// Dynamic config override for maxAttempts.
	if w.dynCfg != nil {
		if ho, ok := w.dynCfg.GetHandler(string(work.TaskType)); ok && ho.MaxAttempts > 0 {
			maxAttempts = ho.MaxAttempts
		}
	}
	if job.DLQAfter != nil && *job.DLQAfter < maxAttempts {
		maxAttempts = *job.DLQAfter
	}

	now := time.Now()
	job.LastRunAt = &now
	job.LastError = &errStr
	job.Attempt = failedAttempts(job, work, maxAttempts) + 1
	job.RunCount++
	job.ConsecutiveErrors++
	job.UpdatedAt = now

	// Check if we should auto-pause instead of going to dead.
	shouldPause := retryPolicy.PauseAfterErrCount > 0 &&
		job.ConsecutiveErrors >= retryPolicy.PauseAfterErrCount

	switch {
	case errClass == types.ErrorClassNonRetryable:
		w.giveUp(sm, job, rev, errStr)

	case shouldPause:
		// Auto-pause: consecutive error threshold reached.
		job.Pause(now, retryPolicy.PauseRetryDelay)
		syncRetrySave(w.syncer, func() error { _, err := w.store.UpdateJob(w.ctx, job, rev); return err }, fmt.Sprintf("pause job %s", work.JobID))
		syncRetryAck(w.syncer, func() error { return w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID) }, sm.TierName, sm.StreamMsgID)
		w.logger.Warn().String("job_id", work.JobID).Int("consecutive_errors", job.ConsecutiveErrors).Msg("worker: auto-paused after consecutive errors")
		w.disp.PublishEvent(types.JobEvent{
			Type:      types.EventJobPaused,
			JobID:     work.JobID,
			RunID:     work.RunID,
			NodeID:    w.nodeID,
			TaskType:  work.TaskType,
			Attempt:   work.Attempt,
			Error:     &errStr,
			Timestamp: now,
		})
		return // skip the generic failed event below

	case job.Attempt >= maxAttempts:
		w.giveUp(sm, job, rev, errStr)

	case errClass == types.ErrorClassRateLimited:
		job.Status = types.JobStatusRetrying
		syncRetrySave(w.syncer, func() error { _, err := w.store.UpdateJob(w.ctx, job, rev); return err }, fmt.Sprintf("update job %s", work.JobID))
		retryAfter := types.GetRetryAfter(execErr)
		if retryAfter == 0 {
			retryAfter = retryPolicy.InitialDelay
		}
		retryWork := work
		retryWork.Attempt = job.Attempt
		// AddDelayed BEFORE ACK to prevent job loss if ACK succeeds but AddDelayed fails.
		syncRetrySave(w.syncer, func() error { return w.store.AddDelayed(w.ctx, sm.TierName, &retryWork, time.Now().Add(retryAfter)) }, fmt.Sprintf("add delayed retry %s", work.JobID))
		syncRetryAck(w.syncer, func() error { return w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID) }, sm.TierName, sm.StreamMsgID)

	default:
		job.Status = types.JobStatusRetrying
		syncRetrySave(w.syncer, func() error { _, err := w.store.UpdateJob(w.ctx, job, rev); return err }, fmt.Sprintf("update job %s", work.JobID))
		retryWork := work
		retryWork.Attempt = job.Attempt
		delay := calculateBackoff(retryWork.Attempt, retryPolicy)
		// AddDelayed BEFORE ACK to prevent job loss if ACK succeeds but AddDelayed fails.
		syncRetrySave(w.syncer, func() error { return w.store.AddDelayed(w.ctx, sm.TierName, &retryWork, time.Now().Add(delay)) }, fmt.Sprintf("add delayed retry %s", work.JobID))
		syncRetryAck(w.syncer, func() error { return w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID) }, sm.TierName, sm.StreamMsgID)
	}

	w.disp.PublishEvent(types.JobEvent{
		Type:       types.EventJobFailed,
		JobID:      work.JobID,
		RunID:      work.RunID,
		NodeID:     w.nodeID,
		TaskType:   work.TaskType,
		Attempt:    work.Attempt,
		Error:      &errStr,
		Timestamp:  now,
		Standalone: sm.standalone,
	})
}

// attemptAfterSuccess returns a job's Attempt once an execution has succeeded.
// The next firing of a recurring job starts counting anew.
func attemptAfterSuccess(job *types.Job, work types.WorkMessage) int {
	if job.Schedule.Type.IsRecurring() {
		return 0
	}
	return work.Attempt + 1
}

// failedAttempts returns how many attempts of work's execution had failed before
// this one.
//
// Versions before v0.2.5 counted a recurring job's attempts up over its whole
// life, so a message they dispatched may arrive with more attempts than an
// execution can have: a firing is given up as soon as it has used up
// maxAttempts, and only an execution resumed after a pause can be at the limit.
// Such a message goes by the failures since the job's last success instead.
func failedAttempts(job *types.Job, work types.WorkMessage, maxAttempts int) int {
	if !job.Schedule.Type.IsRecurring() {
		return work.Attempt
	}
	limit := maxAttempts
	if _, isFiring := work.Headers["x-dureq-firing-id"]; isFiring {
		limit = maxAttempts - 1
	}
	if work.Attempt > limit {
		return min(work.Attempt, job.ConsecutiveErrors)
	}
	return work.Attempt
}

// giveUp ends an execution that is not retried any more: it goes to the DLQ, and its
// job goes dead or, if its schedule is to be kept, waits for the next firing.
func (w *Worker) giveUp(sm *streamMessage, job *types.Job, rev uint64, errStr string) {
	if w.keepsSchedule(job) {
		failed := *job // with the attempts it has used up
		job.Status = types.JobStatusScheduled
		job.Attempt = 0
		syncRetrySave(w.syncer, func() error { _, err := w.store.UpdateJob(w.ctx, job, rev); return err }, fmt.Sprintf("update job %s", job.ID))
		w.disp.DispatchToDLQ(w.ctx, &failed, errStr)
		syncRetryAck(w.syncer, func() error { return w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID) }, sm.TierName, sm.StreamMsgID)
		if hasOverlapBuffer(job) {
			go w.drainOverlapBuffer(job.ID, job)
		}
		return
	}

	w.saveDead(job, rev)
	syncRetryAck(w.syncer, func() error { return w.store.AckMessage(w.ctx, sm.TierName, sm.StreamMsgID) }, sm.TierName, sm.StreamMsgID)
	w.disp.DispatchToDLQ(w.ctx, job, errStr)
}

// keepsSchedule reports whether a job stays on its schedule when a firing is given up.
func (w *Worker) keepsSchedule(job *types.Job) bool {
	if !job.Schedule.KeepOnFailure || !job.Schedule.Type.IsRecurring() {
		return false
	}
	_, _, err := w.store.GetSchedule(w.ctx, job.ID)
	return !errors.Is(err, types.ErrScheduleNotFound)
}

// saveDead stores the job as dead and then frees its unique key.
func (w *Worker) saveDead(job *types.Job, rev uint64) {
	job.Status = types.JobStatusDead
	saved := false
	syncRetrySave(w.syncer, func() error {
		if !saved {
			if _, err := w.store.UpdateJob(w.ctx, job, rev); err != nil {
				return err
			}
			saved = true
		}
		if job.UniqueKey == nil {
			return nil
		}
		return w.store.ReleaseEndedJobUniqueKey(w.ctx, job.ID)
	}, fmt.Sprintf("update job %s", job.ID))
}

// staysEnded reports whether the result of a leftover run must leave its job as it
// ended. A cancelled job stays cancelled. A dead one, whose leftover run may put it
// back to work, stays dead if another job holds its unique key by now or if it is
// recurring and has no schedule to go back to; otherwise it holds its key again
// afterwards. Workflow and batch jobs have no unique key and are left to the
// orchestrator.
func (w *Worker) staysEnded(job *types.Job) bool {
	if job.WorkflowID != nil || job.BatchID != nil {
		return false
	}
	switch job.Status {
	case types.JobStatusCancelled:
		return true
	case types.JobStatusDead:
		if job.Schedule.Type.IsRecurring() {
			if _, _, err := w.store.GetSchedule(w.ctx, job.ID); errors.Is(err, types.ErrScheduleNotFound) {
				return true
			}
		}
		if err := w.store.ClaimJobUniqueKey(w.ctx, job); err != nil {
			w.logger.Warn().String("job_id", job.ID).Err(err).Msg("worker: dead job cannot take its unique key back")
			return true
		}
	}
	return false
}

// cancelSubscribeLoop subscribes to the Redis Pub/Sub cancel channel and cancels
// the context for any matching in-flight run, enabling per-task cancellation.
func (w *Worker) cancelSubscribeLoop() {
	cancelCh := w.store.CancelChannel()
	subscribeCmd := w.store.Client().B().Subscribe().Channel(cancelCh).Build()

	err := w.store.Client().Receive(w.ctx, subscribeCmd, func(msg rueidis.PubSubMessage) {
		runID := msg.Message
		if cancel, loaded := w.cancelations.Load(runID); loaded {
			cancel.(context.CancelFunc)()
			w.logger.Info().String("run_id", runID).Msg("worker: task cancelled via pub/sub")
		}
	})
	if err != nil && w.ctx.Err() == nil {
		w.logger.Warn().Err(err).Msg("worker: cancel subscribe loop ended")
	}
}

// clusterHasTaskType checks if any other live node in the cluster handles the given task type.
func (w *Worker) clusterHasTaskType(tt types.TaskType) bool {
	nodes, err := w.store.ListNodes(w.ctx)
	if err != nil {
		return false
	}
	target := string(tt)
	for _, node := range nodes {
		if node.NodeID == w.nodeID {
			continue
		}
		for _, t := range node.TaskTypes {
			if t == target {
				return true
			}
			// Wildcard pattern match: "email:*" matches "email:send".
			if strings.HasSuffix(t, "*") && strings.HasPrefix(target, t[:len(t)-1]) {
				return true
			}
		}
	}
	return false
}

// calculateBackoff computes exponential backoff with jitter.
func calculateBackoff(attempt int, policy *types.RetryPolicy) time.Duration {
	// attempt is 1-based (first retry = 1), so subtract 1 for the exponent
	// so that the first retry uses InitialDelay * 2^0 = InitialDelay.
	exp := attempt - 1
	if exp < 0 {
		exp = 0
	}
	delay := float64(policy.InitialDelay) * math.Pow(policy.Multiplier, float64(exp))

	if time.Duration(delay) > policy.MaxDelay {
		delay = float64(policy.MaxDelay)
	}

	// Add jitter.
	if policy.Jitter > 0 {
		jitterRange := delay * policy.Jitter
		delay = delay - jitterRange + (rand.Float64() * 2 * jitterRange)
	}

	return time.Duration(delay)
}
