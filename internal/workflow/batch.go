package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// maxBatchCASRetries is how often a change of a batch is applied anew because something
// else changed the batch meanwhile.
const maxBatchCASRetries = 10

// batchStep is what follows from a change of a batch, once the change is stored.
type batchStep struct {
	events   []types.EventType // of the batch
	progress bool              // publish the progress
	jobs     []*types.Job      // start these
}

// changeBatch applies change to a batch that is not terminal, stores it, and then does what
// follows from it. If the batch is changed meanwhile, change is applied to the new state.
// It returns nil if there is nothing to store.
func (o *Orchestrator) changeBatch(ctx context.Context, batchID string, change func(batch *types.BatchInstance, now time.Time) *batchStep) error {
	for range maxBatchCASRetries {
		batch, rev, err := o.store.GetBatch(ctx, batchID)
		if err != nil {
			o.logger.Error().String("batch_id", batchID).Err(err).Msg("batch orchestrator: failed to get batch")
			return err
		}
		if batch.Status.IsTerminal() {
			return nil
		}

		now := time.Now()
		step := change(batch, now)
		if step == nil {
			return nil
		}
		batch.UpdatedAt = now
		if _, err := o.store.UpdateBatch(ctx, batch, rev); err != nil {
			if errors.Is(err, store.ErrCASConflict) {
				continue
			}
			o.logger.Error().String("batch_id", batchID).Err(err).Msg("batch orchestrator: failed to save batch")
			return err
		}

		for _, event := range step.events {
			o.dispatcher.PublishEvent(types.JobEvent{Type: event, JobID: batchID, Timestamp: now})
		}
		if step.progress {
			o.publishBatchProgress(batch)
		}
		if len(step.jobs) > 0 {
			o.logger.Info().String("batch_id", batchID).Int("dispatched", len(step.jobs)).Int("running", batch.RunningItems).Int("remaining", len(batch.Definition.Items)-batch.NextChunkIndex).Msg("batch chunk dispatched")
		}
		return o.startBatchJobs(ctx, batchID, step.jobs)
	}
	return fmt.Errorf("batch %s: too many concurrent changes", batchID)
}

// startBatchJobs creates and dispatches jobs that are reserved in a batch. One that exists is
// left to whoever created it.
func (o *Orchestrator) startBatchJobs(ctx context.Context, batchID string, jobs []*types.Job) error {
	for _, job := range jobs {
		if err := o.startJob(ctx, job); err != nil {
			delete(o.checked, batchID) // the next event of the batch has it looked at again
			o.logger.Error().String("batch_id", batchID).String("job_id", job.ID).Err(err).Msg("batch: failed to start job")
			return err
		}
	}
	return nil
}

// startJob creates and dispatches a job that is reserved. One that exists is left to
// whoever created it.
func (o *Orchestrator) startJob(ctx context.Context, job *types.Job) error {
	if _, err := o.store.CreateJob(ctx, job); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil
		}
		return err
	}
	if err := o.dispatcher.Dispatch(ctx, job, 0); err != nil {
		o.store.DeleteJob(ctx, job.ID)
		return err
	}
	return nil
}

// ensureBatchJobs starts the jobs of a batch that are reserved but do not exist: whoever
// reserved them stopped before they could start them. It does so at the first event of the
// batch that this orchestrator sees, which includes one that its predecessor did not finish.
func (o *Orchestrator) ensureBatchJobs(ctx context.Context, batchID string) error {
	if _, ok := o.checked[batchID]; ok {
		return nil
	}
	batch, _, err := o.store.GetBatch(ctx, batchID)
	if err != nil {
		return err
	}
	if batch.Status.IsTerminal() {
		return nil
	}

	now := time.Now()
	var jobs []*types.Job
	if st := batch.OnetimeState; st != nil && st.Status == types.JobStatusRunning && st.JobID != "" {
		jobs = append(jobs, batchOnetimeJob(batch, now))
	}
	for i := range batch.Definition.Items {
		item := &batch.Definition.Items[i]
		if state := batch.ItemStates[item.ID]; state.Status == types.JobStatusRunning && state.JobID != "" {
			jobs = append(jobs, batchItemJob(batch, item, now))
		}
	}
	missing, err := o.missingJobs(ctx, jobs)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		o.logger.Debug().String("batch_id", batchID).Int("jobs", len(missing)).Msg("batch: starting jobs that are reserved but not started")
	}
	if err := o.startBatchJobs(ctx, batchID, missing); err != nil {
		return err
	}
	o.markChecked(batchID)
	return nil
}

// missingJobs returns the jobs that do not exist.
func (o *Orchestrator) missingJobs(ctx context.Context, jobs []*types.Job) ([]*types.Job, error) {
	ids := make([]string, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
	}
	exist, err := o.store.JobsExist(ctx, ids)
	if err != nil {
		return nil, err
	}
	var missing []*types.Job
	for i, job := range jobs {
		if !exist[i] {
			missing = append(missing, job)
		}
	}
	return missing, nil
}

// markChecked remembers that the reserved jobs of a batch or workflow have been looked at.
func (o *Orchestrator) markChecked(id string) {
	const maxChecked = 10_000
	if len(o.checked) >= maxChecked {
		clear(o.checked) // looking at one again does no harm
	}
	o.checked[id] = struct{}{}
}

// staleJob reports whether an event is not about the job that the state waits for: its work
// has ended, or goes on as another job.
func staleJob(status types.JobStatus, jobID string, event types.JobEvent) bool {
	return status.IsTerminal() || jobID != "" && jobID != event.JobID
}

func (o *Orchestrator) handleBatchJobCompleted(ctx context.Context, event types.JobEvent, job *types.Job) error {
	batchID := *job.BatchID
	if err := o.ensureBatchJobs(ctx, batchID); err != nil {
		return err
	}
	result, resultErr := o.store.GetResult(ctx, event.JobID)
	if resultErr != nil {
		result = nil
	}

	switch role(job) {
	case "onetime":
		// Onetime preprocessing completed — capture result and start item dispatch.
		return o.changeBatch(ctx, batchID, func(batch *types.BatchInstance, now time.Time) *batchStep {
			if st := batch.OnetimeState; st != nil {
				if staleJob(st.Status, st.JobID, event) {
					return nil
				}
				st.Status = types.JobStatusCompleted
				st.FinishedAt = &now
				if result != nil {
					st.ResultData = result.Output
				}
			}
			o.logger.Info().String("batch_id", batchID).Msg("batch onetime completed")
			batch.Status = types.WorkflowStatusRunning
			return &batchStep{
				events: []types.EventType{types.EventBatchOnetimeCompleted},
				jobs:   ReserveBatchChunk(batch, now),
			}
		})

	case "item":
		itemID := *job.BatchItem
		o.logger.Debug().String("batch_id", batchID).String("item_id", itemID).Msg("batch item completed")
		return o.changeBatch(ctx, batchID, func(batch *types.BatchInstance, now time.Time) *batchStep {
			state, ok := batch.ItemStates[itemID]
			if !ok || staleJob(state.Status, state.JobID, event) {
				return o.finishIfDone(batch, now)
			}
			state.Status = types.JobStatusCompleted
			state.FinishedAt = &now
			batch.ItemStates[itemID] = state
			batch.CompletedItems++
			if batch.RunningItems > 0 {
				batch.RunningItems--
			}

			// The results are complete by the time the batch is done.
			itemResult := &types.BatchItemResult{BatchID: batchID, ItemID: itemID, Success: true}
			if result != nil {
				itemResult.Output = result.Output
			}
			o.store.SaveBatchItemResult(ctx, itemResult)

			return o.advanceBatch(batch, now, types.EventBatchItemCompleted)
		})
	}
	return nil
}

func (o *Orchestrator) handleBatchJobFailed(ctx context.Context, event types.JobEvent, job *types.Job) error {
	batchID := *job.BatchID
	if err := o.ensureBatchJobs(ctx, batchID); err != nil {
		return err
	}
	errStr := "execution failed"
	if event.Error != nil {
		errStr = *event.Error
	}

	switch role(job) {
	case "onetime":
		// Onetime failure → entire batch fails.
		return o.changeBatch(ctx, batchID, func(batch *types.BatchInstance, now time.Time) *batchStep {
			if st := batch.OnetimeState; st != nil {
				if staleJob(st.Status, st.JobID, event) {
					return nil
				}
				st.Status = types.JobStatusFailed
				st.FinishedAt = &now
				st.Error = &errStr
			}
			o.logger.Warn().String("batch_id", batchID).Msg("batch onetime failed — failing batch")
			batch.Status = types.WorkflowStatusFailed
			batch.CompletedAt = &now
			return &batchStep{events: []types.EventType{types.EventBatchOnetimeFailed, types.EventBatchFailed}}
		})

	case "item":
		itemID := *job.BatchItem
		return o.changeBatch(ctx, batchID, func(batch *types.BatchInstance, now time.Time) *batchStep {
			state, ok := batch.ItemStates[itemID]
			if !ok || staleJob(state.Status, state.JobID, event) {
				return o.finishIfDone(batch, now)
			}
			o.logger.Warn().String("batch_id", batchID).String("item_id", itemID).Msg("batch item failed")
			state.Status = types.JobStatusFailed
			state.FinishedAt = &now
			state.Error = &errStr
			batch.ItemStates[itemID] = state
			batch.FailedItems++
			if batch.RunningItems > 0 {
				batch.RunningItems--
			}
			o.store.SaveBatchItemResult(ctx, &types.BatchItemResult{BatchID: batchID, ItemID: itemID, Error: &errStr})

			if batch.Definition.FailurePolicy == types.BatchFailFast {
				// Fail-fast: stop the batch immediately.
				o.logger.Info().String("batch_id", batchID).Msg("batch failed (fail_fast)")
				batch.Status = types.WorkflowStatusFailed
				batch.CompletedAt = &now
				return &batchStep{
					events:   []types.EventType{types.EventBatchItemFailed, types.EventBatchFailed},
					progress: true,
				}
			}
			return o.advanceBatch(batch, now, types.EventBatchItemFailed)
		})
	}
	return nil
}

// updateBatchItemRetrying updates a batch item's status to retrying so the UI
// reflects that the item is being retried rather than stuck in running.
func (o *Orchestrator) updateBatchItemRetrying(ctx context.Context, job *types.Job) {
	if job.BatchID == nil || job.BatchItem == nil {
		return
	}
	itemID := *job.BatchItem
	o.changeBatch(ctx, *job.BatchID, func(batch *types.BatchInstance, now time.Time) *batchStep {
		state, ok := batch.ItemStates[itemID]
		if !ok || state.Status != types.JobStatusRunning || state.JobID != job.ID {
			return nil
		}
		state.Status = types.JobStatusRetrying
		batch.ItemStates[itemID] = state
		return &batchStep{progress: true}
	})
}

// handleBatchRetry goes on with a batch that is retried.
func (o *Orchestrator) handleBatchRetry(ctx context.Context, event types.JobEvent) error {
	batchID := event.JobID
	if batchID == "" {
		return nil
	}
	if err := o.ensureBatchJobs(ctx, batchID); err != nil {
		return err
	}
	ended, lost, err := o.endedItems(ctx, batchID)
	if err != nil {
		return err
	}

	err = o.changeBatch(ctx, batchID, func(batch *types.BatchInstance, now time.Time) *batchStep {
		for itemID, jobID := range lost {
			if state := batch.ItemStates[itemID]; state.JobID == jobID {
				resetItem(&state)
				batch.ItemStates[itemID] = state
				batch.NextChunkIndex = 0
			}
		}
		countItems(batch)
		if st := batch.OnetimeState; st != nil && st.Status == types.JobStatusRunning {
			return nil // this event has been handled before
		}
		jobs := ReserveBatch(batch, now)
		if len(jobs) == 0 {
			return o.finishIfDone(batch, now)
		}
		o.logger.Info().String("batch_id", batchID).Msg("orchestrator: dispatching batch retry")
		return &batchStep{jobs: jobs}
	})
	if err != nil {
		return err
	}

	// What ended while the batch had ended, too, was not counted.
	for _, job := range ended {
		if err := o.replayEnd(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

// endedItems returns the jobs of items that have ended although the items count as running,
// and the items, with their job IDs, whose jobs are gone or cancelled.
func (o *Orchestrator) endedItems(ctx context.Context, batchID string) (ended []*types.Job, lost map[string]string, err error) {
	batch, _, err := o.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, nil, err
	}
	lost = make(map[string]string)
	for itemID, state := range batch.ItemStates {
		if state.JobID == "" || state.Status != types.JobStatusRunning && state.Status != types.JobStatusRetrying {
			continue
		}
		job, _, err := o.store.GetJob(ctx, state.JobID)
		switch {
		case errors.Is(err, types.ErrJobNotFound) || err == nil && job.Status == types.JobStatusCancelled:
			lost[itemID] = state.JobID
		case err != nil:
			return nil, nil, err
		case job.Status.IsTerminal():
			ended = append(ended, job)
		}
	}
	return ended, lost, nil
}

// replayEnd handles the end of a job whose event came while its workflow or batch had ended.
func (o *Orchestrator) replayEnd(ctx context.Context, job *types.Job) error {
	event := types.JobEvent{JobID: job.ID, TaskType: job.TaskType, Error: job.LastError, Timestamp: time.Now()}
	if job.Status == types.JobStatusCompleted {
		return o.handleJobCompleted(ctx, event)
	}
	return o.handleJobFailed(ctx, event)
}

// advanceBatch goes on with a batch after an item: with the next chunk, or to its end.
func (o *Orchestrator) advanceBatch(batch *types.BatchInstance, now time.Time, itemEvent types.EventType) *batchStep {
	step := &batchStep{progress: true}
	if o.isBatchDone(batch) {
		step = o.finalizeBatch(batch, now)
	} else {
		step.jobs = ReserveBatchChunk(batch, now)
	}
	step.events = append([]types.EventType{itemEvent}, step.events...)
	return step
}

// finishIfDone ends a batch whose last item came to its end earlier.
func (o *Orchestrator) finishIfDone(batch *types.BatchInstance, now time.Time) *batchStep {
	if !o.isBatchDone(batch) {
		return nil
	}
	return o.finalizeBatch(batch, now)
}

// isBatchDone returns true if all items have been completed or failed.
func (o *Orchestrator) isBatchDone(batch *types.BatchInstance) bool {
	return batch.CompletedItems+batch.FailedItems >= batch.TotalItems
}

// finalizeBatch marks the batch as completed or failed (with auto-retry check).
func (o *Orchestrator) finalizeBatch(batch *types.BatchInstance, now time.Time) *batchStep {
	step := &batchStep{progress: true}
	if batch.FailedItems > 0 && batch.Definition.RetryPolicy != nil && batch.Attempt < batch.Definition.RetryPolicy.MaxAttempts {
		ResetBatch(batch, true, now)
		// Recalculate deadline if ExecutionTimeout is set.
		if batch.Definition.ExecutionTimeout != nil {
			deadline := now.Add(batch.Definition.ExecutionTimeout.Std())
			batch.Deadline = &deadline
		}
		step.jobs = ReserveBatch(batch, now)
		o.logger.Info().String("batch_id", batch.ID).Int("attempt", batch.Attempt).Msg("batch retrying")
		return step
	}

	batch.CompletedAt = &now
	if batch.FailedItems > 0 {
		batch.Status = types.WorkflowStatusFailed
		step.events = []types.EventType{types.EventBatchFailed}
		o.logger.Info().String("batch_id", batch.ID).Int("completed", batch.CompletedItems).Int("failed", batch.FailedItems).Msg("batch finished with failures")
	} else {
		batch.Status = types.WorkflowStatusCompleted
		step.events = []types.EventType{types.EventBatchCompleted}
		o.logger.Info().String("batch_id", batch.ID).Int("completed", batch.CompletedItems).Msg("batch completed successfully")
	}
	return step
}

// role returns the role of a job in its batch.
func role(job *types.Job) string {
	if job.BatchRole == nil || *job.BatchRole == "item" && job.BatchItem == nil {
		return ""
	}
	return *job.BatchRole
}
