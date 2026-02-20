package store

import (
	"context"
)

// PauseQueue marks a tier/queue as paused. Workers will skip paused queues.
func (s *RedisStore) PauseQueue(ctx context.Context, tierName string) error {
	return s.rdb.Do(ctx, s.rdb.B().Set().Key(QueuePausedKey(s.prefix, tierName)).Value("1").Build()).Error()
}

// UnpauseQueue removes the pause flag from a tier/queue.
func (s *RedisStore) UnpauseQueue(ctx context.Context, tierName string) error {
	return s.rdb.Do(ctx, s.rdb.B().Del().Key(QueuePausedKey(s.prefix, tierName)).Build()).Error()
}

// IsQueuePaused checks whether a tier/queue is currently paused.
func (s *RedisStore) IsQueuePaused(ctx context.Context, tierName string) (bool, error) {
	val, err := s.rdb.Do(ctx, s.rdb.B().Exists().Key(QueuePausedKey(s.prefix, tierName)).Build()).AsInt64()
	if err != nil {
		return false, err
	}
	return val > 0, nil
}

// ListPausedQueues returns the names of all currently paused queues.
func (s *RedisStore) ListPausedQueues(ctx context.Context) ([]string, error) {
	var paused []string
	for _, tier := range s.cfg.Tiers {
		ok, err := s.IsQueuePaused(ctx, tier.Name)
		if err != nil {
			return nil, err
		}
		if ok {
			paused = append(paused, tier.Name)
		}
	}
	return paused, nil
}
