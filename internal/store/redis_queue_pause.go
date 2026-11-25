package store

import (
	"context"

	"github.com/redis/rueidis"
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

// FetchGate reports whether a node is draining, and for each configured tier whether it is
// paused. What cannot be told counts as neither.
func (s *RedisStore) FetchGate(ctx context.Context, nodeID string) (draining bool, paused []bool) {
	cmds := make(rueidis.Commands, 0, 1+len(s.cfg.Tiers))
	cmds = append(cmds, s.rdb.B().Get().Key(NodeDrainKey(s.prefix, nodeID)).Build())
	for _, tier := range s.cfg.Tiers {
		cmds = append(cmds, s.rdb.B().Exists().Key(QueuePausedKey(s.prefix, tier.Name)).Build())
	}
	results := s.rdb.DoMulti(ctx, cmds...)

	val, _ := results[0].ToString()
	paused = make([]bool, len(s.cfg.Tiers))
	for i, r := range results[1:] {
		n, _ := r.AsInt64()
		paused[i] = n > 0
	}
	return val == "1", paused
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
