package store

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"

	"github.com/FDK0901/dureq/pkg/types"
	"github.com/redis/rueidis"
)

// Group key patterns.

// GroupSetKey stores the sorted set of groups with their first-added timestamp.
func GroupSetKey(prefix string) string { return prefix + ":groups:active" }

// GroupMessagesKey stores the list of messages for a specific group.
func GroupMessagesKey(prefix, group string) string {
	return fmt.Sprintf("%s:group:%s:messages", prefix, group)
}

// GroupMetaKey stores group metadata (first_added, last_added timestamps).
func GroupMetaKey(prefix, group string) string {
	return fmt.Sprintf("%s:group:%s:meta", prefix, group)
}

// AddToGroup adds a message to a named group.
// Returns the current group size.
func (s *RedisStore) AddToGroup(ctx context.Context, group string, msg types.GroupMessage) (int64, error) {
	data, err := sonic.ConfigFastest.Marshal(msg)
	if err != nil {
		return 0, fmt.Errorf("marshal group message: %w", err)
	}

	now := float64(time.Now().UnixMilli())
	nowStr := time.Now().Format(time.RFC3339Nano)

	cmds := make(rueidis.Commands, 0, 4)
	// Add to messages list.
	cmds = append(cmds, s.rdb.B().Rpush().Key(GroupMessagesKey(s.prefix, group)).Element(string(data)).Build())
	// Track group in active set (score = first added timestamp, NX = don't overwrite).
	cmds = append(cmds, s.rdb.B().Zadd().Key(GroupSetKey(s.prefix)).Nx().ScoreMember().ScoreMember(now, group).Build())
	// Update group metadata.
	cmds = append(cmds, s.rdb.B().Hsetnx().Key(GroupMetaKey(s.prefix, group)).Field("first_added").Value(nowStr).Build())
	cmds = append(cmds, s.rdb.B().Hset().Key(GroupMetaKey(s.prefix, group)).FieldValue().FieldValue("last_added", nowStr).Build())

	results := s.rdb.DoMulti(ctx, cmds...)
	for _, r := range results {
		if err := r.Error(); err != nil {
			return 0, fmt.Errorf("add to group: %w", err)
		}
	}

	// Return current group size.
	size, err := s.rdb.Do(ctx, s.rdb.B().Llen().Key(GroupMessagesKey(s.prefix, group)).Build()).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("get group size: %w", err)
	}
	return size, nil
}

// FlushGroup atomically reads and deletes all messages from a group.
// Uses a single-key Lua script so that concurrent callers on different nodes
// are safe: only one caller will receive the messages; others get an empty result.
// Cleanup of the metadata hash and active set is done separately (idempotent).
func (s *RedisStore) FlushGroup(ctx context.Context, group string) ([]types.GroupMessage, error) {
	msgKey := GroupMessagesKey(s.prefix, group)

	// Atomic read+delete on the messages list (single-key Lua for cluster compat).
	raw, err := s.scriptFlushGroup.Exec(ctx, s.rdb,
		[]string{msgKey},
		[]string{},
	).AsStrSlice()
	if err != nil {
		// rueidis returns error for empty array from Lua; treat as no messages.
		return nil, nil
	}

	if len(raw) == 0 {
		return nil, nil
	}

	// Cleanup metadata and active set (idempotent — safe if these fail).
	metaKey := GroupMetaKey(s.prefix, group)
	setKey := GroupSetKey(s.prefix)
	cleanup := rueidis.Commands{
		s.rdb.B().Del().Key(metaKey).Build(),
		s.rdb.B().Zrem().Key(setKey).Member(group).Build(),
	}
	s.rdb.DoMulti(ctx, cleanup...) // best-effort; orphans are skipped by ListReadyGroups (LLEN=0)

	// Parse messages.
	messages := make([]types.GroupMessage, 0, len(raw))
	for _, r := range raw {
		var msg types.GroupMessage
		if sonic.ConfigFastest.Unmarshal([]byte(r), &msg) == nil {
			messages = append(messages, msg)
		}
	}
	return messages, nil
}

// ListReadyGroups returns groups that are ready to be flushed based on the given criteria.
func (s *RedisStore) ListReadyGroups(ctx context.Context, gracePeriod, maxDelay time.Duration, maxSize int) ([]string, error) {
	now := time.Now()
	var ready []string

	// Get all active groups with scores.
	groups, err := s.rdb.Do(ctx, s.rdb.B().Zrange().Key(GroupSetKey(s.prefix)).Min("0").Max("-1").Withscores().Build()).AsZScores()
	if err != nil {
		return nil, fmt.Errorf("list active groups: %w", err)
	}

	for _, g := range groups {
		group := g.Member
		firstAddedMs := int64(g.Score)
		firstAdded := time.UnixMilli(firstAddedMs)

		// Check max delay: if group has been waiting too long, flush it.
		if now.Sub(firstAdded) >= maxDelay {
			ready = append(ready, group)
			continue
		}

		// Check group size.
		if maxSize > 0 {
			size, err := s.rdb.Do(ctx, s.rdb.B().Llen().Key(GroupMessagesKey(s.prefix, group)).Build()).AsInt64()
			if err == nil && int(size) >= maxSize {
				ready = append(ready, group)
				continue
			}
		}

		// Check grace period: time since last message added.
		lastAdded, err := s.rdb.Do(ctx, s.rdb.B().Hget().Key(GroupMetaKey(s.prefix, group)).Field("last_added").Build()).ToString()
		if err == nil {
			if t, err := time.Parse(time.RFC3339Nano, lastAdded); err == nil {
				if now.Sub(t) >= gracePeriod {
					ready = append(ready, group)
				}
			}
		}
	}

	return ready, nil
}

// GetGroupSize returns the number of messages in a group.
func (s *RedisStore) GetGroupSize(ctx context.Context, group string) (int64, error) {
	return s.rdb.Do(ctx, s.rdb.B().Llen().Key(GroupMessagesKey(s.prefix, group)).Build()).AsInt64()
}

// ListGroups returns all active group names.
func (s *RedisStore) ListGroups(ctx context.Context) ([]string, error) {
	groups, err := s.rdb.Do(ctx, s.rdb.B().Zrange().Key(GroupSetKey(s.prefix)).Min("0").Max("-1").Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	return groups, nil
}
