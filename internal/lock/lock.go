package lock

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/redis/rueidis"
)

// Locker implements distributed locking using Redis SET NX EX + Lua scripts.
type Locker struct {
	rdb                rueidis.Client
	prefix             string
	lockTTL            time.Duration
	autoExtendInterval time.Duration
	logger             gochainedlog.Logger

	unlockScript *rueidis.Lua
	extendScript *rueidis.Lua
}

// LockerConfig holds configuration for the locker.
type LockerConfig struct {
	RDB                rueidis.Client
	Prefix             string
	LockTTL            time.Duration // Default: 30s
	AutoExtendInterval time.Duration // Default: 0 (no auto-extend)
	Logger             gochainedlog.Logger
}

// NewLocker creates a new Redis-based distributed locker.
func NewLocker(cfg LockerConfig) *Locker {
	if cfg.Logger == nil {
		cfg.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	if cfg.LockTTL == 0 {
		cfg.LockTTL = 30 * time.Second
	}
	return &Locker{
		rdb:                cfg.RDB,
		prefix:             cfg.Prefix,
		lockTTL:            cfg.LockTTL,
		autoExtendInterval: cfg.AutoExtendInterval,
		logger:             cfg.Logger,
		unlockScript:       rueidis.NewLuaScript(store.LuaUnlockIfOwnerScript()),
		extendScript:       rueidis.NewLuaScript(store.LuaExtendLockScript()),
	}
}

// Lock acquires a distributed lock for the given key.
// Returns a RedisLock that must be Unlock'd when done.
func (l *Locker) Lock(ctx context.Context, key, owner string) (*RedisLock, error) {
	fullKey := store.LockKey(l.prefix, key)

	err := l.rdb.Do(ctx, l.rdb.B().Set().Key(fullKey).Value(owner).Nx().Ex(l.lockTTL).Build()).Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, types.ErrLockFailed
		}
		return nil, err
	}

	rl := &RedisLock{
		rdb:          l.rdb,
		key:          fullKey,
		owner:        owner,
		lockTTL:      l.lockTTL,
		done:         make(chan struct{}),
		logger:       l.logger,
		unlockScript: l.unlockScript,
		extendScript: l.extendScript,
	}

	if l.autoExtendInterval > 0 {
		go rl.autoExtend(l.autoExtendInterval)
	}

	return rl, nil
}

// RedisLock represents a held distributed lock.
type RedisLock struct {
	rdb          rueidis.Client
	key          string
	owner        string
	lockTTL      time.Duration
	done         chan struct{}
	closeOnce    sync.Once
	logger       gochainedlog.Logger
	unlockScript *rueidis.Lua
	extendScript *rueidis.Lua

	onLost   func() // called once when auto-extend detects lock loss
	lostOnce sync.Once
}

// SetOnLost registers a callback that fires (exactly once) when autoExtend
// discovers the lock is no longer held. Typical use: cancel the handler context.
func (rl *RedisLock) SetOnLost(fn func()) {
	rl.onLost = fn
}

// Unlock releases the distributed lock, but only if we still own it.
// Safe to call multiple times; subsequent calls are no-ops.
func (rl *RedisLock) Unlock(_ context.Context) error {
	rl.closeOnce.Do(func() { close(rl.done) })

	result, err := rl.unlockScript.Exec(context.Background(), rl.rdb,
		[]string{rl.key},
		[]string{rl.owner},
	).AsInt64()
	if err != nil {
		return err
	}
	if result == 0 {
		return types.ErrLockNotHeld
	}
	return nil
}

// autoExtend periodically refreshes the lock TTL to prevent expiry.
func (rl *RedisLock) autoExtend(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-rl.done:
			return
		case <-ticker.C:
			ttlSec := strconv.Itoa(int(rl.lockTTL.Seconds()))
			result, err := rl.extendScript.Exec(context.Background(), rl.rdb,
				[]string{rl.key},
				[]string{rl.owner, ttlSec},
			).AsInt64()
			if err != nil {
				rl.logger.Warn().String("key", rl.key).Err(err).Msg("lock auto-extend: script failed")
				rl.invokeLost()
				return
			}
			if result == 0 {
				rl.logger.Warn().String("key", rl.key).Msg("lock auto-extend: no longer owner")
				rl.invokeLost()
				return
			}
		}
	}
}

// invokeLost fires the OnLost callback exactly once.
func (rl *RedisLock) invokeLost() {
	if rl.onLost != nil {
		rl.lostOnce.Do(rl.onLost)
	}
}
