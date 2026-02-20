package election

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/redis/rueidis"
)

// Elector implements leader election using Redis SET NX EX + Lua refresh.
//
// Algorithm:
//  1. Attempt SET leader {nodeID} NX EX {ttl} — atomic, fails if key exists.
//  2. If elected: refresh the key every TTL/3 using a Lua script.
//  3. If not elected: retry step 1 at TTL/3 intervals.
//  4. On graceful shutdown: delete the key for fast failover.
type Elector struct {
	rdb    rueidis.Client
	prefix string
	nodeID string
	ttl    time.Duration
	logger gochainedlog.Logger

	refreshScript *rueidis.Lua

	mu       sync.RWMutex
	isLeader bool
	leaderID string

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	// OnElected is called when this node becomes the leader.
	OnElected func()
	// OnDemoted is called when this node loses leadership.
	OnDemoted func()
}

// ElectorConfig holds configuration for the elector.
type ElectorConfig struct {
	RDB    rueidis.Client
	Prefix string
	NodeID string
	TTL    time.Duration // Default: 10s
	Logger gochainedlog.Logger
}

// NewElector creates a new Redis-based elector.
func NewElector(cfg ElectorConfig) *Elector {
	if cfg.TTL == 0 {
		cfg.TTL = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	if cfg.NodeID == "" {
		cfg.NodeID = generateNodeID()
	}

	return &Elector{
		rdb:           cfg.RDB,
		prefix:        cfg.Prefix,
		nodeID:        cfg.NodeID,
		ttl:           cfg.TTL,
		logger:        cfg.Logger,
		refreshScript: rueidis.NewLuaScript(store.LuaLeaderRefreshScript()),
		done:          make(chan struct{}),
	}
}

// Start begins the election loop. Non-blocking — spawns a goroutine.
func (e *Elector) Start(ctx context.Context) error {
	e.ctx, e.cancel = context.WithCancel(ctx)
	go e.electionLoop()
	return nil
}

// Stop gracefully stops the elector and releases leadership if held.
func (e *Elector) Stop() error {
	e.cancel()
	<-e.done

	e.mu.RLock()
	wasLeader := e.isLeader
	e.mu.RUnlock()

	if wasLeader {
		if err := e.rdb.Do(context.Background(), e.rdb.B().Del().Key(e.leaderKeyName()).Build()).Error(); err != nil {
			e.logger.Warn().Err(err).Msg("failed to delete leader key on shutdown")
		}
	}

	e.mu.Lock()
	e.isLeader = false
	e.leaderID = ""
	e.mu.Unlock()
	return nil
}

// IsLeader returns nil if this node is the leader, or ErrNotLeader otherwise.
func (e *Elector) IsLeader(_ context.Context) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.isLeader {
		return nil
	}
	return types.ErrNotLeader
}

// LeaderID returns the current leader's node ID.
func (e *Elector) LeaderID() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leaderID
}

// NodeID returns this elector's node ID.
func (e *Elector) NodeID() string {
	return e.nodeID
}

func (e *Elector) electionLoop() {
	defer close(e.done)

	for e.ctx.Err() == nil {
		if e.tryCampaign() {
			e.refreshLoop()
			e.demote("")
		}

		select {
		case <-e.ctx.Done():
			return
		case <-time.After(e.ttl / 3):
		}
	}
}

// tryCampaign attempts to become the leader using SET NX EX.
func (e *Elector) tryCampaign() bool {
	key := e.leaderKeyName()
	err := e.rdb.Do(e.ctx, e.rdb.B().Set().Key(key).Value(e.nodeID).Nx().Ex(e.ttl).Build()).Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			// Key already exists — another node is leader.
			currentLeader, err := e.rdb.Do(e.ctx, e.rdb.B().Get().Key(key).Build()).ToString()
			if err == nil {
				e.mu.Lock()
				e.leaderID = currentLeader
				e.mu.Unlock()
				e.logger.Debug().String("leader", currentLeader).Msg("election: another node is leader")
			}
		}
		return false
	}

	e.promote()
	return true
}

// refreshLoop refreshes the leader key at TTL/3 intervals using Lua.
func (e *Elector) refreshLoop() {
	ticker := time.NewTicker(e.ttl / 3)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			ttlSec := strconv.Itoa(int(e.ttl.Seconds()))
			result, err := e.refreshScript.Exec(e.ctx, e.rdb,
				[]string{e.leaderKeyName()},
				[]string{e.nodeID, ttlSec},
			).AsInt64()
			if err != nil {
				e.logger.Warn().Err(err).Msg("election: failed to refresh leader key")
				return
			}
			if result == 0 {
				e.logger.Info().Msg("election: lost leadership (refresh rejected)")
				return
			}
		}
	}
}

func (e *Elector) promote() {
	e.mu.Lock()
	e.isLeader = true
	e.leaderID = e.nodeID
	e.mu.Unlock()
	e.logger.Info().String("node_id", e.nodeID).Msg("election: promoted to leader")

	if e.OnElected != nil {
		e.OnElected()
	}
}

func (e *Elector) demote(newLeader string) {
	e.mu.Lock()
	wasLeader := e.isLeader
	e.isLeader = false
	e.leaderID = newLeader
	e.mu.Unlock()

	if wasLeader {
		e.logger.Info().String("node_id", e.nodeID).String("new_leader", newLeader).Msg("election: demoted from leader")
		if e.OnDemoted != nil {
			e.OnDemoted()
		}
	}
}

func (e *Elector) leaderKeyName() string {
	return store.ElectionLeaderKey(e.prefix)
}

// GenerateNodeID creates a unique node ID from hostname + random bytes.
func GenerateNodeID() string {
	return generateNodeID()
}

func generateNodeID() string {
	hostname, _ := os.Hostname()
	bs := make([]byte, 8)
	rand.Read(bs)
	all := fmt.Sprintf("%s-%x", hostname, bs)
	all = strings.ReplaceAll(all, ".", "-")
	all = strings.ReplaceAll(all, ">", "-")
	all = strings.ReplaceAll(all, "*", "-")
	all = strings.ReplaceAll(all, "/", "-")
	all = strings.ReplaceAll(all, "\\", "-")
	return strings.ReplaceAll(all, " ", "-")
}
