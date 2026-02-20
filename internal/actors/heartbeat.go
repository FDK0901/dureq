package actors

import (
	"context"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/anthdm/hollywood/actor"
	"github.com/redis/rueidis"
)

const heartbeatInterval = 5 * time.Second

// heartbeatMsg is a local tick message for the heartbeat timer.
type heartbeatMsg struct{}

// HeartbeatActor periodically saves NodeInfo to Redis so the monitoring API
// can discover live worker nodes. One instance runs per node.
type HeartbeatActor struct {
	nodeID    string
	store     *store.RedisStore
	taskTypes []string
	startedAt time.Time
	repeater  actor.SendRepeater
	logger    gochainedlog.Logger
}

// NewHeartbeatActor returns a Hollywood Producer that creates a HeartbeatActor.
func NewHeartbeatActor(nodeID string, s *store.RedisStore, taskTypes []string, logger gochainedlog.Logger) actor.Producer {
	if logger == nil {
		logger = chainedslog.NewSlog(chainedslog.NewSlogBase())
	}
	return func() actor.Receiver {
		return &HeartbeatActor{
			nodeID:    nodeID,
			store:     s,
			taskTypes: taskTypes,
			logger:    logger,
		}
	}
}

// Receive implements actor.Receiver.
func (h *HeartbeatActor) Receive(ctx *actor.Context) {
	switch ctx.Message().(type) {
	case actor.Started:
		h.onStarted(ctx)
	case actor.Stopped:
		h.onStopped()
	case *heartbeatMsg:
		h.onHeartbeat()
	case heartbeatMsg:
		h.onHeartbeat()
	}
}

func (h *HeartbeatActor) onStarted(ctx *actor.Context) {
	h.startedAt = time.Now()
	h.repeater = ctx.SendRepeat(ctx.PID(), heartbeatMsg{}, heartbeatInterval)

	// Send an immediate heartbeat so the node is visible right away.
	h.onHeartbeat()

	h.logger.Info().String("node_id", h.nodeID).Duration("interval", heartbeatInterval).Strs("task_types", h.taskTypes).Msg("heartbeat actor started")
}

func (h *HeartbeatActor) onStopped() {
	h.repeater.Stop()

	// Best-effort delete: remove the node key and the set membership so
	// other nodes see the departure immediately instead of waiting for TTL.
	bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodeKey := store.NodeKey(h.store.Prefix(), h.nodeID)
	nodesKey := store.NodesAllKey(h.store.Prefix())

	cmds := make(rueidis.Commands, 0, 2)
	cmds = append(cmds, h.store.Client().B().Del().Key(nodeKey).Build())
	cmds = append(cmds, h.store.Client().B().Srem().Key(nodesKey).Member(h.nodeID).Build())
	for _, resp := range h.store.Client().DoMulti(bgCtx, cmds...) {
		if err := resp.Error(); err != nil {
			h.logger.Warn().String("node_id", h.nodeID).Err(err).Msg("heartbeat actor: failed to delete node on stop")
			break
		}
	}

	h.logger.Info().String("node_id", h.nodeID).Msg("heartbeat actor stopped")
}

func (h *HeartbeatActor) onHeartbeat() {
	now := time.Now()
	nodeInfo := &types.NodeInfo{
		NodeID:        h.nodeID,
		StartedAt:     h.startedAt,
		LastHeartbeat: now,
		TaskTypes:     h.taskTypes,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := h.store.SaveNode(ctx, nodeInfo); err != nil {
		h.logger.Warn().String("node_id", h.nodeID).Err(err).Msg("heartbeat actor: failed to save node info")
	}
}
