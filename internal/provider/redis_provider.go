package provider

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/anthdm/hollywood/actor"
	"github.com/anthdm/hollywood/cluster"
	"github.com/bytedance/sonic"
	"github.com/redis/rueidis"
)

const (
	defaultTTL          = 15 * time.Second
	defaultPollInterval = 3 * time.Second
)

// RedisProviderConfig holds the configuration for the Redis-backed cluster provider.
type RedisProviderConfig struct {
	RedisClient  rueidis.Client
	KeyPrefix    string
	TTL          time.Duration
	PollInterval time.Duration
}

// refreshMsg is sent periodically to trigger member refresh.
type refreshMsg struct{}

// memberLeaveMsg is sent when a remote member becomes unreachable.
type memberLeaveMsg struct {
	ListenAddr string
}

// memberInfo is the JSON-serializable representation of a cluster member.
type memberInfo struct {
	ID     string   `json:"id"`
	Host   string   `json:"host"`
	Region string   `json:"region"`
	Kinds  []string `json:"kinds"`
}

// RedisProvider implements Hollywood's cluster provider interface using Redis
// for member discovery. It replaces mDNS/Zeroconf-based discovery with
// Redis key expiration (TTL) for automatic dead member cleanup.
type RedisProvider struct {
	config      RedisProviderConfig
	cluster     *cluster.Cluster
	members     *cluster.MemberSet
	refresher   actor.SendRepeater
	eventSubPID *actor.PID
	pid         *actor.PID
}

// NewRedisProvider returns a cluster.Producer that creates a RedisProvider actor.
// The returned Producer conforms to Hollywood's cluster.Producer type:
//
//	func(c *cluster.Cluster) actor.Producer
func NewRedisProvider(config RedisProviderConfig) cluster.Producer {
	if config.TTL == 0 {
		config.TTL = defaultTTL
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultPollInterval
	}
	return func(c *cluster.Cluster) actor.Producer {
		return func() actor.Receiver {
			return &RedisProvider{
				config:  config,
				cluster: c,
				members: cluster.NewMemberSet(),
			}
		}
	}
}

// Receive implements actor.Receiver.
func (p *RedisProvider) Receive(c *actor.Context) {
	switch msg := c.Message().(type) {
	case actor.Started:
		p.pid = c.PID()
		p.members.Add(p.cluster.Member())

		// Register self in Redis.
		if err := p.registerSelf(); err != nil {
			slog.Error("redis provider: failed to register self", "err", err)
		}

		// Send initial member list to cluster agent.
		p.sendMembersToAgent()

		// Start periodic refresh.
		p.refresher = c.SendRepeat(c.PID(), refreshMsg{}, p.config.PollInterval)

		// Subscribe to engine events for remote unreachable detection.
		p.eventSubPID = c.SpawnChildFunc(p.handleEventStream, "event")
		p.cluster.Engine().Subscribe(p.eventSubPID)

	case actor.Stopped:
		p.refresher.Stop()
		p.cluster.Engine().Unsubscribe(p.eventSubPID)
		p.deregisterSelf()

	case refreshMsg:
		p.handleRefresh()

	case *cluster.Handshake:
		p.addMembers(msg.Member)
		members := p.members.Slice()
		p.cluster.Engine().Send(c.Sender(), &cluster.Members{
			Members: members,
		})

	case memberLeaveMsg:
		p.handleMemberLeave(msg.ListenAddr)

	case actor.Initialized:
		_ = msg
	case *actor.Ping:
		// noop
	}
}

// registerSelf stores this node's member info in Redis with a TTL.
func (p *RedisProvider) registerSelf() error {
	member := p.cluster.Member()
	data, err := marshalMember(member)
	if err != nil {
		return fmt.Errorf("marshal member: %w", err)
	}
	key := p.memberKey(member.ID)
	ctx := context.Background()
	return p.config.RedisClient.Do(ctx,
		p.config.RedisClient.B().Set().Key(key).Value(string(data)).Ex(p.config.TTL).Build(),
	).Error()
}

// deregisterSelf removes this node's member key from Redis.
func (p *RedisProvider) deregisterSelf() {
	member := p.cluster.Member()
	key := p.memberKey(member.ID)
	ctx := context.Background()
	if err := p.config.RedisClient.Do(ctx,
		p.config.RedisClient.B().Del().Key(key).Build(),
	).Error(); err != nil {
		slog.Error("redis provider: failed to deregister self", "err", err)
	}
}

// handleRefresh refreshes own TTL and scans all member keys from Redis,
// then sends the full member list to the cluster agent.
func (p *RedisProvider) handleRefresh() {
	// Refresh own TTL.
	if err := p.registerSelf(); err != nil {
		slog.Error("redis provider: failed to refresh TTL", "err", err)
	}

	// Scan all member keys from Redis.
	ctx := context.Background()
	pattern := p.memberKeyPattern()

	// Use SCAN to find keys matching the pattern.
	var keys []string
	var cursor uint64
	for {
		scanCmd := p.config.RedisClient.B().Scan().Cursor(cursor).Match(pattern).Count(100).Build()
		entry, err := p.config.RedisClient.Do(ctx, scanCmd).AsScanEntry()
		if err != nil {
			slog.Error("redis provider: failed to scan members", "err", err)
			return
		}
		keys = append(keys, entry.Elements...)
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}

	if len(keys) == 0 {
		return
	}

	// Fetch all member values using MGET.
	mgetCmd := p.config.RedisClient.B().Mget().Key(keys...).Build()
	vals, err := p.config.RedisClient.Do(ctx, mgetCmd).ToArray()
	if err != nil {
		slog.Error("redis provider: failed to fetch member data", "err", err)
		return
	}

	// Parse members and rebuild local set.
	discovered := make([]*cluster.Member, 0, len(vals))
	for _, val := range vals {
		str, err := val.ToString()
		if err != nil {
			continue
		}
		member, err := unmarshalMember([]byte(str))
		if err != nil {
			slog.Warn("redis provider: failed to unmarshal member", "err", err)
			continue
		}
		discovered = append(discovered, member)
	}

	// Replace the local member set with what Redis knows about.
	p.members = cluster.NewMemberSet(discovered...)

	// Always ensure self is present.
	if !p.members.Contains(p.cluster.Member()) {
		p.members.Add(p.cluster.Member())
	}

	p.sendMembersToAgent()
}

// handleMemberLeave removes an unreachable member from both Redis and the local set.
func (p *RedisProvider) handleMemberLeave(listenAddr string) {
	member := p.members.GetByHost(listenAddr)
	if member == nil {
		return
	}

	// Remove from Redis.
	key := p.memberKey(member.ID)
	ctx := context.Background()
	if err := p.config.RedisClient.Do(ctx,
		p.config.RedisClient.B().Del().Key(key).Build(),
	).Error(); err != nil {
		slog.Error("redis provider: failed to remove dead member from redis",
			"member", member.ID, "err", err)
	}

	// Remove from local set.
	p.members.Remove(member)
	p.sendMembersToAgent()
}

// addMembers adds the given members to the local set and notifies the cluster agent.
func (p *RedisProvider) addMembers(members ...*cluster.Member) {
	for _, member := range members {
		if !p.members.Contains(member) {
			p.members.Add(member)
		}
	}
	p.sendMembersToAgent()
}

// sendMembersToAgent sends the current member list to the cluster agent.
func (p *RedisProvider) sendMembersToAgent() {
	members := &cluster.Members{
		Members: p.members.Slice(),
	}
	p.cluster.Engine().Send(p.cluster.PID(), members)
}

// handleEventStream is the child actor function that listens for engine events.
func (p *RedisProvider) handleEventStream(c *actor.Context) {
	switch msg := c.Message().(type) {
	case actor.RemoteUnreachableEvent:
		c.Send(p.pid, memberLeaveMsg{ListenAddr: msg.ListenAddr})
	}
}

// memberKey returns the Redis key for a given member node ID.
func (p *RedisProvider) memberKey(nodeID string) string {
	return fmt.Sprintf("%s:cluster:member:%s", p.config.KeyPrefix, nodeID)
}

// memberKeyPattern returns the glob pattern to match all member keys.
func (p *RedisProvider) memberKeyPattern() string {
	return fmt.Sprintf("%s:cluster:member:*", p.config.KeyPrefix)
}

// marshalMember serializes a cluster.Member to JSON.
func marshalMember(m *cluster.Member) ([]byte, error) {
	info := memberInfo{
		ID:     m.ID,
		Host:   m.Host,
		Region: m.Region,
		Kinds:  m.Kinds,
	}
	return sonic.ConfigFastest.Marshal(info)
}

// unmarshalMember deserializes JSON into a cluster.Member.
func unmarshalMember(data []byte) (*cluster.Member, error) {
	var info memberInfo
	if err := sonic.ConfigFastest.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &cluster.Member{
		ID:     info.ID,
		Host:   info.Host,
		Region: info.Region,
		Kinds:  info.Kinds,
	}, nil
}
