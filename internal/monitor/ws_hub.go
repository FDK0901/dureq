package monitor

import (
	"context"
	"sync"

	"github.com/FDK0901/go-chainedlog"
	"github.com/redis/rueidis"
)

const wsSendBufferSize = 64

// WSClient represents a single WebSocket connection registered with the Hub.
type WSClient struct {
	Send chan []byte
}

// NewWSClient creates a new WebSocket client with a buffered send channel.
func NewWSClient() *WSClient {
	return &WSClient{Send: make(chan []byte, wsSendBufferSize)}
}

// Hub manages WebSocket client connections and broadcasts Redis Pub/Sub events.
// A single Redis subscription feeds all connected WebSocket clients (fan-out).
type Hub interface {
	Register(c *WSClient)
	Unregister(c *WSClient)

	Broadcast(data []byte)
	ClientCount() int
	SubscribeRedis(ctx context.Context, rdb rueidis.Client, channel string)
}

type hub struct {
	mu      sync.RWMutex
	clients map[*WSClient]struct{}
	logger  chainedlog.Logger
}

func newHub(logger chainedlog.Logger) Hub {
	return &hub{
		clients: make(map[*WSClient]struct{}),
		logger:  logger,
	}
}

func (h *hub) Register(c *WSClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) Unregister(c *WSClient) {
	h.mu.Lock()
	delete(h.clients, c)
	close(c.Send)
	h.mu.Unlock()
}

// Broadcast sends a message to all connected clients.
// Drops the message for any client whose send buffer is full (slow consumer protection).
func (h *hub) Broadcast(data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		select {
		case c.Send <- data:
		default:
		}
	}
}

func (h *hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// SubscribeRedis subscribes to the Redis Pub/Sub events channel and
// broadcasts every received message to all connected WebSocket clients.
func (h *hub) SubscribeRedis(ctx context.Context, rdb rueidis.Client, channel string) {
	err := rdb.Receive(ctx, rdb.B().Subscribe().Channel(channel).Build(),
		func(msg rueidis.PubSubMessage) {
			h.Broadcast([]byte(msg.Message))
		})
	if err != nil && ctx.Err() == nil {
		h.logger.Warn().Err(err).Msg("ws hub: redis subscribe loop ended")
	}
}
