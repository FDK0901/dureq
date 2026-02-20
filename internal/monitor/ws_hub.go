package monitor

import (
	"context"
	"sync"

	gochainedlog "github.com/FDK0901/go-chainedlog"
	"github.com/redis/rueidis"
)

const wsSendBufferSize = 64

// wsClient represents a single WebSocket connection registered with the Hub.
type wsClient struct {
	send chan []byte
}

// Hub manages WebSocket client connections and broadcasts Redis Pub/Sub events.
// A single Redis subscription feeds all connected WebSocket clients (fan-out).
type Hub struct {
	mu      sync.RWMutex
	clients map[*wsClient]struct{}
	logger  gochainedlog.Logger
}

func newHub(logger gochainedlog.Logger) *Hub {
	return &Hub{
		clients: make(map[*wsClient]struct{}),
		logger:  logger,
	}
}

func (h *Hub) register(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) unregister(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	close(c.send)
	h.mu.Unlock()
}

// broadcast sends a message to all connected clients.
// Drops the message for any client whose send buffer is full (slow consumer protection).
func (h *Hub) broadcast(data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		select {
		case c.send <- data:
		default:
			// Slow consumer — drop this event.
		}
	}
}

func (h *Hub) clientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// subscribeRedis subscribes to the Redis Pub/Sub events channel and
// broadcasts every received message to all connected WebSocket clients.
func (h *Hub) subscribeRedis(ctx context.Context, rdb rueidis.Client, channel string) {
	err := rdb.Receive(ctx, rdb.B().Subscribe().Channel(channel).Build(),
		func(msg rueidis.PubSubMessage) {
			h.broadcast([]byte(msg.Message))
		})
	if err != nil && ctx.Err() == nil {
		h.logger.Warn().Err(err).Msg("ws hub: redis subscribe loop ended")
	}
}
