package monitor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FDK0901/go-chainedlog"
	"github.com/redis/rueidis"
)

const (
	wsSendBufferSize = 256

	hubReadCount   = 500
	hubMinBackoff  = 100 * time.Millisecond
	hubMaxBackoff  = 10 * time.Second
	hubCallTimeout = 10 * time.Second
)

var (
	hubReadBlock = 5 * time.Second
	// hubReadSlack is how much longer than hubReadBlock Redis may take to answer a read. If it
	// takes longer, the connection is given up, and the hub is taken not to hear from Redis.
	hubReadSlack = 5 * time.Second
)

// WSClient represents a single WebSocket connection registered with the Hub.
type WSClient struct {
	Send chan []byte

	missed atomic.Bool // events that it did not get
}

// NewWSClient creates a new WebSocket client with a buffered send channel.
func NewWSClient() *WSClient {
	return &WSClient{Send: make(chan []byte, wsSendBufferSize)}
}

// Missed reports whether there have been events since the last call that the client did not
// get: it was too slow, or the hub itself missed some.
func (c *WSClient) Missed() bool { return c.missed.Swap(false) }

// Hub manages WebSocket client connections and broadcasts events to them.
// A single reader of Redis feeds all connected WebSocket clients (fan-out).
type Hub interface {
	Register(c *WSClient)
	Unregister(c *WSClient)

	Broadcast(data []byte)
	ClientCount() int
	SubscribeRedis(ctx context.Context, rdb rueidis.Client, channel string)
}

// HubHealth is what the hub of NewAPIService tells beside Hub, of how it reads the events.
// A handler that sends its clients heartbeats is to stop when the hub is stalled, for they would
// say that events come when none do:
//
//	if h, ok := svc.Hub().(dureq.HubHealth); ok && h.Stalled() { ... }
type HubHealth interface {
	// Stalled reports whether events are not being read: Redis has not answered the hub for
	// longer than a read takes, or the hub has been shut down. It is false once Redis answers
	// again, and for a hub that has not begun to read.
	Stalled() bool
}

type hub struct {
	mu      sync.RWMutex
	clients map[*WSClient]struct{}
	logger  chainedlog.Logger

	started   atomic.Bool  // it has begun to follow the event stream
	following atomic.Bool  // it follows the event stream
	heard     atomic.Int64 // when Redis last answered it, in Unix nanoseconds
}

func newHub(logger chainedlog.Logger) *hub {
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
// Drops the message for any client whose send buffer is full (slow consumer protection),
// which its Missed tells.
func (h *hub) Broadcast(data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		select {
		case c.Send <- data:
		default:
			c.missed.Store(true)
		}
	}
}

// missed notes for all clients that there have been events which the hub did not get.
func (h *hub) missed() {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		c.missed.Store(true)
	}
}

func (h *hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// Stalled reports whether the hub has followed the event stream and does not any more, or Redis
// has not answered it for longer than a read takes.
func (h *hub) Stalled() bool {
	if !h.started.Load() {
		return false
	}
	return !h.following.Load() || time.Since(time.Unix(0, h.heard.Load())) > hubReadBlock+hubReadSlack
}

// answered notes that Redis has answered.
func (h *hub) answered() { h.heard.Store(time.Now().UnixNano()) }

// SubscribeRedis subscribes to the Redis Pub/Sub events channel and
// broadcasts every received message to all connected WebSocket clients, until ctx ends.
func (h *hub) SubscribeRedis(ctx context.Context, rdb rueidis.Client, channel string) {
	h.retry(ctx, "subscription", func() error {
		defer h.missed() // what is published until it is back
		return rdb.Receive(ctx, rdb.B().Subscribe().Channel(channel).Build(),
			func(msg rueidis.PubSubMessage) {
				h.Broadcast([]byte(msg.Message))
			})
	})
}

// follow broadcasts every event that is appended to the event stream, until ctx ends. If Redis
// fails, or does not answer, it goes on where it was. Unlike Pub/Sub, a stream has a memory.
func (h *hub) follow(ctx context.Context, rdb rueidis.Client, streamKey string) {
	h.answered()
	h.following.Store(true)
	h.started.Store(true)
	defer h.following.Store(false)
	last := "" // the last event that was read
	h.retry(ctx, "reader", func() error {
		if last == "" {
			id, err := lastEventID(ctx, rdb, streamKey)
			if err != nil {
				return err
			}
			h.answered()
			last = id
		}
		// What has been trimmed in the meantime was not read.
		if first, err := firstEventID(ctx, rdb, streamKey); err != nil {
			return err
		} else if first.after(parseEventID(last)) && last != "0-0" {
			h.missed()
		}
		h.answered()
		for ctx.Err() == nil {
			streams, err := readEvents(ctx, rdb, streamKey, last)
			if rueidis.IsRedisNil(err) {
				h.answered()
				continue
			}
			if err != nil {
				return err
			}
			h.answered()
			for _, entry := range streams[streamKey] {
				if data := entry.FieldValues["data"]; data != "" {
					h.Broadcast([]byte(data))
				}
				last = entry.ID
			}
			if len(streams[streamKey]) == hubReadCount {
				// Behind: see whether what comes next is still there.
				first, err := firstEventID(ctx, rdb, streamKey)
				if err != nil {
					return err
				}
				if first.after(parseEventID(last)) {
					h.missed()
				}
			}
		}
		return nil
	})
}

// retry runs f until ctx ends, and waits longer the more often it fails in a row.
func (h *hub) retry(ctx context.Context, what string, f func() error) {
	backoff := hubMinBackoff
	for ctx.Err() == nil {
		began := time.Now()
		err := f()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = errors.New("ended")
		}
		h.logger.Warn().Err(err).String("what", what).Msg("ws hub: lost Redis, trying again")
		if time.Since(began) > hubMaxBackoff {
			backoff = hubMinBackoff
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, hubMaxBackoff)
	}
}

// eventID is the ID of an entry of the event stream.
type eventID struct{ ms, seq uint64 }

func (a eventID) after(b eventID) bool { return a.ms > b.ms || a.ms == b.ms && a.seq > b.seq }

func parseEventID(s string) eventID {
	ms, seq, _ := strings.Cut(s, "-")
	var id eventID
	id.ms, _ = strconv.ParseUint(ms, 10, 64)
	id.seq, _ = strconv.ParseUint(seq, 10, 64)
	return id
}

// The commands of the hub are pinned, not given back to rueidis for reuse once they are done. A
// read that is given up, or one that rueidis retries on another connection, may still be in the
// writer of the connection that was given up; reused, it was written to while that was reading it.

// readEvents waits for the events after last, as long as hubReadBlock. A connection can stay
// open and not answer, such as one to a proxy whose own connection to Redis is gone. A read that
// is not answered in time gives it up, and the next one is made on another.
func readEvents(ctx context.Context, rdb rueidis.Client, streamKey, last string) (map[string][]rueidis.XRangeEntry, error) {
	// Not a deadline: with one, rueidis waits for the connection's deadline even if ctx ends.
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := time.AfterFunc(hubReadBlock+hubReadSlack, cancel)
	defer timer.Stop()
	cmd := rdb.B().Xread().Count(hubReadCount).Block(hubReadBlock.Milliseconds()).Streams().Key(streamKey).Id(last).Build().Pin()
	streams, err := rdb.Do(readCtx, cmd).AsXRead()
	if err != nil && ctx.Err() == nil && readCtx.Err() != nil && !rueidis.IsRedisNil(err) {
		return nil, fmt.Errorf("read events: no answer in %v: %w", hubReadBlock+hubReadSlack, err)
	}
	return streams, err
}

func lastEventID(ctx context.Context, rdb rueidis.Client, streamKey string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, hubCallTimeout)
	defer cancel()
	entries, err := rdb.Do(ctx, rdb.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(1).Build().Pin()).AsXRange()
	if err != nil {
		return "", fmt.Errorf("last event: %w", err)
	}
	if len(entries) == 0 {
		return "0-0", nil
	}
	return entries[0].ID, nil
}

func firstEventID(ctx context.Context, rdb rueidis.Client, streamKey string) (eventID, error) {
	ctx, cancel := context.WithTimeout(ctx, hubCallTimeout)
	defer cancel()
	entries, err := rdb.Do(ctx, rdb.B().Xrange().Key(streamKey).Start("-").End("+").Count(1).Build().Pin()).AsXRange()
	if err != nil {
		return eventID{}, fmt.Errorf("first event: %w", err)
	}
	if len(entries) == 0 {
		return eventID{}, nil
	}
	return parseEventID(entries[0].ID), nil
}
