package monitor

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime"
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
	shard  uint32      // the part of the hub that it is in
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

// A hub sends events to its clients one after the other by default, as v0.3.2 did: one lock,
// one map, and each event of a read of the event stream by itself. SetHubParallel makes it keep
// its clients in hubShards parts instead, hand a read over at once, and send to the parts in
// parallel when that pays. What that takes is kept apart, in hubParallel, so that the fan-out in
// sequence reads no more than v0.3.2 did: it only sees that clients is not nil.
//
// The parts are those of ShardedPubSub16 of bbaliweb (ws/pubsub/sharded16.go, 3cf7341).
const hubShards = 16

// What SetHubParallel is given by default, as BenchmarkHubBroadcast found, 10 runs each on a
// cloud VM of 22 vCPUs.
const (
	// DefaultHubParallelMinProcs is how many processors (GOMAXPROCS) it takes. With 2, a
	// parallel fan-out of one event to 1,000 to 10,000 clients took 1.06 to 3.17 times as long
	// as one in sequence, and 100 events to 100 clients 1.13 times; with 4, 8 and 16 it took 0.16
	// to 0.85 times as long wherever there were 1,000 deliveries or more.
	DefaultHubParallelMinProcs = 4
	// DefaultHubParallelMinDeliveries is how many deliveries, clients times messages, a fan-out
	// has at least. One event to 100 clients took longer in parallel with any number of
	// processors.
	DefaultHubParallelMinDeliveries = 1000
)

type hub struct {
	mu      sync.RWMutex
	clients map[*WSClient]struct{} // nil while the clients are in par
	logger  chainedlog.Logger

	following atomic.Bool  // it follows the event stream
	started   atomic.Bool  // it has begun to follow the event stream
	heard     atomic.Int64 // when Redis last answered it, in Unix nanoseconds

	par        *hubParallel // guarded by mu; nil by default
	parallelOn atomic.Bool  // par is set: the reader of the stream hands a read over at once
}

// hubParallel is a hub's clients in parts, and when to send to the parts in parallel. It is
// guarded by the lock of its hub: changed under the write lock, read under the read lock, also by
// the goroutines of a parallel fan-out, which the fan-out waits for.
type hubParallel struct {
	shards  [hubShards]map[*WSClient]struct{}
	next    uint32
	clients int
	procs   int // GOMAXPROCS, as of the last client that came

	minProcs, minDeliveries int
}

func newHub(logger chainedlog.Logger) *hub {
	return &hub{
		clients: make(map[*WSClient]struct{}),
		logger:  logger,
	}
}

// setParallel makes the hub send in parallel with minProcs processors and minDeliveries
// deliveries at least (1000 if 0 or less), or, if minProcs is 0 or less, one after the other as
// by default. The clients that it has move along.
func (h *hub) setParallel(minProcs, minDeliveries int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if minProcs <= 0 {
		if h.par != nil {
			h.clients = make(map[*WSClient]struct{}, h.par.clients)
			for _, shard := range h.par.shards {
				maps.Copy(h.clients, shard)
			}
			h.par = nil
			h.parallelOn.Store(false)
		}
		return
	}
	if minDeliveries <= 0 {
		minDeliveries = DefaultHubParallelMinDeliveries
	}
	if h.par == nil {
		h.par = &hubParallel{procs: runtime.GOMAXPROCS(0)}
		for i := range h.par.shards {
			h.par.shards[i] = make(map[*WSClient]struct{})
		}
		for c := range h.clients {
			h.par.add(c)
		}
		h.clients = nil
		h.parallelOn.Store(true)
	}
	h.par.minProcs, h.par.minDeliveries = minProcs, minDeliveries
}

func (p *hubParallel) add(c *WSClient) {
	p.next++
	c.shard = p.next % hubShards
	p.shards[c.shard][c] = struct{}{}
	p.clients++
	p.procs = runtime.GOMAXPROCS(0)
}

func (p *hubParallel) remove(c *WSClient) {
	if _, ok := p.shards[c.shard][c]; ok {
		delete(p.shards[c.shard], c)
		p.clients--
	}
}

// worth reports whether it pays to send messages to the parts in parallel.
func (p *hubParallel) worth(messages int) bool {
	return p.procs >= p.minProcs && p.clients*messages >= p.minDeliveries
}

// send gives msgs, in order, to each client of the parts, in parallel if it pays, and returns
// once all have them, so that every client gets what comes next after msgs.
func (p *hubParallel) send(msgs [][]byte) {
	if !p.worth(len(msgs)) {
		for _, shard := range p.shards {
			sendShard(shard, msgs)
		}
		return
	}
	var wg sync.WaitGroup
	for _, shard := range p.shards {
		if len(shard) > 0 {
			wg.Go(func() { sendShard(shard, msgs) })
		}
	}
	wg.Wait()
}

// sendShard gives msgs, in order, to each client of a part. A client whose queue is full misses
// what does not fit, which its Missed tells.
func sendShard(shard map[*WSClient]struct{}, msgs [][]byte) {
	for c := range shard {
		for _, m := range msgs {
			select {
			case c.Send <- m:
			default:
				c.missed.Store(true)
			}
		}
	}
}

func (h *hub) Register(c *WSClient) {
	h.mu.Lock()
	if h.clients == nil {
		h.par.add(c)
	} else {
		h.clients[c] = struct{}{}
	}
	h.mu.Unlock()
}

func (h *hub) Unregister(c *WSClient) {
	h.mu.Lock()
	if h.clients == nil {
		h.par.remove(c)
	} else {
		delete(h.clients, c)
	}
	close(c.Send)
	h.mu.Unlock()
}

// Broadcast sends a message to all connected clients.
// Drops the message for any client whose send buffer is full (slow consumer protection),
// which its Missed tells.
func (h *hub) Broadcast(data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.clients == nil {
		h.par.send([][]byte{data})
		return
	}
	for c := range h.clients {
		select {
		case c.Send <- data:
		default:
			c.missed.Store(true)
		}
	}
}

// broadcastBatch sends messages, in order, to all connected clients: each by itself, as
// Broadcast, or, while the hub has its clients in parts, all at once.
func (h *hub) broadcastBatch(msgs [][]byte) {
	if !h.parallelOn.Load() {
		for _, m := range msgs {
			h.Broadcast(m)
		}
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.clients == nil {
		h.par.send(msgs)
		return
	}
	for _, m := range msgs { // it was turned off in the meantime
		for c := range h.clients {
			select {
			case c.Send <- m:
			default:
				c.missed.Store(true)
			}
		}
	}
}

// parallel reports whether the hub would send messages to its parts in parallel.
func (h *hub) parallel(messages int) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.par != nil && h.par.worth(messages)
}

// missed notes for all clients that there have been events which the hub did not get.
func (h *hub) missed() {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.clients == nil {
		for _, shard := range h.par.shards {
			for c := range shard {
				c.missed.Store(true)
			}
		}
		return
	}
	for c := range h.clients {
		c.missed.Store(true)
	}
}

func (h *hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.clients == nil {
		return h.par.clients
	}
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
			if h.parallelOn.Load() {
				// A read is handed over at once.
				entries := streams[streamKey]
				msgs := make([][]byte, 0, len(entries))
				for _, entry := range entries {
					if data := entry.FieldValues["data"]; data != "" {
						msgs = append(msgs, []byte(data))
					}
					last = entry.ID
				}
				h.broadcastBatch(msgs)
			} else {
				for _, entry := range streams[streamKey] {
					if data := entry.FieldValues["data"]; data != "" {
						h.Broadcast([]byte(data))
					}
					last = entry.ID
				}
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
