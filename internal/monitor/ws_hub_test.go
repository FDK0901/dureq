package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/FDK0901/go-chainedlog/impl/chainedslog"
	"github.com/coder/websocket"
	"github.com/redis/rueidis"
)

func quietHub() *hub { return newHub(chainedslog.NewSlog(chainedslog.NewSlogBase())) }

func next(t *testing.T, c *WSClient) string {
	t.Helper()
	select {
	case msg := <-c.Send:
		return string(msg)
	case <-time.After(10 * time.Second):
		t.Fatal("no message")
		return ""
	}
}

// A client that is too slow is told that it has missed events, and others get them.
func TestHub_SlowClient(t *testing.T) {
	h := quietHub()
	slow, quick := NewWSClient(), NewWSClient()
	h.Register(slow)
	h.Register(quick)
	for i := range 2 * wsSendBufferSize {
		h.Broadcast([]byte(strconv.Itoa(i)))
		if got := next(t, quick); got != strconv.Itoa(i) {
			t.Fatalf("got %s, want %d", got, i)
		}
	}
	if !slow.Missed() || slow.Missed() || quick.Missed() {
		t.Error("only the slow client should be told, and once")
	}
}

// downRedis fails blocking reads and subscriptions while it is down.
type downRedis struct {
	rueidis.Client
	down     atomic.Bool
	failures atomic.Int64
}

func (d *downRedis) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	if d.down.Load() && (cmd.Commands()[0] == "XREAD" || cmd.Commands()[0] == "XRANGE") {
		d.failures.Add(1)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		ctx = cancelled
	}
	return d.Client.Do(ctx, cmd)
}

func (d *downRedis) Receive(ctx context.Context, cmd rueidis.Completed, f func(rueidis.PubSubMessage)) error {
	if d.down.Load() {
		d.failures.Add(1)
		return errors.New("refused")
	}
	return d.Client.Receive(ctx, cmd, f)
}

func followed(t *testing.T) (*store.RedisStore, *downRedis, *WSClient, func(n int)) {
	t.Helper()
	st, _ := newTestAPI(t, nil)
	rdb := &downRedis{Client: st.Client()}
	h := quietHub()
	c := NewWSClient()
	h.Register(c)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { h.follow(ctx, rdb, store.EventsStreamKey(st.Prefix())) })
	t.Cleanup(func() { cancel(); wg.Wait() })
	time.Sleep(200 * time.Millisecond)
	sent := 0
	return st, rdb, c, func(n int) {
		t.Helper()
		for range n {
			publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: fmt.Sprint(sent), Timestamp: time.Now()})
			sent++
		}
	}
}

func jobIDOf(t *testing.T, msg string) string {
	t.Helper()
	var e types.JobEvent
	if err := json.Unmarshal([]byte(msg), &e); err != nil {
		t.Fatalf("%q: %v", msg, err)
	}
	return e.JobID
}

// What is published while Redis does not answer is not lost.
func TestHub_GoesOnWhereItWas(t *testing.T) {
	_, rdb, c, send := followed(t)
	send(3)
	for i := range 3 {
		if got := jobIDOf(t, next(t, c)); got != fmt.Sprint(i) {
			t.Fatalf("got event %s, want %d", got, i)
		}
	}
	rdb.down.Store(true)
	send(1) // ends the read that is waiting
	next(t, c)
	waitUntil(t, func() bool { return rdb.failures.Load() > 0 })
	send(5)
	rdb.down.Store(false)
	for i := 4; i < 9; i++ {
		if got := jobIDOf(t, next(t, c)); got != fmt.Sprint(i) {
			t.Fatalf("got event %s, want %d", got, i)
		}
	}
	if c.Missed() {
		t.Error("nothing was missed")
	}
}

// What has been trimmed by the time Redis is back is missed, and the clients are told.
func TestHub_TellsOfTrimmedEvents(t *testing.T) {
	st, rdb, c, send := followed(t)
	rdb.down.Store(true)
	send(1)
	next(t, c)
	waitUntil(t, func() bool { return rdb.failures.Load() > 0 })
	send(20)
	if err := st.Client().Do(context.Background(), st.Client().B().Xtrim().Key(store.EventsStreamKey(st.Prefix())).Maxlen().Threshold("5").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	rdb.down.Store(false)
	for i := 16; i < 21; i++ {
		if got := jobIDOf(t, next(t, c)); got != fmt.Sprint(i) {
			t.Fatalf("got event %s, want %d", got, i)
		}
	}
	if !c.Missed() {
		t.Error("the client is not told that it has missed events")
	}
}

// A subscription that is refused is tried again.
func TestHub_SubscriptionIsTriedAgain(t *testing.T) {
	st, _ := newTestAPI(t, nil)
	rdb := &downRedis{Client: st.Client()}
	rdb.down.Store(true)
	h := quietHub()
	c := NewWSClient()
	h.Register(c)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { h.SubscribeRedis(ctx, rdb, store.EventsPubSubChannel(st.Prefix())) })
	t.Cleanup(func() { cancel(); wg.Wait() })
	waitUntil(t, func() bool { return rdb.failures.Load() > 1 })
	rdb.down.Store(false)
	deadline := time.Now().Add(10 * time.Second)
	for {
		publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: "x", Timestamp: time.Now()})
		select {
		case <-c.Send:
			if !c.Missed() {
				t.Error("the client is not told that it has missed events")
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("no event after the subscription was allowed again")
		}
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
	}
}

func dial(t *testing.T, srv *httptest.Server, query string) func() string {
	t.Helper()
	conn, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http")+"/api/ws"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return func() string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, msg, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return string(msg)
	}
}

// A client that asks for it gets heartbeats, and is told what it has missed. Others get events
// and nothing else, as ever.
func TestWebSocket_Versions(t *testing.T) {
	st, _ := newTestAPI(t, nil)
	api := NewAPI(st, nil, nil)
	t.Cleanup(api.Shutdown)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	v1, v2 := dial(t, srv, ""), dial(t, srv, "?v=2")
	if got := v2(); got != string(wsHeartbeatMsg) {
		t.Fatalf("first message %s, want a heartbeat", got)
	}
	h := api.svc.hub.(*hub)
	waitUntil(t, func() bool { return h.ClientCount() == 2 })
	time.Sleep(200 * time.Millisecond)
	publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: "a", Timestamp: time.Now()})
	if a, b := jobIDOf(t, v1()), jobIDOf(t, v2()); a != "a" || b != "a" {
		t.Fatalf("got %q and %q", a, b)
	}
	h.missed()
	publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: "b", Timestamp: time.Now()})
	if got := jobIDOf(t, v2()); got != "b" {
		t.Fatalf("got %q", got)
	}
	if got := v2(); got != string(wsResyncMsg) {
		t.Fatalf("got %s, want to be told", got)
	}
	publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: "c", Timestamp: time.Now()})
	if a, b := jobIDOf(t, v1()), jobIDOf(t, v1()); a != "b" || b != "c" {
		t.Errorf("a client that has not asked for it got %q and %q", a, b)
	}
}
