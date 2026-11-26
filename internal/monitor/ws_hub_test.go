package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
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
	"github.com/rs/xid"
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
	if got := v2(); frameType(got) != wsHeartbeatType {
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
	if got := v2(); frameType(got) != wsResyncType {
		t.Fatalf("got %s, want to be told", got)
	}
	publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: "c", Timestamp: time.Now()})
	if a, b := jobIDOf(t, v1()), jobIDOf(t, v1()); a != "b" || b != "c" {
		t.Errorf("a client that has not asked for it got %q and %q", a, b)
	}
}

// redisProxy passes TCP through to the test Redis. Hung, a connection stays open and passes
// nothing on any more, as one does behind a proxy whose upstream is gone.
type redisProxy struct {
	ln       net.Listener
	accepted atomic.Int64
	hangNew  atomic.Bool // connections that are opened hang, too

	mu    sync.Mutex
	conns []*proxiedConn
}

type proxiedConn struct {
	client, server net.Conn
	hung           atomic.Bool
}

func newRedisProxy(t *testing.T) *redisProxy {
	t.Helper()
	target, _ := testRedis(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &redisProxy{ln: ln}
	t.Cleanup(func() {
		ln.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.conns {
			c.client.Close()
			c.server.Close()
		}
	})
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			p.accepted.Add(1)
			server, err := net.Dial("tcp", target)
			if err != nil {
				client.Close()
				continue
			}
			c := &proxiedConn{client: client, server: server}
			c.hung.Store(p.hangNew.Load())
			p.mu.Lock()
			p.conns = append(p.conns, c)
			p.mu.Unlock()
			go c.pipe(client, server)
			go c.pipe(server, client)
		}
	}()
	return p
}

func (c *proxiedConn) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		for c.hung.Load() {
			time.Sleep(5 * time.Millisecond)
		}
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				src.Close()
				return
			}
		}
		if err != nil {
			dst.Close()
			return
		}
	}
}

// hang makes the connections that are open hang, and those that are opened from now on if
// alsoNew.
func (p *redisProxy) hang(alsoNew bool) {
	p.hangNew.Store(alsoNew)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.hung.Store(true)
	}
}

// release lets all connections pass again.
func (p *redisProxy) release() {
	p.hangNew.Store(false)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.hung.Store(false)
	}
}

// client connects to Redis through the proxy.
func (p *redisProxy) client(t *testing.T) rueidis.Client {
	t.Helper()
	_, password := testRedis(t)
	rdb, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{p.ln.Addr().String()}, Password: password})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rdb.Close)
	return rdb
}

func nextWithin(t *testing.T, c *WSClient, d time.Duration) string {
	t.Helper()
	select {
	case msg := <-c.Send:
		return string(msg)
	case <-time.After(d):
		t.Fatalf("no message within %v", d)
		return ""
	}
}

// A connection that is open but does not answer any more is given up, and the hub goes on
// where it was on another one.
func TestHub_GivesUpHungConnection(t *testing.T) {
	st, _ := newTestAPI(t, nil)
	px := newRedisProxy(t)
	h := quietHub()
	c := NewWSClient()
	h.Register(c)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { h.follow(ctx, px.client(t), store.EventsStreamKey(st.Prefix())) })
	t.Cleanup(func() { cancel(); wg.Wait() })
	time.Sleep(200 * time.Millisecond)
	for i := range 3 {
		publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: fmt.Sprint(i)})
	}
	for i := range 3 {
		if got := jobIDOf(t, next(t, c)); got != fmt.Sprint(i) {
			t.Fatalf("got event %s, want %d", got, i)
		}
	}

	px.hang(false)
	opened := px.accepted.Load()
	start := time.Now()
	for i := 3; i < 8; i++ {
		publish(t, st, types.JobEvent{Type: types.EventJobStarted, JobID: fmt.Sprint(i)})
	}
	// A read takes hubReadBlock at most; Redis also checks the connections that are not
	// reading, every 10 seconds by default.
	for i := 3; i < 8; i++ {
		if got := jobIDOf(t, nextWithin(t, c, 45*time.Second)); got != fmt.Sprint(i) {
			t.Fatalf("got event %s, want %d", got, i)
		}
	}
	t.Logf("back after %v, on %d new connections", time.Since(start).Round(time.Millisecond), px.accepted.Load()-opened)
	select {
	case msg := <-c.Send:
		t.Errorf("got %s twice", msg)
	case <-time.After(200 * time.Millisecond):
	}
	if c.Missed() {
		t.Error("nothing was missed")
	}
}

// While the hub does not hear from Redis, a client that asks for heartbeats gets none: it is
// told to come back later, and is not let in until the hub hears again.
func TestWebSocket_NoHeartbeatWhileTheHubHearsNothing(t *testing.T) {
	defer shorten(&hubReadBlock, 500*time.Millisecond)()
	defer shorten(&hubReadSlack, 500*time.Millisecond)()
	defer shorten(&wsHeartbeat, 300*time.Millisecond)()
	px := newRedisProxy(t)
	st, err := store.NewRedisStore(px.client(t), store.RedisStoreConfig{
		KeyPrefix: "test_" + xid.New().String(),
		Tiers:     []store.TierConfig{{Name: "normal", Weight: 1, FetchBatch: 10}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	api := NewAPI(st, nil, nil)
	t.Cleanup(api.Shutdown)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/ws?v=2"

	conn, _, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	beat := func() (time.Time, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, msg, err := conn.Read(ctx)
		if err == nil && frameType(string(msg)) != wsHeartbeatType {
			err = fmt.Errorf("got %s", msg)
		}
		return time.Now(), err
	}
	for range 3 {
		if _, err := beat(); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
	}

	px.hang(true)
	hung := time.Now()
	last := hung
	for {
		at, err := beat()
		if err != nil {
			if code := websocket.CloseStatus(err); code != websocket.StatusTryAgainLater {
				t.Fatalf("connection ended with %v (%d), want %d", err, code, websocket.StatusTryAgainLater)
			}
			break
		}
		last = at
		if at.Sub(hung) > 10*time.Second {
			t.Fatal("still heartbeats 10 seconds after Redis stopped answering")
		}
	}
	if late := last.Sub(hung); late > hubReadBlock+hubReadSlack+wsHeartbeat+500*time.Millisecond {
		t.Errorf("heartbeat %v after Redis stopped answering", late)
	}
	if _, res, err := websocket.Dial(context.Background(), url, nil); err == nil || res == nil || res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("a client was let in while the hub hears nothing: %v", err)
	}

	px.release()
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, _, err := websocket.Dial(context.Background(), url, nil)
		if err == nil {
			defer conn.CloseNow()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, msg, err := conn.Read(ctx)
			cancel()
			if err != nil || frameType(string(msg)) != wsHeartbeatType {
				t.Fatalf("first message %s (%v), want a heartbeat", msg, err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not let in again: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// frameType returns the type of a frame of the WebSocket.
func frameType(msg string) string {
	var f struct {
		Type string `json:"type"`
	}
	json.Unmarshal([]byte(msg), &f)
	return f.Type
}

// shorten sets *d to v for a test, and returns what sets it back.
func shorten(d *time.Duration, v time.Duration) func() {
	old := *d
	*d = v
	return func() { *d = old }
}

// smallBuffers keeps little of what is sent on the connections that it accepts, so that a
// client that does not read soon holds the writer up.
type smallBuffers struct{ net.Listener }

func (l smallBuffers) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetWriteBuffer(8 << 10)
	}
	return c, err
}

// A client that has missed events and does not catch up, as events keep coming faster than it
// reads, is told so all the same.
func TestWebSocket_SlowClientIsToldInTime(t *testing.T) {
	defer shorten(&wsResyncAfter, 300*time.Millisecond)()
	st, _ := newTestAPI(t, nil)
	api := NewAPI(st, nil, nil)
	t.Cleanup(api.Shutdown)
	srv := httptest.NewUnstartedServer(api.Handler())
	srv.Listener = smallBuffers{srv.Listener}
	srv.Start()
	t.Cleanup(srv.Close)
	dialer := &net.Dialer{}
	conn, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http")+"/api/ws?v=2", &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dialer.DialContext(ctx, network, addr)
			if tc, ok := c.(*net.TCPConn); ok {
				tc.SetReadBuffer(8 << 10)
			}
			return c, err
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	wg.Go(func() {
		// About a thousand a second, appended in one round trip each 50 at a time, so that
		// they come faster than the client reads also on a busy machine.
		big := types.TaskType(strings.Repeat("x", 1000))
		rdb, key := st.Client(), store.EventsStreamKey(st.Prefix())
		for i := 0; ctx.Err() == nil; {
			cmds := make(rueidis.Commands, 0, 50)
			for range 50 {
				data, _ := json.Marshal(types.JobEvent{Type: types.EventJobStarted, JobID: fmt.Sprint(i), TaskType: big, Timestamp: time.Now()})
				cmds = append(cmds, rdb.B().Xadd().Key(key).Id("*").FieldValue().FieldValue("type", string(types.EventJobStarted)).FieldValue("data", string(data)).Build())
				i++
			}
			rdb.DoMulti(ctx, cmds...)
			time.Sleep(50 * time.Millisecond)
		}
	})

	start := time.Now()
	events := 0
	for time.Since(start) < 15*time.Second {
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, msg, err := conn.Read(rctx)
		rcancel()
		if err != nil {
			t.Fatal(err)
		}
		if frameType(string(msg)) == wsResyncType {
			t.Logf("told after %v and %d events", time.Since(start).Round(time.Millisecond), events)
			return
		}
		events++
		time.Sleep(20 * time.Millisecond) // reads fifty a second
	}
	t.Fatalf("not told in 15 seconds, after %d events", events)
}

// A hub that has been shut down reads no events any more, and says so.
func TestHub_StalledOnceShutDown(t *testing.T) {
	st, _ := newTestAPI(t, nil)
	api := NewAPI(st, nil, nil)
	h := api.svc.hub.(*hub)
	time.Sleep(200 * time.Millisecond)
	if h.Stalled() {
		t.Fatal("stalled while it reads")
	}
	api.Shutdown()
	waitUntil(t, h.Stalled)
}

// A heartbeat says when the server wrote it, so that a client that gets it late can tell by how
// much: behind a slow connection, events come late, and the connection is still open.
func TestWebSocket_HeartbeatSaysWhenItWasWritten(t *testing.T) {
	defer shorten(&wsHeartbeat, 200*time.Millisecond)()
	st, _ := newTestAPI(t, nil)
	api := NewAPI(st, nil, nil)
	t.Cleanup(api.Shutdown)
	srv := httptest.NewUnstartedServer(api.Handler())
	srv.Listener = smallBuffers{srv.Listener}
	srv.Start()
	t.Cleanup(srv.Close)
	dialer := &net.Dialer{}
	conn, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http")+"/api/ws?v=2", &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dialer.DialContext(ctx, network, addr)
			if tc, ok := c.(*net.TCPConn); ok {
				tc.SetReadBuffer(8 << 10)
			}
			return c, err
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)
	type beat struct {
		Type         string `json:"type"`
		ServerTimeMs *int64 `json:"server_time_ms"`
	}
	read := func() (beat, time.Time) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, msg, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var b beat
		json.Unmarshal(msg, &b)
		return b, time.Now()
	}
	first, at := read()
	if first.Type != wsHeartbeatType || first.ServerTimeMs == nil {
		t.Fatalf("first frame %+v, want a heartbeat with server_time_ms", first)
	}
	if d := at.Sub(time.UnixMilli(*first.ServerTimeMs)); d < -time.Second || d > time.Second {
		t.Fatalf("server_time_ms is %v off", d)
	}

	// Events that the client reads slowly hold the heartbeats up behind them.
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	wg.Go(func() {
		big := types.TaskType(strings.Repeat("x", 1000))
		rdb, key := st.Client(), store.EventsStreamKey(st.Prefix())
		for i := 0; ctx.Err() == nil; {
			cmds := make(rueidis.Commands, 0, 50)
			for range 50 {
				data, _ := json.Marshal(types.JobEvent{Type: types.EventJobStarted, JobID: fmt.Sprint(i), TaskType: big, Timestamp: time.Now()})
				cmds = append(cmds, rdb.B().Xadd().Key(key).Id("*").FieldValue().FieldValue("type", string(types.EventJobStarted)).FieldValue("data", string(data)).Build())
				i++
			}
			rdb.DoMulti(ctx, cmds...)
			time.Sleep(50 * time.Millisecond)
		}
	})
	var late time.Duration
	for start := time.Now(); time.Since(start) < 10*time.Second && late < time.Second; time.Sleep(20 * time.Millisecond) {
		if b, at := read(); b.Type == wsHeartbeatType && b.ServerTimeMs != nil {
			late = max(late, at.Sub(time.UnixMilli(*b.ServerTimeMs)))
		}
	}
	if late < time.Second {
		t.Errorf("heartbeats came at most %v late, want one that tells of a second or more", late)
	}
	t.Logf("a heartbeat came %v after it was written", late.Round(time.Millisecond))
}
