package monitor

import (
	"context"
	"net/http"
	"time"

	"github.com/FDK0901/go-chainedlog"
	"github.com/coder/websocket"
)

const (
	wsPingInterval = 30 * time.Second
	wsWriteTimeout = 10 * time.Second
	wsHeartbeat    = 15 * time.Second
)

// Messages that a client gets beside events if it asks for them, with ?v=2.
var (
	// It comes right after the connection is open, and then regularly: if it stops coming,
	// the connection is dead.
	wsHeartbeatMsg = []byte(`{"type":"ws.heartbeat"}`)
	// There have been events that the client did not get.
	wsResyncMsg = []byte(`{"type":"ws.resync"}`)
)

// HandleWebSocket returns an http.HandlerFunc that upgrades connections to
// WebSocket and streams real-time events from the Hub. Whether a web page of another origin
// may connect is up to whoever serves it.
func HandleWebSocket(hub Hub, logger chainedlog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true, // see originGuard
		})
		if err != nil {
			logger.Warn().Err(err).Msg("ws: failed to accept connection")
			return
		}
		defer conn.CloseNow()

		client := NewWSClient()
		hub.Register(client)
		defer hub.Unregister(client)

		logger.Debug().Int("total_clients", hub.ClientCount()).Msg("ws: client connected")

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// Read pump — reads (and discards) client messages, detects disconnection.
		go func() {
			defer cancel()
			for {
				_, _, err := conn.Read(ctx)
				if err != nil {
					return
				}
			}
		}()

		// Pings wait for the pong, so they have a goroutine of their own.
		go func() {
			defer cancel()
			ticker := time.NewTicker(wsPingInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				pingCtx, pingCancel := context.WithTimeout(ctx, wsWriteTimeout)
				err := conn.Ping(pingCtx)
				pingCancel()
				if err != nil {
					logger.Debug().Err(err).Msg("ws: ping timeout, closing connection")
					return
				}
			}
		}()

		write := func(msg []byte) bool {
			writeCtx, writeCancel := context.WithTimeout(ctx, wsWriteTimeout)
			defer writeCancel()
			if err := conn.Write(writeCtx, websocket.MessageText, msg); err != nil {
				logger.Debug().Err(err).Msg("ws: write error, closing connection")
				return false
			}
			return true
		}

		// Write pump — forwards events from hub to WebSocket connection.
		v2 := r.URL.Query().Get("v") == "2"
		var heartbeat <-chan time.Time
		if v2 {
			ticker := time.NewTicker(wsHeartbeat)
			defer ticker.Stop()
			heartbeat = ticker.C
			if !write(wsHeartbeatMsg) {
				return
			}
		}
		for {
			select {
			case <-ctx.Done():
				conn.Close(websocket.StatusNormalClosure, "server shutting down")
				return
			case msg, ok := <-client.Send:
				if !ok {
					conn.Close(websocket.StatusNormalClosure, "")
					return
				}
				if !write(msg) {
					return
				}
			case <-heartbeat:
				if !write(wsHeartbeatMsg) {
					return
				}
			}
			// Once it has caught up, it is told that it has missed some.
			if v2 && len(client.Send) == 0 && client.Missed() && !write(wsResyncMsg) {
				return
			}
		}
	}
}
