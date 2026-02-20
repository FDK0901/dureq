package monitor

import (
	"context"
	"net/http"
	"time"

	gochainedlog "github.com/FDK0901/go-chainedlog"
	"nhooyr.io/websocket"
)

const (
	wsPingInterval = 30 * time.Second
	wsWriteTimeout = 10 * time.Second
)

func handleWebSocket(hub *Hub, logger gochainedlog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			logger.Warn().Err(err).Msg("ws: failed to accept connection")
			return
		}
		defer conn.CloseNow()

		client := &wsClient{
			send: make(chan []byte, wsSendBufferSize),
		}
		hub.register(client)
		defer hub.unregister(client)

		logger.Debug().Int("total_clients", hub.clientCount()).Msg("ws: client connected")

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

		// Write pump — forwards events from hub to WebSocket connection.
		ticker := time.NewTicker(wsPingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				conn.Close(websocket.StatusNormalClosure, "server shutting down")
				return
			case msg, ok := <-client.send:
				if !ok {
					conn.Close(websocket.StatusNormalClosure, "")
					return
				}
				writeCtx, writeCancel := context.WithTimeout(ctx, wsWriteTimeout)
				err := conn.Write(writeCtx, websocket.MessageText, msg)
				writeCancel()
				if err != nil {
					logger.Debug().Err(err).Msg("ws: write error, closing connection")
					return
				}
			case <-ticker.C:
				pingCtx, pingCancel := context.WithTimeout(ctx, wsWriteTimeout)
				err := conn.Ping(pingCtx)
				pingCancel()
				if err != nil {
					logger.Debug().Err(err).Msg("ws: ping timeout, closing connection")
					return
				}
			}
		}
	}
}
