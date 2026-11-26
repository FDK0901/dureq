package monitor

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
	"time"
)

func init() {
	setFanOut = func(h *hub, mode string) func() {
		old, procs := hubParallelMin, h.procs.Load()
		switch mode {
		case "seq":
			hubParallelMin = math.MaxInt
		case "par":
			hubParallelMin = 0
			h.procs.Store(64) // also with one processor, to see what it costs there
		}
		return func() { hubParallelMin = old; h.procs.Store(procs) }
	}
}

// However it fans out, every client gets every event once and in order, and one that does not
// read holds up no other.
func TestHub_FanOutKeepsOrder(t *testing.T) {
	for _, mode := range []string{"seq", "par"} {
		t.Run(mode, func(t *testing.T) {
			h := quietHub()
			const n, events = 200, 300
			got := make([][]string, n)
			var wg sync.WaitGroup
			clients := make([]*WSClient, n)
			for i := range clients {
				c := NewWSClient()
				clients[i] = c
				h.Register(c)
				wg.Go(func() {
					for m := range c.Send {
						got[i] = append(got[i], string(m))
					}
				})
			}
			stuck := NewWSClient() // reads nothing
			h.Register(stuck)
			defer setFanOut(h, mode)()

			var want []string
			for e := 0; e < events; {
				k := min(1+e%7, events-e) // batches of 1 to 7, and single events
				msgs := make([][]byte, k)
				for j := range msgs {
					msgs[j] = []byte(fmt.Sprint(e + j))
					want = append(want, fmt.Sprint(e+j))
				}
				if k == 1 {
					h.Broadcast(msgs[0])
				} else {
					h.broadcastBatch(msgs)
				}
				e += k
				if e%100 < k { // so that the clients that read keep up
					for _, c := range clients {
						for len(c.Send) > 0 {
							time.Sleep(time.Millisecond)
						}
					}
				}
			}
			for _, c := range clients {
				h.Unregister(c)
			}
			wg.Wait()
			for i := range got {
				if !slices.Equal(got[i], want) {
					t.Fatalf("client %d got %d events, want %d in order", i, len(got[i]), len(want))
				}
			}
			if !stuck.Missed() || len(stuck.Send) != wsSendBufferSize {
				t.Errorf("the client that does not read: missed %v, %d queued", stuck.missed.Load(), len(stuck.Send))
			}
			if h.ClientCount() != 1 {
				t.Errorf("%d clients", h.ClientCount())
			}
		})
	}
}
