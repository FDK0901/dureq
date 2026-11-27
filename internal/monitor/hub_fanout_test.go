package monitor

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

func init() {
	setFanOut = func(h *hub, mode string) func() {
		switch mode {
		case "seq": // as by default
			h.setParallel(0, 0)
		case "par": // also with one processor, to see what it costs there
			h.setParallel(1, 1)
		}
		return func() { h.setParallel(0, 0) }
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

// By default the hub sends in sequence, as v0.3.2 did, and allocates nothing for it. Asked to,
// it sends in parallel with 4 processors and 1,000 deliveries at least, and keeps its clients
// when it is turned on and off.
func TestHub_ParallelWhenAskedAndItPays(t *testing.T) {
	for _, procs := range []int{1, 2, 4, 8} {
		old := runtime.GOMAXPROCS(procs)
		h := quietHub()
		clients := make([]*WSClient, 5000)
		for i := range clients {
			clients[i] = NewWSClient()
			h.Register(clients[i]) // nothing reads: what is sent fills the queues, then is missed
		}
		msg := []byte("e")
		if h.parallel(1) {
			t.Errorf("GOMAXPROCS %d: parallel by default", procs)
		}
		if allocs := testing.AllocsPerRun(100, func() { h.Broadcast(msg) }); allocs != 0 {
			t.Errorf("GOMAXPROCS %d: a fan-out by default allocates %.0f times", procs, allocs)
		}
		h.setParallel(DefaultHubParallelMinProcs, DefaultHubParallelMinDeliveries)
		if got, want := h.parallel(1), procs >= 4; got != want {
			t.Errorf("GOMAXPROCS %d, asked to, one event to 5,000: parallel %v, want %v", procs, got, want)
		}
		if h.ClientCount() != len(clients) {
			t.Errorf("%d clients after it was turned on", h.ClientCount())
		}
		h.setParallel(0, 0)
		if h.parallel(1) || h.ClientCount() != len(clients) {
			t.Errorf("turned off: parallel %v, %d clients", h.parallel(1), h.ClientCount())
		}
		for _, c := range clients {
			h.Unregister(c)
		}
		if h.ClientCount() != 0 {
			t.Errorf("%d clients left", h.ClientCount())
		}
		runtime.GOMAXPROCS(old)
	}

	h := quietHub()
	for range 600 {
		h.Register(NewWSClient())
	}
	svc := &APIService{hub: h}
	svc.SetHubParallel(1, 500)
	if !h.parallel(1) {
		t.Error("asked to go parallel with 1 processor and 500 deliveries, it does not")
	}
	svc.SetHubParallel(0, 0)
	if h.parallel(1) {
		t.Error("asked to stop, it goes on")
	}
}
