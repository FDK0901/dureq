package monitor

import (
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

func init() {
	setFanOut = func(h *hub, mode string) func() {
		procs, deliveries := h.parallelProcs.Load(), h.parallelDeliveries.Load()
		switch mode {
		case "seq":
			h.parallelDeliveries.Store(math.MaxInt64)
		case "par":
			// also with one processor, to see what it costs there
			h.parallelProcs.Store(1)
			h.parallelDeliveries.Store(1)
		}
		return func() { h.parallelProcs.Store(procs); h.parallelDeliveries.Store(deliveries) }
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

// The hub sends in parallel only with 4 processors and 1,000 deliveries at least, unless it is
// told otherwise; in sequence, the fan-out does not allocate.
func TestHub_ParallelWhenItPays(t *testing.T) {
	for _, procs := range []int{1, 2, 4, 8} {
		old := runtime.GOMAXPROCS(procs)
		h := quietHub()
		clients := make([]*WSClient, 5000)
		for i := range clients {
			clients[i] = NewWSClient()
			h.Register(clients[i]) // nothing reads: what is sent fills the queues, then is missed
		}
		if got, want := h.parallel(1), procs >= 4; got != want {
			t.Errorf("GOMAXPROCS %d, one event to 5,000: parallel %v, want %v", procs, got, want)
		}
		if h.parallel(1) && len(clients) < 1000 {
			t.Error("parallel with fewer deliveries")
		}
		msg := []byte("e")
		if allocs := testing.AllocsPerRun(100, func() { h.Broadcast(msg) }); !h.parallel(1) && allocs != 0 {
			t.Errorf("GOMAXPROCS %d: a fan-out in sequence allocates %.0f times", procs, allocs)
		}
		for _, c := range clients {
			h.Unregister(c)
		}
		runtime.GOMAXPROCS(old)
	}

	h := quietHub()
	for range 600 {
		h.Register(NewWSClient())
	}
	h.procs.Store(2)
	if h.parallel(1) {
		t.Error("600 deliveries with 2 processors: parallel by default")
	}
	svc := &APIService{hub: h}
	svc.SetHubParallel(2, 500)
	if !h.parallel(1) {
		t.Error("told to go parallel with 2 processors and 500 deliveries, it does not")
	}
	svc.SetHubParallel(0, 0) // keeps what it is
	if !h.parallel(1) {
		t.Error("0 changed it")
	}
}
