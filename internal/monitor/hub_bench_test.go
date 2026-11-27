package monitor

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
)

// setFanOut makes the hub fan out one way, "seq" or "par", in versions that can, and returns
// what undoes it. A version without it fans out as it does.
var setFanOut func(h *hub, mode string) func()

// broadcastEach hands the events of one read of the event stream to the hub as its reader does:
// all at once where it can, else one by one.
func broadcastEach(h *hub, msgs [][]byte) {
	if bb, ok := any(h).(interface{ broadcastBatch([][]byte) }); ok {
		bb.broadcastBatch(msgs)
		return
	}
	for _, m := range msgs {
		h.Broadcast(m)
	}
}

// BenchmarkHubBroadcast is what fanning events out to N WebSocket clients costs, each of which
// has a goroutine that takes what it gets, as the write pump of a connection does. batch=1 is
// one event (Broadcast), batch=100 a read of the event stream that brought 100.
//
// It needs no Redis. With DUREQ_HUB_BENCH_MODES=1, a version that can also runs it sequential
// (mode=seq) and in parallel (mode=par) at every N.
//
// Give the processors with GOMAXPROCS, one process each, not with -cpu: testing runs a
// sub-benchmark that uses b.Loop, the first time in a process, for the first value of -cpu with
// the GOMAXPROCS of the last value. The metric procs says which it was.
//
//	GOMAXPROCS=2 go test -run '^$' -bench BenchmarkHubBroadcast -benchmem -count 10 ./internal/monitor/
func BenchmarkHubBroadcast(b *testing.B) {
	modes := []string{""}
	if os.Getenv("DUREQ_HUB_BENCH_MODES") == "1" && setFanOut != nil {
		modes = append(modes, "seq", "par")
	}
	msg := []byte(`{"type":"job.completed","job_id":"d4f1jglh2hivgkds0","run_id":"d4f1jglh2hivgkdsg","task_type":"send","status":"completed","timestamp":"2026-11-26T22:30:00.123456789+09:00"}`)
	for _, n := range []int{100, 1000, 5000, 10000} {
		for _, batch := range []int{1, 100} {
			msgs := make([][]byte, batch)
			for i := range msgs {
				msgs[i] = msg
			}
			for _, mode := range modes {
				name := fmt.Sprintf("clients=%d/batch=%d", n, batch)
				if mode != "" {
					name += "/mode=" + mode
				}
				b.Run(name, func(b *testing.B) {
					h := quietHub()
					clients := make([]*WSClient, n)
					var wg sync.WaitGroup
					for i := range clients {
						c := NewWSClient()
						clients[i] = c
						h.Register(c)
						wg.Go(func() {
							for range c.Send {
							}
						})
					}
					if mode != "" {
						defer setFanOut(h, mode)()
					}
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if batch == 1 {
							h.Broadcast(msg)
						} else {
							broadcastEach(h, msgs)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n*batch), "ns/delivery")
					b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "procs")
					for _, c := range clients {
						h.Unregister(c)
					}
					wg.Wait()
				})
			}
		}
	}
}
