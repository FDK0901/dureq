package worker

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// A pool has room as soon as its messages are done, not only once its goroutines expire.
func TestTaskPool_RoomIsFreeAtOnce(t *testing.T) {
	freed := make(chan struct{}, 1)
	p, err := newTaskPool(2, freed)
	if err != nil {
		t.Fatal(err)
	}
	defer p.pool.Release()

	for range 100 {
		release := make(chan struct{})
		var started sync.WaitGroup
		started.Add(2)
		for range 2 {
			if err := p.submit(func() { started.Done(); <-release }); err != nil {
				t.Fatal(err)
			}
		}
		started.Wait()
		if p.running() != 2 || p.free() != 0 {
			t.Fatalf("running %d, free %d with 2 of 2 messages", p.running(), p.free())
		}
		close(release)
		for p.free() < 2 {
			select {
			case <-freed:
			case <-time.After(5 * time.Second):
				t.Fatalf("free %d after the messages were done", p.free())
			}
		}
	}
}

// What waits for room is not counted as running.
func TestTaskPool_Waiting(t *testing.T) {
	p, err := newTaskPool(1, make(chan struct{}, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.pool.Release()

	release := make(chan struct{})
	p.submit(func() { <-release })
	submitted := make(chan struct{})
	go func() {
		p.submit(func() {})
		close(submitted)
	}()
	for {
		p.mu.Lock()
		busy := p.busy
		p.mu.Unlock()
		if busy == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if p.running() != 1 || p.free() != 0 {
		t.Fatalf("running %d, free %d", p.running(), p.free())
	}
	close(release)
	<-submitted
}

// The pool takes messages again after a panic.
func TestTaskPool_Panic(t *testing.T) {
	freed := make(chan struct{}, 1)
	p, err := newTaskPool(1, freed)
	if err != nil {
		t.Fatal(err)
	}
	defer p.pool.Release()
	p.submit(func() { panic("boom") })
	select {
	case <-freed:
	case <-time.After(5 * time.Second):
		t.Fatal("not freed")
	}
	if p.free() != 1 {
		t.Fatalf("free %d", p.free())
	}
}

func message(id string) *streamMessage { return &streamMessage{StreamMsgID: id} }

// A pool with a limit keeps a few messages at hand and runs them in their order, and does not
// take more.
func TestTaskPool_Offer(t *testing.T) {
	freed := make(chan struct{}, 1)
	p, err := newTaskPool(2, freed)
	if err != nil {
		t.Fatal(err)
	}
	defer p.pool.Release()

	var mu sync.Mutex
	var ran []string
	release := make(chan struct{})
	offer := func(id string) bool {
		return p.offer(message(id), func() {
			<-release
			mu.Lock()
			defer mu.Unlock()
			ran = append(ran, id)
		})
	}
	if p.room() != 4 {
		t.Errorf("room for %d, want 4", p.room())
	}
	var taken []string
	for i := range 7 {
		if id := fmt.Sprint(i); offer(id) {
			taken = append(taken, id)
		}
	}
	if !slices.Equal(taken, []string{"0", "1", "2", "3"}) || p.room() != 0 || p.running() != 2 || len(p.held()) != 2 {
		t.Errorf("took %v, room for %d, %d running, %d held", taken, p.room(), p.running(), len(p.held()))
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for p.free() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("%d free, %d held", p.free(), len(p.held()))
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	slices.Sort(ran)
	if !slices.Equal(ran, taken) {
		t.Errorf("ran %v, want %v", ran, taken)
	}
	mu.Unlock()
}

// What is kept at hand is given back when the pool is closed, and it takes no more.
func TestTaskPool_Close(t *testing.T) {
	p, err := newTaskPool(1, make(chan struct{}, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.pool.Release()
	release := make(chan struct{})
	p.offer(message("running"), func() { <-release })
	p.offer(message("held"), func() { t.Error("ran") })
	held := p.close()
	if len(held) != 1 || held[0].StreamMsgID != "held" {
		t.Errorf("held %v", held)
	}
	if p.offer(message("late"), func() { t.Error("ran") }) {
		t.Error("took a message after it was closed")
	}
	close(release)
	for p.free() < 1 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
}

func TestTaskPool_MaxWaiting(t *testing.T) {
	for _, tc := range []struct {
		size    int
		average time.Duration
		want    int
	}{
		{2, 0, 2},                       // not known yet
		{2, 100 * time.Millisecond, 60}, // three seconds of work
		{1, 2 * time.Second, 2},
		{1, 5 * time.Second, 1}, // the next one
		{1, time.Hour, 0},       // other nodes may have room
		{100, 30 * time.Second, 10},
		{100, time.Millisecond, 1000}, // no more than that
		{5, time.Nanosecond, 1000},
	} {
		p, err := newTaskPool(tc.size, make(chan struct{}, 1))
		if err != nil {
			t.Fatal(err)
		}
		p.average = tc.average
		if got := p.maxWaiting(); got != tc.want {
			t.Errorf("%d at once of %v: %d at hand, want %d", tc.size, tc.average, got, tc.want)
		}
		p.pool.Release()
	}
}

// What is put back comes again one after the other, at 200 a second, or as fast as it can be
// processed.
func TestTaskPool_ReturnTimes(t *testing.T) {
	for _, tc := range []struct{ average, step time.Duration }{
		{0, 5 * time.Millisecond}, {time.Minute, 5 * time.Millisecond}, {200 * time.Microsecond, 100 * time.Microsecond},
	} {
		p, err := newTaskPool(2, make(chan struct{}, 1))
		if err != nil {
			t.Fatal(err)
		}
		p.average = tc.average
		start := time.Now()
		times := append(p.returnTimes(1500), p.returnTimes(1500)...)
		if first := times[0].Sub(start); first < time.Second || first > 1200*time.Millisecond {
			t.Errorf("average %v: the first one comes after %v", tc.average, first)
		}
		for i := 1; i < len(times); i++ {
			if d := times[i].Sub(times[i-1]); d != tc.step {
				t.Errorf("average %v: %v between two, want %v", tc.average, d, tc.step)
				break
			}
		}
		p.pool.Release()
	}
}
