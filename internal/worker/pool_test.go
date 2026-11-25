package worker

import (
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
	for p.busy.Load() < 2 {
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
