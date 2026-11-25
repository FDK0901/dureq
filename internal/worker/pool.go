package worker

import (
	"math"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
)

const (
	// atHand is how much work a pool with a limit keeps at hand, as what it puts back takes
	// up to about two seconds to come again.
	atHand = 3 * time.Second
	// maxAtHand is how many messages that are at most.
	maxAtHand = 1000
	// longMessage is how long messages take that a pool need not keep at hand if one takes
	// longer than atHand: there are few of them, and other nodes may have room.
	longMessage = 10 * time.Second

	// What is put back comes again after minReturn at the earliest, and in intervals of
	// maxReturnStep at most, so that a worker looks at no more than 200 a second, or as many
	// as it can process.
	minReturn     = 1 * time.Second
	maxReturnStep = 5 * time.Millisecond
)

// taskPool runs messages on a pool of goroutines and knows how many it has in hand. The
// pool itself does not: it counts a goroutine that has nothing to do until it expires.
type taskPool struct {
	pool  *ants.Pool
	freed chan<- struct{} // is sent to, without blocking, when a message is done

	mu       sync.Mutex
	busy     int
	waiting  []heldMessage
	closed   bool
	average  time.Duration // of a message
	returnAt time.Time     // when the last message that was put back comes again
}

// heldMessage is a message that a pool keeps at hand.
type heldMessage struct {
	sm  *streamMessage
	run func()
}

func newTaskPool(size int, freed chan<- struct{}) (*taskPool, error) {
	pool, err := ants.NewPool(size, ants.WithNonblocking(false))
	if err != nil {
		return nil, err
	}
	return &taskPool{pool: pool, freed: freed}, nil
}

// submit has task run as soon as there is room, and waits until then.
func (p *taskPool) submit(task func()) error {
	p.mu.Lock()
	p.busy++
	p.mu.Unlock()
	return p.start(task)
}

// offer has a message run, at once or when it is its turn, and reports false if there are
// enough others.
func (p *taskPool) offer(sm *streamMessage, run func()) bool {
	p.mu.Lock()
	switch {
	case p.closed:
		p.mu.Unlock()
		return false
	case p.busy < p.pool.Cap():
		p.busy++
		p.mu.Unlock()
		return p.start(run) == nil
	}
	defer p.mu.Unlock()
	if len(p.waiting) >= p.maxWaiting() {
		return false
	}
	p.waiting = append(p.waiting, heldMessage{sm, run})
	return true
}

func (p *taskPool) start(task func()) error {
	err := p.pool.Submit(func() {
		began := time.Now()
		defer func() { p.done(time.Since(began)) }()
		task()
	})
	if err != nil {
		p.done(0)
	}
	return err
}

func (p *taskPool) done(took time.Duration) {
	p.mu.Lock()
	if p.average == 0 {
		p.average = took
	} else if took > 0 {
		p.average += (took - p.average) / 8
	}
	if len(p.waiting) > 0 {
		next := p.waiting[0]
		p.waiting[0] = heldMessage{}
		p.waiting = p.waiting[1:]
		go p.start(next.run) // the goroutine that this runs on is not free yet
	} else {
		p.busy--
	}
	p.mu.Unlock()
	select {
	case p.freed <- struct{}{}:
	default:
	}
}

// maxWaiting returns how many messages the pool keeps at hand.
func (p *taskPool) maxWaiting() int {
	size := p.pool.Cap()
	if p.average <= 0 {
		return size
	}
	n := float64(size) * float64(atHand) / float64(p.average)
	if p.average <= longMessage {
		n = math.Ceil(n)
	}
	return int(min(n, maxAtHand))
}

// returnTimes returns when n messages that there is no room for should come again.
func (p *taskPool) returnTimes(n int) []time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	step := maxReturnStep
	if p.average > 0 {
		step = min(step, p.average/time.Duration(p.pool.Cap()))
	}
	at := time.Now().Add(minReturn)
	if at.Before(p.returnAt) {
		at = p.returnAt
	}
	times := make([]time.Time, n)
	for i := range times {
		at = at.Add(step)
		times[i] = at
	}
	p.returnAt = at
	return times
}

// held returns the messages that are kept at hand.
func (p *taskPool) held() []*streamMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.heldLocked()
}

func (p *taskPool) heldLocked() []*streamMessage {
	sms := make([]*streamMessage, len(p.waiting))
	for i, h := range p.waiting {
		sms[i] = h.sm
	}
	return sms
}

// close has the pool take no more messages, and returns those that were kept at hand.
func (p *taskPool) close() []*streamMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	sms := p.heldLocked()
	p.closed, p.waiting = true, nil
	return sms
}

// running returns how many messages are being processed.
func (p *taskPool) running() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return min(p.busy, p.pool.Cap())
}

// free returns how many more messages can be processed at once.
func (p *taskPool) free() int { return p.pool.Cap() - p.running() }

// room returns how many more messages offer takes.
func (p *taskPool) room() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return max(0, p.pool.Cap()-p.busy) + max(0, p.maxWaiting()-len(p.waiting))
}
