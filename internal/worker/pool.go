package worker

import (
	"sync/atomic"

	"github.com/panjf2000/ants/v2"
)

// taskPool runs messages on a pool of goroutines and knows how many it has in hand. The
// pool itself does not: it counts a goroutine that has nothing to do until it expires.
type taskPool struct {
	pool  *ants.Pool
	busy  atomic.Int64
	freed chan<- struct{} // is sent to, without blocking, when a message is done
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
	p.busy.Add(1)
	err := p.pool.Submit(func() {
		defer p.done()
		task()
	})
	if err != nil {
		p.done()
	}
	return err
}

func (p *taskPool) done() {
	p.busy.Add(-1)
	select {
	case p.freed <- struct{}{}:
	default:
	}
}

// running returns how many messages are being processed.
func (p *taskPool) running() int { return min(int(p.busy.Load()), p.pool.Cap()) }

// free returns how many more messages can be processed at once.
func (p *taskPool) free() int { return p.pool.Cap() - p.running() }
