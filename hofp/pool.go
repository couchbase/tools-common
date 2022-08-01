// Package hofp exposes a generic higher order function pool which abstracts aways the logic/error handling required to
// perform tasks concurrently by wrapping complex tasks into a common 'func(context.Context) error' interface.
package hofp

import (
	"context"
	"sync"

	"github.com/couchbase/tools-common/log"
)

// Function is a higher order function to be executed by the worker pool, where possible, the function should honor the
// cancellation of the given context and return as quickly/cleanly as possible.
type Function func(ctx context.Context) error

// Pool is a generic higher order function worker pool which executes the provided functions concurrently using a
// configurable number of workers.
//
// NOTE: Fails fast in the event of an error, subsequent attempts to use the worker pool will return the error which
// caused the pool to stop processing requests.
type Pool struct {
	opts Options

	hofs chan Function
	err  error

	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	cleanup sync.Once

	lock sync.RWMutex
}

// NewPool returns a new higher order function worker pool with the provided number of workers.
func NewPool(opts Options) *Pool {
	// Fill out any missing fields with the sane defaults
	opts.defaults()

	ctx, cancel := context.WithCancel(opts.Context)

	pool := &Pool{
		opts:   opts,
		hofs:   make(chan Function, opts.Size*opts.BufferMultiplier),
		ctx:    ctx,
		cancel: cancel,
	}

	pool.wg.Add(opts.Size)

	for w := 0; w < opts.Size; w++ {
		go pool.work()
	}

	return pool
}

// work will process the provided functions until it hits the first error, at which point the pool will begin teardown.
func (p *Pool) work() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case fn, ok := <-p.hofs:
			if !ok {
				return
			}

			err := fn(p.ctx)

			if err == nil {
				continue
			}

			// The worker pool may already be tearing down, in which case we should log the function error so that it's
			// not missed whilst debugging.
			if !p.setErr(err) {
				log.Errorf("%s Failed to execute function: %v", p.opts.LogPrefix, err)
			}

			return
		}
	}
}

// Size returns the number of workers in the pool.
func (p *Pool) Size() int {
	return p.opts.Size
}

// Queue a function for execution by the worker pool, returns an error if the worker pool has encountered an error and
// is tearing down. This return value should be used to prematurely stop queuing work, or to initiate teardown of the
// wrapping workload.
func (p *Pool) Queue(fn Function) error {
	if err := p.getErr(); err != nil {
		return err
	}

	select {
	case p.hofs <- fn:
	case <-p.ctx.Done():
	}

	return p.getErr()
}

// Stop the worker pool gracefully executing any remaining functions. Subsequent calls to 'Stop' will only return the
// error which caused the pool to tear down (if there was one).
func (p *Pool) Stop() error {
	p.cleanup.Do(func() {
		close(p.hofs)
		p.wg.Wait()
		p.cancel()
	})

	return p.getErr()
}

// getErr is thread safe getter for the internal error attribute.
func (p *Pool) getErr() error {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.err
}

// setErr is a thread safe setter for the internal error attribute, returns a boolean indicating if this is the first
// error which indicates that the worker pool has begun tear down.
func (p *Pool) setErr(err error) bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	// This is a secondary error, we're already tearing down ignore the request
	if p.err != nil {
		return false
	}

	// Set the error and begin teardown
	p.err = err
	p.cancel()

	return true
}
