package syncutil

// InitBarrier wraps around a channel to expose an interface to use the channel as an initialization barrier.
type InitBarrier chan struct{}

// NewInitBarrier creates a new populated initialization barrier which will allow a single thread to perform
// initialization.
func NewInitBarrier() InitBarrier {
	barrier := make(chan struct{}, 1)
	barrier <- struct{}{}

	return barrier
}

// Wait will wait for access to whatever the initialization barrier is guarding, returns a boolean indicating whether
// the calling thread has exclusive access i.e. is the initializing thread.
func (i InitBarrier) Wait() bool {
	_, ok := <-i

	return ok
}

// Failed indicates that initialization failed in some way, another thread will be allowed to attempt initialization.
func (i InitBarrier) Failed() {
	i <- struct{}{}
}

// Success indicates that the initialization was a success, all threads blocking on 'Wait' will now be unblocked.
func (i InitBarrier) Success() {
	close(i)
}
