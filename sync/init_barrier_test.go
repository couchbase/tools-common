package sync

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewInitBarrier(t *testing.T) {
	barrier := NewInitBarrier()
	require.Len(t, barrier, 1)
}

func TestInitBarrierWait(t *testing.T) {
	barrier := NewInitBarrier()
	require.True(t, barrier.Wait())

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		require.False(t, barrier.Wait())
	}()

	barrier.Success()

	wg.Wait()

	require.False(t, barrier.Wait())
}

func TestInitBarrierWaitWithFailure(t *testing.T) {
	barrier := NewInitBarrier()
	require.True(t, barrier.Wait())

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		require.True(t, barrier.Wait())
		barrier.Success()
	}()

	barrier.Failed()

	wg.Wait()

	require.False(t, barrier.Wait())
}
