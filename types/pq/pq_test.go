package pq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPriorityQueue(t *testing.T) {
	var (
		expected = &PriorityQueue[int]{inner: make(pq[int], 0, 42)}
		actual   = NewPriorityQueue[int](42)
	)

	require.Equal(t, expected, actual)
	require.Equal(t, 42, cap(actual.inner))
}

func TestPriorityQueueEnqueueDequeueNoPriority(t *testing.T) {
	queue := NewPriorityQueue[int](5)

	for i := 0; i < 5; i++ {
		queue.Enqueue(Item[int]{Payload: i})
	}

	require.Equal(t, 5, queue.Len())

	var (
		expected = map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}, 4: {}}
		actual   = make(map[int]struct{})
	)

	require.NoError(t, queue.Drain(func(item Item[int]) error { actual[item.Payload] = struct{}{}; return nil }))
	require.Equal(t, expected, actual)
}

func TestPriorityQueueEnqueueDequeueWithPriority(t *testing.T) {
	queue := NewPriorityQueue[int](5)

	for i := 0; i < 5; i++ {
		queue.Enqueue(Item[int]{Payload: i, Priority: i})
	}

	require.Equal(t, 5, queue.Len())

	var (
		expected = []int{4, 3, 2, 1, 0}
		actual   = make([]int, 0, 5)
	)

	require.NoError(t, queue.Drain(func(item Item[int]) error { actual = append(actual, item.Payload); return nil }))
	require.Equal(t, expected, actual)
}

func TestPriorityQueueDrainNoItems(t *testing.T) {
	queue := NewPriorityQueue[int](5)

	var run bool

	require.NoError(t, queue.Drain(func(item Item[int]) error { run = true; return nil }))
	require.False(t, run)
}

func TestPriorityQueueDrainWithError(t *testing.T) {
	queue := NewPriorityQueue[int](5)

	var run int

	err := queue.Drain(func(item Item[int]) error { run++; return assert.AnError })
	require.NoError(t, err)
	require.Zero(t, run)

	for i := 0; i < 5; i++ {
		queue.Enqueue(Item[int]{Payload: i})
	}

	err = queue.Drain(func(item Item[int]) error { run++; return assert.AnError })
	require.ErrorIs(t, err, assert.AnError)
	require.Equal(t, 1, run)
}
