package ringbuf

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var items = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

func TestRingbufConstructor(t *testing.T) {
	capacities := []int{1, 2, 128}

	for _, n := range capacities {
		c := n
		t.Run(fmt.Sprintf("Capacity%d", n), func(t *testing.T) {
			rb := NewRingbuf[int](c)
			require.True(t, rb.Empty())
			require.False(t, rb.Full())
			require.Equal(t, 0, rb.Len())
			require.Len(t, rb.items, c+1)
			require.Equal(t, c, rb.Cap())
			require.Equal(t, c+1, cap(rb.items))
		})
	}
}

func TestRingbufLen(t *testing.T) {
	tests := []struct {
		name     string
		rb       Ringbuf[int]
		expected int
	}{
		{
			name: "Empty",
			rb:   Ringbuf[int]{head: 0, tail: 0, items: items},
		},
		{
			name:     "Len1AtStart",
			rb:       Ringbuf[int]{head: 0, tail: 1, items: items},
			expected: 1,
		},
		{
			name:     "Len1InMiddle",
			rb:       Ringbuf[int]{head: 4, tail: 5, items: items},
			expected: 1,
		},
		{
			name:     "Len1Overflow",
			rb:       Ringbuf[int]{head: 9, tail: 0, items: items},
			expected: 1,
		},
		{
			name:     "Len5AtStart",
			rb:       Ringbuf[int]{head: 0, tail: 5, items: items},
			expected: 5,
		},
		{
			name:     "Len5InMiddle",
			rb:       Ringbuf[int]{head: 3, tail: 8, items: items},
			expected: 5,
		},
		{
			name:     "Len5Overflow",
			rb:       Ringbuf[int]{head: 7, tail: 2, items: items},
			expected: 5,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.rb.Len())
		})
	}
}

func TestRingbufPopFront(t *testing.T) {
	tests := []struct {
		name            string
		rb              Ringbuf[int]
		expectedNewHead int
		expectedValue   int
		expectedOk      bool
	}{
		{
			name: "Empty",
			rb:   NewRingbuf[int](10),
		},
		{
			name:            "HeadAtFront",
			rb:              Ringbuf[int]{head: 0, tail: 5, items: items},
			expectedValue:   0,
			expectedOk:      true,
			expectedNewHead: 1,
		},
		{
			name:            "HeadInMiddle",
			rb:              Ringbuf[int]{head: 2, tail: 5, items: items},
			expectedValue:   2,
			expectedOk:      true,
			expectedNewHead: 3,
		},
		{
			name:            "HeadAtEnd",
			rb:              Ringbuf[int]{head: 9, tail: 3, items: items},
			expectedValue:   9,
			expectedOk:      true,
			expectedNewHead: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				length = test.rb.Len()
				v, ok  = test.rb.PopFront()
			)

			require.Equal(t, test.expectedOk, ok)
			require.Equal(t, test.expectedValue, v)
			require.Equal(t, test.expectedNewHead, test.rb.head)

			if length != 0 {
				require.Equal(t, length-1, test.rb.Len())
			}
		})
	}
}

func TestRingbufPopBack(t *testing.T) {
	tests := []struct {
		name            string
		rb              Ringbuf[int]
		expectedNewTail int
		expectedValue   int
		expectedOk      bool
	}{
		{
			name: "Empty",
			rb:   NewRingbuf[int](10),
		},
		{
			name:            "TailAtEnd",
			rb:              Ringbuf[int]{head: 0, tail: 9, items: items},
			expectedValue:   8,
			expectedOk:      true,
			expectedNewTail: 8,
		},
		{
			name:            "TailInMiddle",
			rb:              Ringbuf[int]{head: 2, tail: 5, items: items},
			expectedValue:   4,
			expectedOk:      true,
			expectedNewTail: 4,
		},
		{
			name:            "TailAtFront",
			rb:              Ringbuf[int]{head: 7, tail: 0, items: items},
			expectedValue:   9,
			expectedOk:      true,
			expectedNewTail: 9,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				length = test.rb.Len()
				v, ok  = test.rb.PopBack()
			)

			require.Equal(t, test.expectedOk, ok)
			require.Equal(t, test.expectedValue, v)
			require.Equal(t, test.expectedNewTail, test.rb.tail)

			if length != 0 {
				require.Equal(t, length-1, test.rb.Len())
			}
		})
	}
}

func TestRingbufPushFront(t *testing.T) {
	tests := []struct {
		name       string
		rb         Ringbuf[int]
		expectedRb Ringbuf[int]
	}{
		{
			name:       "Empty",
			rb:         NewRingbuf[int](10),
			expectedRb: Ringbuf[int]{head: 10, tail: 0, items: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 147}},
		},
		{
			name:       "HeadInMiddle",
			rb:         Ringbuf[int]{head: 3, tail: 7, items: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
			expectedRb: Ringbuf[int]{head: 2, tail: 7, items: []int{0, 0, 147, 0, 0, 0, 0, 0, 0, 0}},
		},
		{
			name:       "HeadAtEnd",
			rb:         Ringbuf[int]{head: 9, tail: 2, items: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
			expectedRb: Ringbuf[int]{head: 8, tail: 2, items: []int{0, 0, 0, 0, 0, 0, 0, 0, 147, 0}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			length := test.rb.Len()

			test.rb.PushFront(147)
			require.Equal(t, test.expectedRb, test.rb)
			require.Equal(t, length+1, test.rb.Len())
		})
	}
}

func TestRingbufPushBack(t *testing.T) {
	tests := []struct {
		name       string
		rb         Ringbuf[int]
		expectedRb Ringbuf[int]
	}{
		{
			name:       "Empty",
			rb:         NewRingbuf[int](10),
			expectedRb: Ringbuf[int]{head: 0, tail: 1, items: []int{147, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		},
		{
			name:       "TailInMiddle",
			rb:         Ringbuf[int]{head: 3, tail: 7, items: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
			expectedRb: Ringbuf[int]{head: 3, tail: 8, items: []int{0, 0, 0, 0, 0, 0, 0, 147, 0, 0}},
		},
		{
			name:       "TailAtEnd",
			rb:         Ringbuf[int]{head: 3, tail: 9, items: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
			expectedRb: Ringbuf[int]{head: 3, tail: 0, items: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 147}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			length := test.rb.Len()

			test.rb.PushBack(147)
			require.Equal(t, test.expectedRb, test.rb)
			require.Equal(t, length+1, test.rb.Len())
		})
	}
}

func TestRingbufIter(t *testing.T) {
	tests := []struct {
		name          string
		rb            Ringbuf[int]
		expectedOrder []int
	}{
		{
			name:          "Empty",
			rb:            Ringbuf[int]{head: 0, tail: 0, items: items},
			expectedOrder: []int{},
		},
		{
			name:          "Len1",
			rb:            Ringbuf[int]{head: 0, tail: 1, items: items},
			expectedOrder: []int{0},
		},
		{
			name:          "FullInOrder",
			rb:            Ringbuf[int]{head: 0, tail: 9, items: items},
			expectedOrder: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			name:          "HeadInMiddle",
			rb:            Ringbuf[int]{head: 2, tail: 5, items: items},
			expectedOrder: []int{2, 3, 4},
		},
		{
			name:          "HeadAtEnd",
			rb:            Ringbuf[int]{head: 9, tail: 5, items: items},
			expectedOrder: []int{9, 0, 1, 2, 3, 4},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				items = make([]int, 0)
				fn    = func(v int) {
					items = append(items, v)
				}
			)

			test.rb.Iter(fn)
			require.Equal(t, test.expectedOrder, items)
		})
	}
}

func TestRingbuf(t *testing.T) {
	rb := NewRingbuf[int](10)

	for i := 0; i < 5; i++ {
		require.True(t, rb.PushBack(i))
	}

	require.Equal(t, 5, rb.Len())

	v, ok := rb.PopFront()
	require.True(t, ok)
	require.Equal(t, 0, v)

	v, ok = rb.PopBack()
	require.True(t, ok)
	require.Equal(t, 4, v)

	for i := 0; i > -3; i-- {
		require.True(t, rb.PushFront(i))
	}

	require.Equal(t, 6, rb.Len())

	for i := 4; i < 8; i++ {
		require.True(t, rb.PushBack(i))
	}

	require.Equal(t, 10, rb.Len())
	require.False(t, rb.PushBack(147))
	require.False(t, rb.PushFront(147))

	for i := 0; i < 10; i++ {
		_, ok := rb.PopBack()
		require.True(t, ok)
	}
}
