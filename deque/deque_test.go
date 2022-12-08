package deque

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDequeGrow(t *testing.T) {
	d := NewDequeWithCapacity[int](2)
	require.Equal(t, 0, d.Len())

	d.PushBack(1)
	d.PushBack(2)
	require.Equal(t, 2, d.Len())

	d.PushBack(3)
	require.Equal(t, 3, d.Len())

	for i := 1; i <= 3; i++ {
		v, ok := d.PopFront()
		require.True(t, ok)
		require.Equal(t, i, v)
	}

	require.Equal(t, 0, d.Len())
}

func TestDequePushPop(t *testing.T) {
	d := NewDeque[int]()

	tests := []struct {
		name string
		push func(v int)
		pop  func() (int, bool)
	}{
		{
			name: "Back",
			push: d.PushBack,
			pop:  d.PopBack,
		},
		{
			name: "Front",
			push: d.PushFront,
			pop:  d.PopFront,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i < 10; i++ {
				test.push(i)
			}

			require.Equal(t, 10, d.Len())

			for i := 9; i >= 0; i-- {
				require.Equal(t, i+1, d.Len())

				v, ok := test.pop()
				require.True(t, ok)
				require.Equal(t, i, v)
			}
		})

		d.Clear()
	}
}

func TestDequeIter(t *testing.T) {
	d := NewDeque[int]()

	for i := 0; i < 10; i++ {
		d.PushBack(i)
	}

	i := 0

	d.Iter(func(v int) {
		require.Equal(t, i, v)
		i++
	})
}
