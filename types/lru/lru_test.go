package lru

import (
	"container/list"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheNew(t *testing.T) {
	expected := &Cache[int, string]{
		capacity: 42,
		list:     list.New(),
		elements: make(map[int]*list.Element),
	}

	require.Equal(t, expected, New[int, string](42))
}

func TestCacheGeneralUse(t *testing.T) {
	cache := New[string, string](42)

	require.Equal(t, 0, cache.list.Len())
	require.Len(t, cache.elements, 0)

	require.False(t, cache.Set("key", "value"))
	require.Equal(t, 1, cache.list.Len())
	require.Len(t, cache.elements, 1)

	require.True(t, cache.Set("key", "value"))
	require.Equal(t, 1, cache.list.Len())
	require.Len(t, cache.elements, 1)

	require.True(t, cache.Has("key"))

	val, ok := cache.Get("key")
	require.Equal(t, "value", val)
	require.True(t, ok)

	require.True(t, cache.Delete("key"))
	require.Equal(t, 0, cache.list.Len())
	require.Len(t, cache.elements, 0)

	require.False(t, cache.Delete("key"))
	require.Zero(t, cache.list.Len())
	require.Len(t, cache.elements, 0)
}

func TestCacheOverCapacity(t *testing.T) {
	cache := New[string, string](2)

	require.False(t, cache.Set("key1", "value1"))
	require.Equal(t,
		&item[string, string]{key: "key1", value: "value1"}, cache.list.Front().Value.(*item[string, string]))

	require.False(t, cache.Set("key2", "value2"))
	require.Equal(t,
		&item[string, string]{key: "key2", value: "value2"}, cache.list.Front().Value.(*item[string, string]))

	require.False(t, cache.Set("key3", "value3"))
	require.Equal(t,
		&item[string, string]{key: "key3", value: "value3"}, cache.list.Front().Value.(*item[string, string]))

	require.False(t, cache.Has("key1"))

	val, ok := cache.Get("key1")
	require.Zero(t, val)
	require.False(t, ok)

	require.True(t, cache.Has("key2"))
	require.True(t, cache.Has("key3"))

	require.Equal(t, 2, cache.list.Len())
	require.Len(t, cache.elements, 2)
}

func TestCacheForEach(t *testing.T) {
	cache := New[string, string](42)

	for i := 1; i <= 84; i++ {
		require.False(t, cache.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
	}

	// Should come out in reverse order
	i := 84

	err := cache.ForEach(func(key, value string) error {
		require.Equal(t, fmt.Sprintf("key%d", i), key)
		require.Equal(t, fmt.Sprintf("value%d", i), value)
		i--
		return nil
	})
	require.NoError(t, err)
}

func TestCacheForEachPropagateUserError(t *testing.T) {
	cache := New[string, string](42)

	for i := 1; i <= 84; i++ {
		require.False(t, cache.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
	}

	var called int

	err := cache.ForEach(func(key, value string) error { called++; return assert.AnError })
	require.ErrorIs(t, err, assert.AnError)
	require.Equal(t, 1, called)
}

func BenchmarkCacheSetSameKey(b *testing.B) {
	cache := New[string, string](1024)

	for i := 0; i < b.N; i++ {
		cache.Set("key", "value")
	}
}

func BenchmarkCacheDifferentKey(b *testing.B) {
	cache := New[string, string](1024)

	for i := 0; i < b.N; i++ {
		cache.Set("key"+strconv.Itoa(i), "value")
	}
}

func BenchmarkCacheEviction(b *testing.B) {
	cache := New[string, string](0)

	for i := 0; i < b.N; i++ {
		cache.Set("key", "value")
	}
}
