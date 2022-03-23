// Package lru implements a constant time, generic LRU cache.
package lru

import (
	"container/list"

	"golang.org/x/exp/constraints"
)

// item is a wrapper type used to track key/value pairs.
type item[K constraints.Ordered, V any] struct {
	key   K
	value V
}

// Cache exposes an interface for an LRU cache.
type Cache[K constraints.Ordered, V any] struct {
	capacity int
	list     *list.List
	elements map[K]*list.Element
}

// New returns a new cache with the given capacity.
func New[K constraints.Ordered, V any](capacity uint) *Cache[K, V] {
	return &Cache[K, V]{
		capacity: int(capacity),
		list:     list.New(),
		elements: make(map[K]*list.Element),
	}
}

// Get returns the value for the given key if it exists in the cache.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	element, ok := c.elements[key]
	if !ok {
		return *new(V), false
	}

	return element.Value.(*item[K, V]).value, true
}

// Set the value for the given key, returns a boolean indicating whether the key was already in the cache.
func (c *Cache[K, V]) Set(key K, value V) bool {
	ok := c.set(key, value)

	if c.list.Len() <= c.capacity {
		return ok
	}

	c.del(c.list.Back())

	return ok
}

// set adds the key/value pair to the cache.
func (c *Cache[K, V]) set(key K, value V) bool {
	element, ok := c.elements[key]
	if !ok {
		c.elements[key] = c.list.PushFront(&item[K, V]{key: key, value: value})

		return false
	}

	element.Value.(*item[K, V]).value = value

	c.list.MoveToFront(element)

	return true
}

// Delete the given key from the cache.
func (c *Cache[K, V]) Delete(key K) bool {
	element, ok := c.elements[key]
	if !ok {
		return false
	}

	c.del(element)

	return true
}

// del removes the given list element from the cache.
func (c *Cache[K, V]) del(element *list.Element) {
	c.list.Remove(element)
	delete(c.elements, element.Value.(*item[K, V]).key)
}

// Has returns a boolean indicating whether the cache contains the given key.
func (c *Cache[K, V]) Has(key K) bool {
	_, ok := c.elements[key]
	return ok
}

// ForEach runs the given function for each key/value pair in the cache, iteration takes place with most used first.
func (c *Cache[K, V]) ForEach(fn func(key K, value V) error) error {
	for e := c.list.Front(); e != nil; e = e.Next() {
		err := fn(e.Value.(*item[K, V]).key, e.Value.(*item[K, V]).value)
		if err != nil {
			return err
		}
	}

	return nil
}
