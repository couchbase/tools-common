// Package lru implements a constant time, generic LRU cache.
package lru

import (
	"container/list"
)

// item is a wrapper type used to track key/value pairs.
type item struct {
	key, value interface{}
}

// Cache exposes an interface for an LRU cache.
type Cache struct {
	capacity int
	list     *list.List
	elements map[interface{}]*list.Element
}

// New returns a new cache with the given capacity.
func New(capacity uint) *Cache {
	return &Cache{
		capacity: int(capacity),
		list:     list.New(),
		elements: make(map[interface{}]*list.Element),
	}
}

// Get returns the value for the given key if it exists in the cache.
func (c *Cache) Get(key interface{}) (interface{}, bool) {
	element, ok := c.elements[key]
	if !ok {
		return nil, false
	}

	return element.Value.(*item).value, true
}

// Set the value for the given key, returns a boolean indicating whether the key was already in the cache.
func (c *Cache) Set(key, value interface{}) bool {
	ok := c.set(key, value)

	if c.list.Len() <= c.capacity {
		return ok
	}

	c.del(c.list.Back())

	return ok
}

// set adds the key/value pair to the cache.
func (c *Cache) set(key, value interface{}) bool {
	element, ok := c.elements[key]
	if !ok {
		c.elements[key] = c.list.PushFront(&item{key: key, value: value})

		return false
	}

	element.Value.(*item).value = value

	c.list.MoveToFront(element)

	return true
}

// Delete the given key from the cache.
func (c *Cache) Delete(key interface{}) bool {
	element, ok := c.elements[key]
	if !ok {
		return false
	}

	c.del(element)

	return true
}

// del removes the given list element from the cache.
func (c *Cache) del(element *list.Element) {
	c.list.Remove(element)
	delete(c.elements, element.Value.(*item).key)
}

// Has returns a boolean indicating whether the cache contains the given key.
func (c *Cache) Has(key interface{}) bool {
	_, ok := c.elements[key]
	return ok
}

// ForEach runs the given function for each key/value pair in the cache, iteration takes place with most used first.
func (c *Cache) ForEach(fn func(key, value interface{}) error) error {
	for e := c.list.Front(); e != nil; e = e.Next() {
		err := fn(e.Value.(*item).key, e.Value.(*item).value)
		if err != nil {
			return err
		}
	}

	return nil
}
