package cache

import (
	"sync"
)

type Cache[T interface{}] struct {
	cache map[string]T
	mutex sync.RWMutex
}

func New[T interface{}]() *Cache[T] {
	return &Cache[T]{
		cache: make(map[string]T),
	}
}

func (c *Cache[T]) Size() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return len(c.cache)
}

func (c *Cache[T]) Remove(streamName string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.cache, streamName)
}

func (c *Cache[T]) Get(streamName string) T {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	info, ok := c.cache[streamName]
	if ok {
		return info
	}
	var zero T
	return zero
}

func (c *Cache[T]) Store(streamName string, value T) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cache[streamName] = value
}

func (c *Cache[T]) UnittestIntrospection() *map[string]T {
	return &c.cache
}
