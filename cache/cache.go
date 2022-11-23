package cache

import (
	"sync"

	"github.com/livepeer/catalyst-api/log"
)

type Cache[T interface{}] struct {
	cache map[string]T
	mutex sync.Mutex
}

func New[T interface{}]() *Cache[T] {
	return &Cache[T]{
		cache: make(map[string]T),
	}
}

func (c *Cache[T]) Remove(requestID, streamName string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.cache, streamName)
	log.Log(requestID, "Deleting from Segmenting Cache", "stream_name", streamName)
}

func (c *Cache[T]) Get(streamName string) T {
	c.mutex.Lock()
	defer c.mutex.Unlock()
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
	// log.Log(streamInfo.RequestID, "Writing to Segmenting Cache", "stream_name", streamName)
}

func (c *Cache[T]) UnittestIntrospection() *map[string]T {
	return &c.cache
}
