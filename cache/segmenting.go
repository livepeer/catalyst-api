package cache

import (
	"sync"
)

type SegmentingCache struct {
	cache map[string]StreamInfo
	mutex sync.Mutex
}

type StreamInfo struct {
	callbackUrl string
}

func (c *SegmentingCache) Remove(streamName string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.cache, streamName)
}

func (c *SegmentingCache) GetCallbackUrl(streamName string) string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info, ok := c.cache[streamName]
	if ok {
		return info.callbackUrl
	}
	return ""
}

func (c *SegmentingCache) Store(streamName, callbackUrl string) {
	c.mutex.Lock()
	c.cache[streamName] = StreamInfo{callbackUrl: callbackUrl}
	c.mutex.Unlock()
}
