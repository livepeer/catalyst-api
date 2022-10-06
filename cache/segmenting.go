package cache

import (
	"sync"
)

type SegmentingCache struct {
	cache map[string]StreamInfo
	mutex sync.Mutex
}

type StreamInfo struct {
	SourceFile string
	CallbackUrl string
	UploadDir string
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
		return info.CallbackUrl
	}
	return ""
}

func (c *SegmentingCache) Get(streamName string) StreamInfo {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info, ok := c.cache[streamName]
	if ok {
		return info
	}
	return StreamInfo{} 
}

func (c *SegmentingCache) Store(streamName string, streamInfo StreamInfo) {
	c.mutex.Lock()
	c.cache[streamName] = StreamInfo{
					SourceFile: streamInfo.SourceFile,
					CallbackUrl: streamInfo.CallbackUrl,
					UploadDir: streamInfo.UploadDir,
				}
	c.mutex.Unlock()
}
