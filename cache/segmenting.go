package cache

import (
	"sync"
)

type SegmentingCache struct {
	cache map[string]StreamInfo
	mutex sync.Mutex
}

type StreamInfo struct {
	SourceFile            string
	CallbackURL           string
	UploadURL             string
	AccessToken           string
	TranscodeAPIUrl       string
	HardcodedBroadcasters string
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
		return info.CallbackURL
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
		SourceFile:            streamInfo.SourceFile,
		CallbackURL:           streamInfo.CallbackURL,
		UploadURL:             streamInfo.UploadURL,
		AccessToken:           streamInfo.AccessToken,
		TranscodeAPIUrl:       streamInfo.TranscodeAPIUrl,
		HardcodedBroadcasters: streamInfo.HardcodedBroadcasters,
	}
	c.mutex.Unlock()
}
