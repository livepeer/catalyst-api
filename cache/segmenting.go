package cache

import (
	"sync"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/log"
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
	RequestID             string
	Profiles              []clients.EncodedProfile
}

func (c *SegmentingCache) Remove(requestID, streamName string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.cache, streamName)
	log.Log(requestID, "Deleting from Segmenting Cache", "stream_name", streamName)
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

func (c *SegmentingCache) GetRequestID(streamName string) string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info, ok := c.cache[streamName]
	if ok {
		return info.RequestID
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
	defer c.mutex.Unlock()
	c.cache[streamName] = StreamInfo{
		SourceFile:            streamInfo.SourceFile,
		CallbackURL:           streamInfo.CallbackURL,
		UploadURL:             streamInfo.UploadURL,
		AccessToken:           streamInfo.AccessToken,
		TranscodeAPIUrl:       streamInfo.TranscodeAPIUrl,
		HardcodedBroadcasters: streamInfo.HardcodedBroadcasters,
		Profiles:              streamInfo.Profiles,
		RequestID:             streamInfo.RequestID,
	}
	log.Log(streamInfo.RequestID, "Writing to Segmenting Cache", "stream_name", streamName)
}

func (c *SegmentingCache) UnittestIntrospection() *map[string]StreamInfo {
	return &c.cache
}
