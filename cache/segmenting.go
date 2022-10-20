package cache

import (
	"sync"

	"github.com/kylelemons/godebug/pretty"
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
	c.debugPrint("remove", streamName)
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
	defer c.mutex.Unlock()
	c.cache[streamName] = StreamInfo{
		SourceFile:            streamInfo.SourceFile,
		CallbackURL:           streamInfo.CallbackURL,
		UploadURL:             streamInfo.UploadURL,
		AccessToken:           streamInfo.AccessToken,
		TranscodeAPIUrl:       streamInfo.TranscodeAPIUrl,
		HardcodedBroadcasters: streamInfo.HardcodedBroadcasters,
	}
	c.debugPrint("add", streamName)
}

func (c *SegmentingCache) debugPrint(action, streamName string) {
	var id string = action + " " + streamName + ": SegmentingCache"
	pretty.Print(id, c.cache)
}
