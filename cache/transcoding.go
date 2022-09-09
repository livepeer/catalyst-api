package cache

import (
	"sync"
)

type TranscodingCache struct {
	pushes map[string]*SegmentInfo
	mutex  sync.Mutex
}

type EncodedProfile struct {
	Name         string `json:"name"`
	Width        int32  `json:"width"`
	Height       int32  `json:"height"`
	Bitrate      int32  `json:"bitrate"`
	FPS          uint   `json:"fps"`
	FPSDen       uint   `json:"fpsDen"`
	Profile      string `json:"profile"`
	GOP          string `json:"gop"`
	Encoder      string `json:"encoder"`
	ColorDepth   int32  `json:"colorDepth"`
	ChromaFormat int32  `json:"chromaFormat"`
}

type SegmentInfo struct {
	CallbackUrl  string
	Source       string           // S3 input we are transcoding
	UploadDir    string           // S3 destination url for multiple renditions
	Profiles     []EncodedProfile // Requested encoding profiles to produce
	Destinations []string         // Rendition URLS go here on push start and removed on push end
}

func (si SegmentInfo) ContainsDestination(destination string) bool {
	for _, existing := range si.Destinations {
		if existing == destination {
			return true
		}
	}
	return false
}

func (c *TranscodingCache) AddDestination(streamName, destination string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info, ok := c.pushes[streamName]
	if ok {
		info.Destinations = append(info.Destinations, destination)
	}
}

func (c *TranscodingCache) AreDestinationsEmpty(streamName string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info, ok := c.pushes[streamName]
	if ok {
		return len(info.Destinations) == 0
	}
	return true
}

func (c *TranscodingCache) RemovePushDestination(streamName, destination string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info, ok := c.pushes[streamName]
	if ok {
		for i := 0; i < len(info.Destinations); i++ {
			if info.Destinations[i] == destination {
				info.Destinations[i] = info.Destinations[len(info.Destinations)-1]
				info.Destinations = info.Destinations[:len(info.Destinations)-1]
				break
			}
		}
	}
}

func (c *TranscodingCache) Remove(streamName string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.pushes, streamName)
}

func (c *TranscodingCache) Get(streamName string) *SegmentInfo {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info, ok := c.pushes[streamName]
	if ok {
		return info
	}
	return nil
}

func (c *TranscodingCache) Store(streamName string, info SegmentInfo) {
	c.mutex.Lock()
	c.pushes[streamName] = &info
	c.mutex.Unlock()
}
