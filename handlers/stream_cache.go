package handlers

import (
	"fmt"
	"log"
	"math"
	"net/url"
	"sync"
)

// StreamCache is per server shared state.
// Each pipeline (usually endpoint) having separate structure for keeping state between HTTP calls.
// State is protected by mutex allowing concurent usage.
// All state manipulation is contained in this file with goal to be brief and release mutex asap.
type StreamCache struct {
	Segmenting  SegmentingCache
	Transcoding TranscodingCache
}

// NewStreamCache returns pointer so each handler would refer to same object (kind of singleton)
func NewStreamCache() *StreamCache {
	c := &StreamCache{}
	c.Init()
	return c
}

type TranscodingCache struct {
	pushes map[string]*SegmentInfo
	mutex  sync.Mutex
}

type SegmentInfo struct {
	CallbackUrl  string
	Source       string           // S3 input we are transcoding
	UploadDir    *url.URL         // S3 destination url for multiple renditions
	Profiles     []EncodedProfile // Requested encoding profiles to produce
	Destinations []string         // Rendition URLS go here on push start and removed on push end
}

func (c *SegmentInfo) GetMatchingProfile(t MistTrack) (p EncodedProfile, found bool) {
	trackPixels := float64(t.Width * t.Height)
	size := len(c.Profiles)
	if size == 0 {
		return
	}
	p = c.Profiles[0]
	for _, profile := range c.Profiles[1:] {
		if profile.Width == t.Width && profile.Height == t.Height {
			log.Printf("FOUND GetMatchingProfile %dx%d", t.Width, t.Height)
			return profile, true
		}
		distance := math.Abs(float64(profile.Width*profile.Height) - trackPixels)
		choice := math.Abs(float64(p.Width*p.Height) - trackPixels)
		if distance < choice {
			p = profile
			log.Printf("_select_ GetMatchingProfile next:%dx%d", profile.Width, profile.Height)
		} else {
			log.Printf("_X_ GetMatchingProfile next:%dx%d > choice:%dx%d", profile.Width, profile.Height, p.Width, p.Height)
		}
	}
	found = p.Bitrate != 0
	log.Printf("returning GetMatchingProfile %dx%d", p.Width, p.Height)
	return
}

func (c *TranscodingCache) Init() {
	c.pushes = make(map[string]*SegmentInfo)
}

type IsEmpty = bool

func (c *TranscodingCache) AddDestination(streamName, destination string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info, ok := c.pushes[streamName]
	if ok {
		info.Destinations = append(info.Destinations, destination)
	}
}

func (c *TranscodingCache) RemovePushDestination(streamName, destination string) IsEmpty {
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
		return len(info.Destinations) == 0
	}
	return false
}

func (c *TranscodingCache) Remove(streamName string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.pushes, streamName)
}

func (c *TranscodingCache) Get(streamName string) (SegmentInfo, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info, ok := c.pushes[streamName]
	if ok {
		return *info, nil
	}
	return SegmentInfo{}, fmt.Errorf("cache mismatch for %s", streamName)
}

func (c *TranscodingCache) Store(streamName string, info SegmentInfo) {
	c.mutex.Lock()
	c.pushes[streamName] = &info
	c.mutex.Unlock()
}

type SegmentingCache struct {
	cache map[string]StreamInfo
	mutex sync.Mutex
}

type StreamInfo struct {
	callbackUrl string
}

func (c *SegmentingCache) Init() {
	c.cache = make(map[string]StreamInfo)
}

func (c *SegmentingCache) Remove(streamName string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.cache, streamName)
}

func (c *SegmentingCache) GetCallbackUrl(streamName string) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info, ok := c.cache[streamName]
	if ok {
		return info.callbackUrl, nil
	}
	return "", fmt.Errorf("cache mismatch for %s", streamName)
}

func (c *SegmentingCache) Store(streamName, callbackUrl string) {
	c.mutex.Lock()
	c.cache[streamName] = StreamInfo{callbackUrl: callbackUrl}
	c.mutex.Unlock()
}

func (c *StreamCache) Init() {
	c.Segmenting.Init()
	c.Transcoding.Init()
}
