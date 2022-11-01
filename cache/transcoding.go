package cache

import (
	"sync"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/log"
)

type TranscodingCache struct {
	pushes map[string]*SegmentInfo
	mutex  sync.Mutex
}

type SegmentInfo struct {
	CallbackUrl  string
	Source       string                   // S3 input we are transcoding
	UploadDir    string                   // S3 destination url for multiple renditions
	Profiles     []clients.EncodedProfile // Requested encoding profiles to produce
	Destinations []string                 // Rendition URLS go here on push start and removed on push end
	Outputs      []clients.OutputVideo    // Information about the final transcoded outputs we've created
	updatedAt    time.Time                // Time at which this object was last updated in cache
}

// Send "keepalive" callbacks to ensure the caller (Studio) knows transcoding is still ongoing and hasn't failed
func (t *TranscodingCache) SendTranscodingHeartbeats(interval time.Duration, maxAge time.Duration, quit chan bool) {
	for {
		// Stop the infinite loop if we receive a quit message
		select {
		case <-quit:
			return
		default:
		}

		jobs := t.GetAll()
		for id, job := range jobs {
			// If the job is past the expiry time then we've probably failed to remove it from the cache when it completed / errored
			if job.updatedAt.Add(maxAge).Before(time.Now()) {
				t.Remove(id)
				continue
			}

			err := clients.DefaultCallbackClient.SendTranscodeStatus(job.CallbackUrl, clients.TranscodeStatusTranscoding, 0.5)
			if err == nil {
				log.LogNoRequestID("Sent Transcode Status heartbeat", "id", id, "callback_url", job.CallbackUrl)
			} else {
				log.LogNoRequestID("failed to send Transcode Status heartbeat", "id", id, "callback_url", job.CallbackUrl, "error", err)
			}
		}
		time.Sleep(interval)
	}
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
	c.debugPrint("remove", streamName)
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

func (c *TranscodingCache) GetAll() map[string]*SegmentInfo {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.pushes
}

func (c *TranscodingCache) Store(streamName string, info SegmentInfo) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	info.updatedAt = time.Now()
	c.pushes[streamName] = &info
	c.debugPrint("add", streamName)
}

func (c *TranscodingCache) debugPrint(action, streamName string) {
	var id string = action + " " + streamName + ": TranscodingCache"
	pretty.Print(id, c.pushes)
}
