package mistapiconnector

import (
	"github.com/livepeer/go-api-client"
	"sync"
	"time"
)

const lapiCacheDuration = 1 * time.Second

type ApiClientCached struct {
	lapi        *api.Client
	streamCache map[string]entry
	mu          sync.RWMutex
	ttl         time.Duration
}

func NewApiClientCached(lapi *api.Client) *ApiClientCached {
	return &ApiClientCached{
		lapi:        lapi,
		streamCache: make(map[string]entry),
		ttl:         lapiCacheDuration,
	}
}

type entry struct {
	stream   *api.Stream
	err      error
	updateAt time.Time
}

func (a *ApiClientCached) GetStreamByPlaybackID(playbackId string) (*api.Stream, error) {
	a.mu.RLock()
	e, ok := a.streamCache[playbackId]
	if ok && e.updateAt.Add(a.ttl).After(time.Now()) {
		// Use cached value
		a.mu.RUnlock()
		return e.stream, e.err
	}
	a.mu.RUnlock()

	// Value not cached or expired
	a.mu.Lock()
	defer a.mu.Unlock()
	// Check again in case another goroutine has updated the cache in the meantime
	e, ok = a.streamCache[playbackId]
	if ok && e.updateAt.Add(a.ttl).After(time.Now()) {
		return e.stream, e.err
	}
	// No value in the cache, fetch from Livepeer API and store the result in a cache
	stream, err := a.lapi.GetStreamByPlaybackID(playbackId)
	a.streamCache[playbackId] = entry{
		stream:   stream,
		err:      err,
		updateAt: time.Now(),
	}
	return stream, err
}
