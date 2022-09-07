package cache

// StreamCache is per server shared state.
// Each pipeline (usually endpoint) having separate structure for keeping state between HTTP calls.
// State is protected by mutex allowing concurent usage.
// All state manipulation is contained in this file with goal to be brief and release mutex asap.
type StreamCache struct {
	Segmenting  SegmentingCache
	Transcoding TranscodingCache
}

var DefaultStreamCache = NewStreamCache()

// NewStreamCache returns pointer so each handler would refer to same object (kind of singleton)
func NewStreamCache() *StreamCache {
	return &StreamCache{
		Segmenting: SegmentingCache{
			cache: make(map[string]StreamInfo),
		},
		Transcoding: TranscodingCache{
			pushes: make(map[string]*SegmentInfo),
		},
	}
}
