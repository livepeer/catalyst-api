package analytics

import (
	"github.com/livepeer/go-api-client"
	"github.com/stretchr/testify/require"
	"testing"
)

type MockMapicCache struct {
	streams   map[string]*api.Stream
	callCount int
}

func (c *MockMapicCache) GetCachedStream(playbackID string) *api.Stream {
	c.callCount = c.callCount + 1
	return c.streams[playbackID]
}

func TestFetch(t *testing.T) {
	require := require.New(t)

	playbackID := "playback-id-1"
	userID := "user-id-1"
	creatorID := "creator-id-1"

	mockMapicCache := &MockMapicCache{streams: map[string]*api.Stream{
		playbackID: {
			UserID:    userID,
			CreatorID: api.CreatorID{Value: creatorID},
		},
	}}

	e := NewExternalDataFetcher(mockMapicCache, nil)

	// First call
	res, err := e.Fetch(playbackID)
	require.NoError(err)
	require.Equal(userID, res.UserID)
	require.Equal(1, mockMapicCache.callCount)

	// Second call, use cache
	res, err = e.Fetch(playbackID)
	require.NoError(err)
	require.Equal(userID, res.UserID)
	require.Equal(creatorID, res.CreatorID)
	require.Equal("stream", res.SourceType)
	require.Equal(1, mockMapicCache.callCount)
}
