package mistapiconnector

import (
	"encoding/json"
	"github.com/livepeer/go-api-client"
	require2 "github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGetStreamByPlaybackID(t *testing.T) {
	require := require2.New(t)

	// given
	playbackId := "some-playback-id"
	stubStream := &api.Stream{
		ID: "123456",
	}
	var hits int

	lapiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		err := json.NewEncoder(w).Encode(stubStream)
		if err != nil {
			t.Fail()
		}
	}))
	defer lapiServer.Close()

	lapi, _ := api.NewAPIClientGeolocated(api.ClientOptions{
		Server: lapiServer.URL,
	})
	cachedClient := NewApiClientCached(lapi)

	// when first call to Livepeer API
	s, err := cachedClient.GetStreamByPlaybackID(playbackId)
	require.NoError(err)
	require.Equal(stubStream, s)
	require.Equal(1, hits)

	// when multiple time the same call within 1 second
	for i := 0; i < 10; i++ {
		s, err = cachedClient.GetStreamByPlaybackID(playbackId)
		require.NoError(err)
		require.Equal(stubStream, s)
		require.Equal(1, hits)
	}

	// when ttl is expired
	time.Sleep(1 * time.Second)
	s, err = cachedClient.GetStreamByPlaybackID(playbackId)
	require.NoError(err)
	require.Equal(stubStream, s)
	require.Equal(2, hits)
}
