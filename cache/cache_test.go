package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testStreamInfo struct {
	CallbackURL string
}

func TestStoreAndRetrieveSegmenting(t *testing.T) {
	c := New[testStreamInfo]()
	c.Store(
		"some-stream-name",
		testStreamInfo{
			CallbackURL: "http://some-callback-url.com",
		},
	)
	require.Equal(t, "http://some-callback-url.com", c.Get("some-stream-name").CallbackURL)
}

func TestStoreAndRemoveSegmenting(t *testing.T) {
	c := New[testStreamInfo]()
	c.Store(
		"some-stream-name",
		testStreamInfo{
			CallbackURL: "http://some-callback-url.com",
		},
	)
	require.Equal(t, "http://some-callback-url.com", c.Get("some-stream-name").CallbackURL)

	c.Remove("request-id", "some-stream-name")
	require.Equal(t, "", c.Get("some-stream-name").CallbackURL)
}
