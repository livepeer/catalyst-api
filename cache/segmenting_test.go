package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreAndRetrieveSegmenting(t *testing.T) {
	c := NewStreamCache()
	c.Segmenting.Store(
		"some-stream-name",
		StreamInfo{
			CallbackURL: "http://some-callback-url.com",
		},
	)
	require.Equal(t, "http://some-callback-url.com", c.Segmenting.GetCallbackUrl("some-stream-name"))
}

func TestStoreAndRemoveSegmenting(t *testing.T) {
	c := NewStreamCache()
	c.Segmenting.Store(
		"some-stream-name",
		StreamInfo{
			CallbackURL: "http://some-callback-url.com",
		},
	)
	require.Equal(t, "http://some-callback-url.com", c.Segmenting.GetCallbackUrl("some-stream-name"))

	c.Segmenting.Remove("some-stream-name")
	require.Equal(t, "", c.Segmenting.GetCallbackUrl("some-stream-name"))
}
