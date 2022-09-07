package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreAndRetrieveTranscoding(t *testing.T) {
	c := NewStreamCache()
	c.Transcoding.Store("some-stream-name", SegmentInfo{
		CallbackUrl: "some-callback-url",
		Source:      "s3://source",
		UploadDir:   "upload-dir",
		Destinations: []string{
			"s3://destination-1",
			"s3://destination-2",
		},
	})

	si := c.Transcoding.Get("some-stream-name")
	require.NotNil(t, si)
	require.Equal(t, "some-callback-url", si.CallbackUrl)
	require.Equal(t, "s3://source", si.Source)
	require.Equal(t, "upload-dir", si.UploadDir)
	require.Equal(t, []string{"s3://destination-1", "s3://destination-2"}, si.Destinations)
}

func TestStoreAndRemoveTranscoding(t *testing.T) {
	c := NewStreamCache()
	c.Transcoding.Store("some-stream-name", SegmentInfo{
		CallbackUrl: "some-callback-url",
		Source:      "s3://source",
		UploadDir:   "upload-dir",
		Destinations: []string{
			"s3://destination-1",
			"s3://destination-2",
		},
	})
	require.NotNil(t, c.Transcoding.Get("some-stream-name"))

	c.Transcoding.Remove("some-stream-name")
	require.Nil(t, c.Transcoding.Get("some-stream-name"))
}
