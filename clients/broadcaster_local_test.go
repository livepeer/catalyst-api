package clients

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
)

func TestLocalBroadcasterValidatesProfiles(t *testing.T) {
	require := require.New(t)

	called := 0
	testserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusTeapot)
		w.Write([]byte("hissss"))
	}))
	defer testserver.Close()

	client, err := NewLocalBroadcasterClient(testserver.URL)
	require.NoError(err)

	_, err = client.TranscodeSegment(nil, 0, 0, "", LivepeerTranscodeConfiguration{Profiles: []video.EncodedProfile{{Name: "bad", Copy: true}}})
	require.ErrorContains(err, "copy profile not supported on transcode pipeline")
	require.Equal(0, called)

	_, err = client.TranscodeSegment(nil, 0, 0, "", LivepeerTranscodeConfiguration{Profiles: []video.EncodedProfile{{Name: "360p0", Width: 640, Height: 360, Bitrate: 900_000, Quality: video.DefaultQuality}}})
	require.ErrorContains(err, "418 I'm a teapot")
	require.ErrorContains(err, "hissss")
	require.Equal(1, called)
}
