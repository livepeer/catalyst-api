package clients

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
)

func TestRemoteBroadcasterValidatesProfiles(t *testing.T) {
	require := require.New(t)

	called := 0
	testserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/broadcaster") {
			_, err := w.Write([]byte(`[{"address":"0x1234"}]`))
			require.NoError(err)
			return
		}
		called++
		w.WriteHeader(http.StatusTeapot)
		_, err := w.Write([]byte("🫖"))
		require.NoError(err)
	}))
	defer testserver.Close()

	client, err := NewRemoteBroadcasterClient(Credentials{CustomAPIURL: testserver.URL, AccessToken: "test"})
	require.NoError(err)

	_, err = client.TranscodeSegmentWithRemoteBroadcaster(nil, 0, []video.EncodedProfile{{Name: "bad", Copy: true}}, "", 0)
	require.ErrorContains(err, "copy profile not supported on transcode pipeline")
	require.Equal(0, called)

	_, err = client.TranscodeSegmentWithRemoteBroadcaster(nil, 0, []video.EncodedProfile{{Name: "360p0", Width: 640, Height: 360, Bitrate: 900_000, Quality: video.DefaultQuality}}, "", 0)
	require.ErrorContains(err, "418 I'm a teapot")
	require.Equal(1, called)
}
