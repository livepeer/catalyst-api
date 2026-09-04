package clients

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
)

func TestRemoteBroadcasterValidatesProfiles(t *testing.T) {
	require := require.New(t)

	called := 0
	apiClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/broadcaster") {
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`[{"address":"https://broadcaster.example.com"}]`))}, nil
		}
		called++
		return &http.Response{StatusCode: http.StatusTeapot, Status: "418 I'm a teapot", Body: http.NoBody}, nil
	})}

	client, err := newRemoteBroadcasterClient(
		Credentials{CustomAPIURL: "https://api.example.com", AccessToken: "test"},
		apiClient,
		apiClient,
	)
	require.NoError(err)

	_, err = client.TranscodeSegmentWithRemoteBroadcaster(nil, 0, []video.EncodedProfile{{Name: "bad", Copy: true}}, "", 0)
	require.ErrorContains(err, "copy profile not supported on transcode pipeline")
	require.Equal(0, called)

	_, err = client.TranscodeSegmentWithRemoteBroadcaster(nil, 0, []video.EncodedProfile{{Name: "360p0", Width: 640, Height: 360, Bitrate: 900_000, Quality: video.DefaultQuality}}, "", 0)
	require.ErrorContains(err, "418 I'm a teapot")
	require.Equal(1, called)
}

func TestRemoteBroadcasterRejectsNonPublicTargets(t *testing.T) {
	_, err := NewRemoteBroadcasterClient(Credentials{CustomAPIURL: "http://api.example.com", AccessToken: "test"})
	require.Error(t, err)
	_, err = NewRemoteBroadcasterClient(Credentials{CustomAPIURL: "https://127.0.0.1", AccessToken: "test"})
	require.Error(t, err)
	_, err = NewRemoteBroadcasterClient(Credentials{CustomAPIURL: "https://user:secret@api.example.com", AccessToken: "test"})
	require.Error(t, err)

	client, err := NewRemoteBroadcasterClient(Credentials{CustomAPIURL: "https://api.example.com", AccessToken: "test"})
	require.NoError(t, err)
	_, err = client.pickRandomBroadcaster(BroadcasterList{{Address: "https://10.0.0.1"}})
	require.Error(t, err)
	_, err = client.pickRandomBroadcaster(BroadcasterList{{Address: "http://broadcaster.example.com"}})
	require.Error(t, err)
}

func TestRemoteBroadcasterClientsShareGuardedTransports(t *testing.T) {
	first, err := NewRemoteBroadcasterClient(Credentials{CustomAPIURL: "https://api.example.com", AccessToken: "first"})
	require.NoError(t, err)
	second, err := NewRemoteBroadcasterClient(Credentials{CustomAPIURL: "https://api.example.com", AccessToken: "second"})
	require.NoError(t, err)

	require.Same(t, first.apiClient, second.apiClient)
	require.Same(t, first.mediaClient, second.mediaClient)
}
