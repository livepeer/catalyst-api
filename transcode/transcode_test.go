package transcode

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/stretchr/testify/require"
)

const exampleMediaManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.4160000000,
0.ts
#EXTINF:5.3340000000,
5000.ts
#EXT-X-ENDLIST`

type StubBroadcasterClient struct {
	tr clients.TranscodeResult
}

func (c StubBroadcasterClient) TranscodeSegment(segment io.Reader, sequenceNumber int64, profiles []clients.EncodedProfile, durationMillis int64) (clients.TranscodeResult, error) {
	return c.tr, nil
}

func TestItCanTranscode(t *testing.T) {
	dir := os.TempDir()

	// Create temporary manifest + segment files on the local filesystem
	manifestFile, err := os.CreateTemp(dir, "manifest-*.m3u8")
	require.NoError(t, err)

	segment0, err := os.Create(dir + "/0.ts")
	require.NoError(t, err)

	segment1, err := os.Create(dir + "/5000.ts")
	require.NoError(t, err)

	// Write some data to it
	_, err = manifestFile.WriteString(exampleMediaManifest)
	require.NoError(t, err)
	_, err = segment0.WriteString("segment data")
	require.NoError(t, err)
	_, err = segment1.WriteString("lots of segment data")
	require.NoError(t, err)

	// Set up somewhere to output the results to
	outputDir := os.TempDir()

	// Set up a server to receive callbacks and store them in an array for future verification
	var callbacks []map[string]interface{}
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that we got the callback we're expecting
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var callback map[string]interface{}
		err = json.Unmarshal(body, &callback)
		require.NoError(t, err)
		callbacks = append(callbacks, callback)
	}))
	defer callbackServer.Close()

	// Set up a fake Broadcaster that returns the rendition segments we'd expect based on the
	// transcode request we send in the next step
	localBroadcasterClient = StubBroadcasterClient{
		tr: clients.TranscodeResult{
			Renditions: []*clients.RenditionSegment{
				{
					Name:      "lowlowlow",
					MediaData: []byte("pretend media data"),
				},
				{
					Name:      "super-high-def",
					MediaData: []byte("pretend high-def media data"),
				},
			},
		},
	}

	// Check we don't get an error downloading or parsing it
	err = RunTranscodeProcess(
		TranscodeSegmentRequest{
			Profiles: []clients.EncodedProfile{
				{
					Name:   "lowlowlow",
					FPS:    60,
					Width:  800,
					Height: 600,
				},
				{
					Name:   "super-high-def",
					FPS:    30,
					Width:  1080,
					Height: 720,
				},
			},
			CallbackURL: callbackServer.URL,
			UploadURL:   manifestFile.Name(),
		},
		"streamName",
	)
	require.NoError(t, err)

	// Confirm the master manifest was created and that it looks like a manifest
	masterManifestBytes, err := os.ReadFile(filepath.Join(outputDir, "transcoded/index.m3u8"))
	require.NoError(t, err)
	require.Greater(t, len(masterManifestBytes), 0)
	require.Contains(t, string(masterManifestBytes), "#EXTM3U")
	require.Contains(t, string(masterManifestBytes), "#EXT-X-STREAM-INF")

	// Check we received a progress callback for each segment
	require.Equal(t, 2, len(callbacks))
	require.Equal(t, 0.7, callbacks[0]["completion_ratio"])
	require.Equal(t, 1.0, callbacks[1]["completion_ratio"])
}

func TestItCalculatesTheTranscodeCompletionPercentageCorrectly(t *testing.T) {
	require.Equal(t, 0.5, calculateCompletedRatio(2, 1))
	require.Equal(t, 0.5, calculateCompletedRatio(4, 2))
	require.Equal(t, 0.1, calculateCompletedRatio(10, 1))
	require.Equal(t, 0.01, calculateCompletedRatio(100, 1))
	require.Equal(t, 0.6, calculateCompletedRatio(100, 60))
}
