package transcode

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
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

func (c StubBroadcasterClient) TranscodeSegment(segment io.Reader, sequenceNumber int64, profiles []clients.EncodedProfile, durationMillis int64, manifestID string) (clients.TranscodeResult, error) {
	return c.tr, nil
}

func TestItCanTranscode(t *testing.T) {
	dir := os.TempDir()
	fmt.Println("TestItCanTranscode running using Temp Dir:", dir)

	// Create 2 layers of subdirectories to ensure runs of the test don't interfere with each other
	// and that it simulates the production layout
	topLevelDir := filepath.Join(dir, "unit-test-dir-"+config.RandomTrailer(8))
	err := os.Mkdir(topLevelDir, os.ModePerm)
	require.NoError(t, err)

	dir = filepath.Join(topLevelDir, "unit-test-subdir")
	err = os.Mkdir(dir, os.ModePerm)
	require.NoError(t, err)

	dir = filepath.Join(topLevelDir, "unit-test-subdir")
	err = os.Mkdir(dir, os.ModePerm)
	require.NoError(t, err)

	// Create temporary manifest + segment files on the local filesystem
	manifestFile, err := os.CreateTemp(dir, "index.m3u8")
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
					Name:      "low-bitrate",
					MediaData: []byte("pretend media data"),
				},
				{
					Name:      "source",
					MediaData: []byte("pretend high-def media data"),
				},
			},
		},
	}

	// Check we don't get an error downloading or parsing it
	_, err = RunTranscodeProcess(
		TranscodeSegmentRequest{
			CallbackURL: callbackServer.URL,
			UploadURL:   manifestFile.Name(),
		},
		"streamName",
		clients.InputVideo{
			Duration:  123.0,
			Format:    "some-format",
			SizeBytes: 123,
			Tracks: []clients.InputTrack{
				{
					Type: "video",
					VideoTrack: clients.VideoTrack{
						Width:  2020,
						Height: 2020,
					},
				},
			},
		},
	)
	require.NoError(t, err)

	// Confirm the master manifest was created and that it looks like a manifest
	masterManifestBytes, err := os.ReadFile(filepath.Join(topLevelDir, "index.m3u8"))
	require.NoError(t, err)
	require.Greater(t, len(masterManifestBytes), 0)
	require.Contains(t, string(masterManifestBytes), "#EXTM3U")
	require.Contains(t, string(masterManifestBytes), "#EXT-X-STREAM-INF")

	// Confirm that the master manifest contains links to 2 renditions
	require.Contains(t, string(masterManifestBytes), "rendition-0/index.m3u8")
	require.Contains(t, string(masterManifestBytes), "rendition-1/index.m3u8")

	// Check we received progress callback at least for the completion (progress package will handle update frequency)
	require.GreaterOrEqual(t, len(callbacks), 1)
	require.Equal(t, 1.0, callbacks[len(callbacks)-1]["completion_ratio"])

	// Check we received a final Transcode Completed callback
	require.Equal(t, "success", callbacks[1]["status"])
}

func TestItCalculatesTheTranscodeCompletionPercentageCorrectly(t *testing.T) {
	require.Equal(t, 0.5, calculateCompletedRatio(2, 1))
	require.Equal(t, 0.5, calculateCompletedRatio(4, 2))
	require.Equal(t, 0.1, calculateCompletedRatio(10, 1))
	require.Equal(t, 0.01, calculateCompletedRatio(100, 1))
	require.Equal(t, 0.6, calculateCompletedRatio(100, 60))
}
