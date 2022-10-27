package transcode

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/livepeer/catalyst-api/cache"
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
	outputMasterManifest := filepath.Join(outputDir, "output-master.m3u8")

	// Check we don't get an error downloading or parsing it
	err = RunTranscodeProcess(
		manifestFile.Name(),
		outputMasterManifest,
		[]cache.EncodedProfile{
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
	)
	require.NoError(t, err)

	// Confirm the master manifest was created and that it looks like a manifest
	masterManifestBytes, err := os.ReadFile(outputMasterManifest)
	require.NoError(t, err)
	require.Greater(t, len(masterManifestBytes), 0)
	require.Contains(t, string(masterManifestBytes), "#EXTM3U")
	require.Contains(t, string(masterManifestBytes), "#EXT-X-STREAM-INF")
}
