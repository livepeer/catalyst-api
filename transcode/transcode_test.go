package transcode

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const exampleManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.4160000000,
0.ts
#EXTINF:5.3340000000,
5000.ts
#EXT-X-ENDLIST`

func TestItCanParseAValidManifest(t *testing.T) {
	dir := os.TempDir()

	// Create temporary manifest + segment files on the local filesystem
	manifestFile, err := os.CreateTemp(dir, "manifest-*.m3u8")
	require.NoError(t, err)

	segment0, err := os.Create(dir + "/0.ts")
	require.NoError(t, err)

	segment1, err := os.Create(dir + "/5000.ts")
	require.NoError(t, err)

	// Write some data to it
	_, err = manifestFile.WriteString(exampleManifest)
	require.NoError(t, err)
	_, err = segment0.WriteString("segment data")
	require.NoError(t, err)
	_, err = segment1.WriteString("lots of segment data")
	require.NoError(t, err)

	// Check we don't get an error downloading or parsing it
	err = RunTranscodeProcess(manifestFile.Name(), "/tmp/target.m3u8")
	require.NoError(t, err)
}
