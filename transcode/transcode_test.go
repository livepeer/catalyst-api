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
	// Create a temporary file on the local filesystem
	f, err := os.CreateTemp(os.TempDir(), "manifest*.m3u8")
	require.NoError(t, err)

	// Write some data to it
	_, err = f.WriteString(exampleManifest)
	require.NoError(t, err)

	// Check we don't get an error downloading or parsing it
	err = RunTranscodeProcess(f.Name(), "/tmp/target.m3u8")
	require.NoError(t, err)
}
