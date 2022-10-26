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
5000.ts`

func TestItFailsWhenTheURLDoesntExist(t *testing.T) {
	err := RunTranscodeProcess("/some/fake/url.m3u8", "/tmp/target.m3u8")
	require.Error(t, err)
	require.Contains(t, err.Error(), "error downloading manifest")
	require.Contains(t, err.Error(), "failed to read from OS URL")
	require.Contains(t, err.Error(), "/some/fake/url.m3u8")
}

func TestItFailsWhenItCantParseTheManifest(t *testing.T) {
	// Create a temporary file on the local filesystem
	f, err := os.CreateTemp(os.TempDir(), "manifest*.m3u8")
	require.NoError(t, err)

	// Write some data to it
	_, err = f.WriteString("This isn't a manifest file!")
	require.NoError(t, err)

	// Check it fails with a parsing error
	err = RunTranscodeProcess(f.Name(), "/tmp/target.m3u8")
	require.Error(t, err)
	require.Contains(t, err.Error(), "error decoding manifest")
	require.Contains(t, err.Error(), f.Name())
}

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
