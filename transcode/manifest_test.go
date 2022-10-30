package transcode

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/stretchr/testify/require"
)

const validMasterManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-INDEPENDENT-SEGMENTS
#EXT-X-STREAM-INF:BANDWIDTH=2665726,AVERAGE-BANDWIDTH=2526299,RESOLUTION=960x540,FRAME-RATE=29.970,CODECS="avc1.640029,mp4a.40.2",SUBTITLES="subtitles"
index_1.m3u8`

const validMediaManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.4160000000,
0.ts
#EXTINF:5.3340000000,
5000.ts
#EXT-X-ENDLIST`

func TestDownloadRenditionManifestFailsWhenItCantFindTheManifest(t *testing.T) {
	_, err := DownloadRenditionManifest("/tmp/something/x.m3u8")
	require.Error(t, err)
	require.Contains(t, err.Error(), "error downloading manifest")
}

func TestDownloadRenditionManifestFailsWhenItCantParseTheManifest(t *testing.T) {
	manifestFile, err := os.CreateTemp(os.TempDir(), "manifest-*.m3u8")
	require.NoError(t, err)
	_, err = manifestFile.WriteString("This isn't a manifest!")
	require.NoError(t, err)

	_, err = DownloadRenditionManifest(manifestFile.Name())
	require.Error(t, err)
	require.Contains(t, err.Error(), "error decoding manifest")
}

func TestDownloadRenditionManifestFailsWhenItReceivesAMasterManifest(t *testing.T) {
	manifestFile, err := os.CreateTemp(os.TempDir(), "manifest-*.m3u8")
	require.NoError(t, err)
	_, err = manifestFile.WriteString(validMasterManifest)
	require.NoError(t, err)

	_, err = DownloadRenditionManifest(manifestFile.Name())
	require.Error(t, err)
	require.Contains(t, err.Error(), "only Media playlists are supported")
}

func TestItCanDownloadAValidRenditionManifest(t *testing.T) {
	manifestFile, err := os.CreateTemp(os.TempDir(), "manifest-*.m3u8")
	require.NoError(t, err)
	_, err = manifestFile.WriteString(validMediaManifest)
	require.NoError(t, err)

	_, err = DownloadRenditionManifest(manifestFile.Name())
	require.NoError(t, err)
}

func TestItCanConvertRelativeURLsToOSURLs(t *testing.T) {
	u, err := manifestURLToSegmentURL("/tmp/file/something.m3u8", "001.ts")
	require.NoError(t, err)
	require.Equal(t, "/tmp/file/001.ts", u)

	u, err = manifestURLToSegmentURL("s3+https://REDACTED:REDACTED@storage.googleapis.com/something/output.m3u8", "001.ts")
	require.NoError(t, err)
	require.Equal(t, "s3+https://REDACTED:REDACTED@storage.googleapis.com/something/001.ts", u)
}

func TestItParsesManifestAndConvertsRelativeURLs(t *testing.T) {
	sourceManifest, _, err := m3u8.DecodeFrom(strings.NewReader(validMediaManifest), true)
	require.NoError(t, err)

	sourceMediaManifest, ok := sourceManifest.(*m3u8.MediaPlaylist)
	require.True(t, ok)

	us, err := GetSourceSegmentURLs("s3+https://REDACTED:REDACTED@storage.googleapis.com/something/output.m3u8", *sourceMediaManifest)
	require.NoError(t, err)

	require.Equal(t, 2, len(us))
	require.Equal(t, "s3+https://REDACTED:REDACTED@storage.googleapis.com/something/0.ts", us[0].URL)
	require.Equal(t, int64(10416), us[0].DurationMillis)
	require.Equal(t, "s3+https://REDACTED:REDACTED@storage.googleapis.com/something/5000.ts", us[1].URL)
	require.Equal(t, int64(5334), us[1].DurationMillis)
}

func TestItCanGenerateAndWriteManifests(t *testing.T) {
	// Set up the parameters we pass in
	sourceManifest, _, err := m3u8.DecodeFrom(strings.NewReader(validMediaManifest), true)
	require.NoError(t, err)

	sourceMediaPlaylist, ok := sourceManifest.(*m3u8.MediaPlaylist)
	require.True(t, ok)

	outputDir, err := os.MkdirTemp(os.TempDir(), "TestItCanGenerateAndWriteManifests-*")
	require.NoError(t, err)

	// Do the thing
	err = GenerateAndUploadManifests(
		*sourceMediaPlaylist,
		outputDir,
		[]clients.EncodedProfile{
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

	// Confirm we wrote out the master manifest that we expected
	masterManifest := filepath.Join(outputDir, "index.m3u8")
	require.FileExists(t, masterManifest)
	masterManifestContents, err := os.ReadFile(masterManifest)
	require.NoError(t, err)

	const expectedMasterManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=1,RESOLUTION=800x600,NAME="0-lowlowlow",FRAME-RATE=60.000
rendition-0/rendition.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=1,RESOLUTION=1080x720,NAME="1-super-high-def",FRAME-RATE=30.000
rendition-1/rendition.m3u8
`
	require.Equal(t, expectedMasterManifest, string(masterManifestContents))

	// Confirm we wrote out the rendition manifests that we expected
	require.FileExists(t, filepath.Join(outputDir, "rendition-0/rendition.m3u8"))
	require.FileExists(t, filepath.Join(outputDir, "rendition-1/rendition.m3u8"))
	require.NoFileExists(t, filepath.Join(outputDir, "rendition-2/rendition.m3u8"))
}
