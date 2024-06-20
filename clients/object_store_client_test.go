package clients

import (
	"io"
	"path"
	"strings"
	"testing"
	"time"

	caterrors "github.com/livepeer/catalyst-api/errors"
	"github.com/stretchr/testify/require"
)

const (
	exampleFileContents = "زن, زندگی, آزادی "
	exampleFilename     = "manifest.m3u8"
)

func TestItCanUploadAndDownloadWithOSURL(t *testing.T) {
	dir := t.TempDir()

	err := UploadToOSURL(dir, exampleFilename, strings.NewReader(exampleFileContents), 5*time.Minute)
	require.NoError(t, err)

	rc, err := DownloadOSURL(path.Join(dir, exampleFilename))
	require.NoError(t, err)

	buf := new(strings.Builder)
	_, err = io.Copy(buf, rc)
	require.NoError(t, err)
	require.Equal(t, exampleFileContents, buf.String())
}

func TestItFailsWithInvalidURLs(t *testing.T) {
	_, err := DownloadOSURL("s4+htps://123/456.m3u8")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse OS URL")
	require.Contains(t, err.Error(), "unrecognized OS scheme")
}

func TestItFailsWithMissingFile(t *testing.T) {
	_, err := DownloadOSURL("/tmp/this/should/not/exist.m3u8")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ObjectNotFoundError")
	require.True(t, caterrors.IsObjectNotFound(err))
}

func TestPublish(t *testing.T) {
	require := require.New(t)

	hlsUrl := "s3+http://username:password@bucket/hls/whatever"
	mp4Url := "s3+http://username:password@bucket/mp4/whatever"
	hlsPlaybackUrl, mp4PlaybackUrl, err := Publish(hlsUrl, mp4Url)
	require.NoError(err)
	require.Equal(hlsPlaybackUrl, hlsUrl)
	require.Equal(mp4PlaybackUrl, mp4Url)

	hlsUrl = "s3+http://first:first@bucket/hls/whatever"
	mp4Url = "s3+http://second:second@bucket/mp4/whatever"
	hlsPlaybackUrl, mp4PlaybackUrl, err = Publish(hlsUrl, mp4Url)
	require.NoError(err)
	require.Equal(hlsPlaybackUrl, hlsUrl)
	require.Equal(mp4PlaybackUrl, mp4Url)

	invalidUrl := "invalid://some-invalid-url"
	hlsPlaybackUrl, mp4PlaybackUrl, err = Publish(invalidUrl, invalidUrl)
	require.Error(err)
	require.Equal("", hlsPlaybackUrl)
	require.Equal("", mp4PlaybackUrl)
}
