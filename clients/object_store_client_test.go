package clients

import (
	"github.com/stretchr/testify/require"
	"io"
	"path"
	"strings"
	"testing"
	"time"
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
	require.Contains(t, err.Error(), "failed to read from OS URL")
	require.Contains(t, err.Error(), "no such file or directory")
}

func TestPublishDriverSession(t *testing.T) {
	require := require.New(t)

	s3Url := "s3+http://usename:password@bucket/hls/"
	s3UrlRes, err := PublishDriverSession(s3Url, "whatever")
	require.NoError(err)
	require.Equal(s3Url, s3UrlRes)

	invalidUrl := "invalid://some-invalid-url"
	invalidUrlRes, err := PublishDriverSession(invalidUrl, "whatever")
	require.Error(err)
	require.Equal("", invalidUrlRes)
}
