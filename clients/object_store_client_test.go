package clients

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/catalyst-api/config"
	"github.com/stretchr/testify/require"
)

const exampleFileContents = "زن, زندگی, آزادی "

func TestItCanDownloadAnOSURL(t *testing.T) {
	// Create a temporary file on the local filesystem
	f, err := os.CreateTemp(os.TempDir(), "manifest*.m3u8")
	require.NoError(t, err)

	// Write some data to it
	_, err = f.WriteString(exampleFileContents)
	require.NoError(t, err)

	// Try to "download" it using the OS URL format for local filesystem files
	rc, err := DownloadOSURL(f.Name())
	require.NoError(t, err)

	buf := new(strings.Builder)
	_, err = io.Copy(buf, rc)
	require.NoError(t, err)

	// Check that the file we downloaded matches the one we created
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

func TestItRetriesReadingData(t *testing.T) {
	var retries = 0
	var original = makeOperation
	makeOperation = func(fn func() error) func() error {
		return func() error {
			if retries <= 1 {
				retries++
				return errors.New("some-error")
			} else {
				return fn()
			}
		}
	}
	defer func() { makeOperation = original }()

	// Create a temporary file on the local filesystem
	f, err := os.CreateTemp(os.TempDir(), "manifest*.m3u8")
	require.NoError(t, err)

	// Write some data to it
	_, err = f.WriteString(exampleFileContents)
	require.NoError(t, err)

	// Try to "download" it using the OS URL format for local filesystem files
	_, err = DownloadOSURL(f.Name())

	require.NoError(t, err)
	require.Equal(t, 2, retries)
}

func TestItFailsAfterMaxReadsReached(t *testing.T) {
	var retries uint64 = 0
	var original = makeOperation

	var originalRetries = config.DownloadOSURLRetries
	config.DownloadOSURLRetries = 3
	defer func() {
		config.DownloadOSURLRetries = originalRetries
	}()

	makeOperation = func(fn func() error) func() error {
		return func() error {
			retries++
			return errors.New("some-error")
		}
	}
	defer func() { makeOperation = original }()

	// Create a temporary file on the local filesystem
	f, err := os.CreateTemp(os.TempDir(), "manifest*.m3u8")
	require.NoError(t, err)

	// Write some data to it
	_, err = f.WriteString(exampleFileContents)
	require.NoError(t, err)

	// Try to "download" it using the OS URL format for local filesystem files
	_, err = DownloadOSURL(f.Name())

	require.Error(t, err)
	require.Equal(t, config.DownloadOSURLRetries+1, retries)
}

func TestItRetriesSavingData(t *testing.T) {
	var retries = 0
	var original = makeOperation
	makeOperation = func(fn func() error) func() error {
		return func() error {
			if retries <= 1 {
				retries++
				return errors.New("some-error")
			} else {
				return fn()
			}
		}
	}
	defer func() { makeOperation = original }()

	_, err := UploadToOSURL(os.TempDir(), "name", bytes.NewReader([]byte("foo")), 1*time.Second)

	require.NoError(t, err)
	require.Equal(t, 2, retries)
}

func TestItFailsAfterMaxSavesRetriesReached(t *testing.T) {
	var retries = 0
	var original = makeOperation
	makeOperation = func(fn func() error) func() error {
		return func() error {
			retries++
			return errors.New("some-error")
		}
	}
	defer func() { makeOperation = original }()

	_, err := UploadToOSURL(os.TempDir(), "name", bytes.NewReader([]byte("foo")), 1*time.Second)

	require.Error(t, err)
	require.Equal(t, 3, retries)
}
