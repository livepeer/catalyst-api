package clients

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItCanRecogniseArweaveURLs(t *testing.T) {
	require.False(t, IsArweaveURL("http://google.com"))                                              // Some random URL
	require.False(t, IsArweaveURL("https://twitter.com/search?q=arweave"))                           // Checking we don't match any URL with 'arweave' in
	require.True(t, IsArweaveURL("https://arweave.net/jL-YU1yUcZ5aWPku6dcjwLnoS-E0qs2QPzVXIA7Hfz0")) // Legit HTTPS Arweave URL
	require.True(t, IsArweaveURL("http://arweave.net/jL-YU1yUcZ5aWPku6dcjwLnoS-E0qs2QPzVXIA7Hfz0"))  // Legit HTTP Arweave URL
}

func TestItCanCopyAnArweaveHTTPFileToS3(t *testing.T) {
	outputDir, err := os.MkdirTemp(os.TempDir(), "TestItCanCopyAnArweaveHTTPFileToS3-*")
	require.NoError(t, err)

	// Have the server fail the first time, to also test our retry logic
	var counter = 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if counter == 0 {
			counter++
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, err := w.Write([]byte("some file contents"))
		require.NoError(t, err)
	}))
	defer ts.Close()

	// Create a temporary "S3" location to write to
	outputFile := filepath.Join(outputDir, "filename.txt")

	// Do the copy
	err = CopyArweaveToS3(ts.URL, outputFile)
	require.NoError(t, err)

	// Check that the file has the contents we'd expect
	dat, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	require.Equal(t, "some file contents", string(dat))
}
