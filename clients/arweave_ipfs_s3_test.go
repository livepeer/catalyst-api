package clients

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/livepeer/catalyst-api/config"
	"github.com/stretchr/testify/require"
)

func TestItCanRecogniseArweaveOrIPFSURLs(t *testing.T) {
	require.False(t, IsContentAddressedResource("http://google.com"))
	require.False(t, IsContentAddressedResource("https://twitter.com/search?q=arweave"))
	require.False(t, IsContentAddressedResource("arr://jL-YU1yUcZ5aWPku6dcjwLnoS-E0qs2QPzVXIA7Hfz0"))
	require.True(t, IsContentAddressedResource("ar://jL-YU1yUcZ5aWPku6dcjwLnoS-E0qs2QPzVXIA7Hfz0"))
	require.True(t, IsContentAddressedResource("ipfs://bafkreiasibks3ncaz4tbcedhqgwqoaxvipluqv5bhwboq2yny63omyll5i"))
}

func TestItCanCopyAnArweaveOrIPFSHTTPFileToS3(t *testing.T) {
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

	gateway, _ := url.Parse(ts.URL)
	config.ImportArweaveGatewayURLs = []*url.URL{gateway}
	defer func() { config.ImportArweaveGatewayURLs = []*url.URL{} }()

	// Create a temporary "S3" location to write to
	outputFile := filepath.Join(outputDir, "filename.txt")

	// Do the copy
	err = CopyDStorageToS3("ar://jL-YU1yUcZ5aWPku6dcjwLnoS-E0qs2QPzVXIA7Hfz0", outputFile)
	require.NoError(t, err)

	// Check that the file has the contents we'd expect
	dat, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	require.Equal(t, "some file contents", string(dat))
}
