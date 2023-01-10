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
	require.False(t, IsDStorageResource("http://google.com"))
	require.False(t, IsDStorageResource("https://twitter.com/search?q=arweave"))
	require.False(t, IsDStorageResource("arr://jL-YU1yUcZ5aWPku6dcjwLnoS-E0qs2QPzVXIA7Hfz0"))
	require.True(t, IsDStorageResource("ar://jL-YU1yUcZ5aWPku6dcjwLnoS-E0qs2QPzVXIA7Hfz0"))
	require.True(t, IsDStorageResource("ipfs://bafkreiasibks3ncaz4tbcedhqgwqoaxvipluqv5bhwboq2yny63omyll5i"))
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
	err = CopyDStorageToS3("ar://jL-YU1yUcZ5aWPku6dcjwLnoS-E0qs2QPzVXIA7Hfz0", outputFile, "reqID")
	require.NoError(t, err)

	// Check that the file has the contents we'd expect
	dat, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	require.Equal(t, "some file contents", string(dat))
}

func TestItHandlesPinataGatewayTokenAsQueryString(t *testing.T) {
	outputDir, err := os.MkdirTemp(os.TempDir(), "TestItHandlesPinataGatewayTokenAsQueryString-*")
	require.NoError(t, err)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "tokenValue", r.URL.Query().Get("pinataGatewayToken"))
		_, err := w.Write([]byte("some file contents"))
		require.NoError(t, err)
	}))
	defer ts.Close()

	gateway, _ := url.Parse(ts.URL + "/?pinataGatewayToken=tokenValue")
	config.ImportIPFSGatewayURLs = []*url.URL{gateway}
	defer func() { config.ImportIPFSGatewayURLs = []*url.URL{} }()
	defer func() { config.LP_PINATA_GATEWAY_TOKEN = "" }()

	outputFile := filepath.Join(outputDir, "filename.txt")

	err = CopyDStorageToS3("ipfs://Qme7ss3ARVgxv6rXqVPiikMJ8u2NLgmgszg13pYrDKEoiu", outputFile, "reqID")
	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	require.Equal(t, "some file contents", string(data))
}
