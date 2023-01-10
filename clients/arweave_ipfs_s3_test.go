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
	cid := "Qme7ss3ARVgxv6rXqVPiikMJ8u2NLgmgszg13pYrDKEoiu"
	outputDir, err := os.MkdirTemp(os.TempDir(), "TestItHandlesPinataGatewayTokenAsQueryString-*")
	require.NoError(t, err)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "tokenValue", r.URL.Query().Get("pinataGatewayToken"))
		require.Contains(t, r.URL.Path, cid)
		_, err := w.Write([]byte("some file contents"))
		require.NoError(t, err)
	}))
	defer ts.Close()

	gateway, _ := url.Parse(ts.URL + "/ipfs/?pinataGatewayToken=tokenValue")
	config.ImportIPFSGatewayURLs = []*url.URL{gateway}
	defer func() { config.ImportIPFSGatewayURLs = []*url.URL{} }()
	defer func() { config.LP_PINATA_GATEWAY_TOKEN = "" }()

	outputFile := filepath.Join(outputDir, "filename.txt")

	err = CopyDStorageToS3("ipfs://"+cid, outputFile, "reqID")
	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	require.Equal(t, "some file contents", string(data))
}

func TestItTriesWithMultipleGateways(t *testing.T) {
	type TestResult struct {
		gatewayCallLog   []string
		successfulCalls  []string
		gatewayCallCount []int
	}
	var testResult = TestResult{[]string{}, []string{}, []int{0, 0, 0}}

	outputDir, err := os.MkdirTemp(os.TempDir(), "TestItHandlesPinataGatewayTokenAsQueryString-*")
	require.NoError(t, err)

	for i := range testResult.gatewayCallCount {
		serverIndex := i
		var ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			testResult.gatewayCallLog = append(testResult.gatewayCallLog, r.Host)
			count := testResult.gatewayCallCount[serverIndex]
			if count == 0 {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				_, err := w.Write([]byte("some file contents"))
				require.NoError(t, err)
				require.Equal(t, 0, serverIndex)
				testResult.successfulCalls = append(testResult.successfulCalls, r.Host)
			}
			testResult.gatewayCallCount[serverIndex]++
		}))

		defer ts.Close()

		url, err := url.Parse(ts.URL)
		require.NoError(t, err)

		config.ImportIPFSGatewayURLs = append(config.ImportIPFSGatewayURLs, url)
	}

	defer func() { config.ImportIPFSGatewayURLs = []*url.URL{} }()

	outputFile := filepath.Join(outputDir, "filename.txt")

	err = CopyDStorageToS3("ipfs://Qme7ss3ARVgxv6rXqVPiikMJ8u2NLgmgszg13pYrDKEoiu", outputFile, "reqID")
	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	require.Equal(t, "some file contents", string(data))
	require.Equal(t, 2, testResult.gatewayCallCount[0])
	require.Equal(t, 1, testResult.gatewayCallCount[1])
	require.Equal(t, 1, testResult.gatewayCallCount[2])
	require.Len(t, testResult.successfulCalls, 1)
	require.Equal(t, testResult.successfulCalls[0], config.ImportIPFSGatewayURLs[0].Host)
	require.Len(t, testResult.gatewayCallLog, 4)
	var expectedSequence = []string{
		config.ImportIPFSGatewayURLs[0].Host,
		config.ImportIPFSGatewayURLs[1].Host,
		config.ImportIPFSGatewayURLs[2].Host,
		config.ImportIPFSGatewayURLs[0].Host,
	}
	for i, log := range expectedSequence {
		require.Equal(t, log, testResult.gatewayCallLog[i])
	}
}
