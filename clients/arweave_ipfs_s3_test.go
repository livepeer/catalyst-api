package clients

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/go-tools/drivers"
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

func TestDownloadDStorageFromGatewayListRetries(t *testing.T) {
	gatewayCallCount := 0
	var ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatewayCallCount++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	u, err := url.Parse(ts.URL)
	require.NoError(t, err)
	gatewayCount := 4
	for i := 0; i < gatewayCount; i++ {
		config.ImportIPFSGatewayURLs = append(config.ImportIPFSGatewayURLs, u)
	}

	_, err = DownloadDStorageFromGatewayList("ipfs://Qme7ss3ARVgxv6rXqVPiikMJ8u2NLgmgszg13pYrDKEoiu", "reqID", 4)
	require.Error(t, err)
	require.Equal(t, 0, gatewayCallCount)

	_, err = DownloadDStorageFromGatewayList("ipfs://Qme7ss3ARVgxv6rXqVPiikMJ8u2NLgmgszg13pYrDKEoiu", "reqID", 0)
	require.Error(t, err)
	require.Equal(t, 4, gatewayCallCount)

	_, err = DownloadDStorageFromGatewayList("ipfs://Qme7ss3ARVgxv6rXqVPiikMJ8u2NLgmgszg13pYrDKEoiu", "reqID", 2)
	require.Error(t, err)
	require.Equal(t, 6, gatewayCallCount)

	config.ImportIPFSGatewayURLs = []*url.URL{u}
	_, err = DownloadDStorageFromGatewayList("ipfs://Qme7ss3ARVgxv6rXqVPiikMJ8u2NLgmgszg13pYrDKEoiu", "reqID", 0)
	require.Error(t, err)
	require.Equal(t, 7, gatewayCallCount)
}

func TestItExtractsGatewayDStorageType(t *testing.T) {
	u, err := url.Parse("https://cloudflare-ipfs.com/ipfs/12345/file.json?queryString=value")
	require.NoError(t, err)
	id, gateway, gatewayType := parseDStorageGatewayURL(u)
	require.Equal(t, SCHEME_IPFS, gatewayType)
	require.Equal(t, "12345/file.json", id)
	require.Equal(t, "https://cloudflare-ipfs.com/ipfs/?queryString=value", gateway)

	u, err = url.Parse("https://arweave.net/12345")
	require.NoError(t, err)
	id, gateway, gatewayType = parseDStorageGatewayURL(u)
	require.Equal(t, SCHEME_ARWEAVE, gatewayType)
	require.Equal(t, "12345", id)
	require.Equal(t, "https://arweave.net/", gateway)

	u, err = url.Parse("http://not-a-dstorage-url.com/12345")
	require.NoError(t, err)
	id, gateway, gatewayType = parseDStorageGatewayURL(u)
	require.Equal(t, "", gatewayType)
	require.Equal(t, "", id)
	require.Equal(t, "", gateway)
}

func TestItHandlesGatewayURLsAsSource(t *testing.T) {
	resourceId := "Qme7ss3ARVgxv6rXqVPiikMJ8u2NLgmgszg13pYrDKEoiu/file.json"
	outputDir, err := os.MkdirTemp(os.TempDir(), "TestItHandlesGatewayURLsAsSource-*")
	require.NoError(t, err)

	requestedURLs := []string{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedURLs = append(requestedURLs, r.URL.Path)
		if strings.HasPrefix(r.URL.Path, "/fallback/ipfs/") || len(requestedURLs) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			require.Contains(t, r.URL.Path, resourceId)
			_, err := w.Write([]byte("some file contents"))
			require.NoError(t, err)
		}
	}))
	defer ts.Close()

	fallbackGateway, err := url.Parse(ts.URL + "/fallback/ipfs/")
	require.NoError(t, err)
	config.ImportIPFSGatewayURLs = []*url.URL{fallbackGateway}
	defer func() { config.ImportIPFSGatewayURLs = []*url.URL{} }()

	outputFile := filepath.Join(outputDir, "filename.txt")

	err = CopyDStorageToS3(ts.URL+"/ipfs/"+resourceId+"?queryString=value", outputFile, "reqID")
	require.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	require.Equal(t, "some file contents", string(data))
	require.Len(t, requestedURLs, 3)

	// provided gateway is used first
	require.Equal(t, "/ipfs/"+resourceId, requestedURLs[0])

	// our fallback gateway is used
	require.Equal(t, "/fallback/ipfs/"+resourceId, requestedURLs[1])

	// it cat fetch from provided gateway
	require.Equal(t, "/ipfs/"+resourceId, requestedURLs[2])
}

func Test_IPFSResourceIDParsing(t *testing.T) {
	drivers.Testing = true
	tests := []struct {
		name        string
		url         string
		expectedURL string
	}{
		{
			name:        "simple resource ID",
			url:         "ipfs://foo",
			expectedURL: "/foo",
		},
		{
			name:        "resource ID with path",
			url:         "ipfs://foo/bar",
			expectedURL: "/foo/bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, tt.expectedURL, r.URL.String())
			}))
			defer ts.Close()

			gateway, err := url.Parse(ts.URL)
			require.NoError(t, err)
			config.ImportIPFSGatewayURLs = []*url.URL{gateway}

			err = CopyDStorageToS3(tt.url, "memory://foo", "reqID")
			require.NoError(t, err)
		})
	}
}
