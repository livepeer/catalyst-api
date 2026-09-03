package clients

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/config"
	catErrs "github.com/livepeer/catalyst-api/errors"
	"github.com/stretchr/testify/require"
)

func TestIsPublicIP(t *testing.T) {
	tests := []struct {
		ip     string
		public bool
	}{
		{"8.8.8.8", true},
		{"1.1.1.1", true},
		{"9.255.255.255", true},
		{"11.0.0.0", true},
		{"100.63.255.255", true},
		{"100.128.0.0", true},
		{"172.15.255.255", true},
		{"172.32.0.0", true},
		{"192.167.255.255", true},
		{"192.169.0.0", true},
		{"198.17.255.255", true},
		{"198.20.0.0", true},
		{"2606:4700:4700::1111", true},
		{"0.0.0.0", false},
		{"0.0.0.1", false},
		{"10.0.0.1", false},
		{"100.64.0.1", false},
		{"100.127.255.255", false},
		{"127.0.0.1", false},
		{"169.254.169.254", false},
		{"172.16.0.1", false},
		{"172.31.255.255", false},
		{"192.168.0.1", false},
		{"192.0.2.1", false},
		{"192.31.196.1", false},
		{"192.52.193.1", false},
		{"192.88.99.1", false},
		{"192.175.48.1", false},
		{"198.18.0.1", false},
		{"198.19.255.255", false},
		{"224.0.0.1", false},
		{"240.0.0.1", false},
		{"::", false},
		{"::1", false},
		{"::ffff:10.0.0.1", false},
		{"64:ff9b::a00:1", false},
		{"2001:2::1", false},
		{"2001:20::1", false},
		{"2002:0a00:1::", false},
		{"3fff::1", false},
		{"5f00::1", false},
		{"fc00::1", false},
		{"fe80::1", false},
		{"ff02::1", false},
	}

	for _, tt := range tests {
		t.Run(tt.ip, func(t *testing.T) {
			require.Equal(t, tt.public, isPublicIP(net.ParseIP(tt.ip)))
		})
	}
}

func TestPublicNetworkControlRejectsNonPublicAddresses(t *testing.T) {
	control := publicNetworkControl("network")
	for _, address := range []string{"127.0.0.1:80", "10.0.0.1:80", "169.254.169.254:80", "[::1]:80", "[fc00::1]:80"} {
		require.ErrorContains(t, control(context.Background(), "tcp", address, nil), "non-public address")
	}
	require.NoError(t, control(context.Background(), "tcp", "8.8.8.8:443", nil))
}

func TestImportHTTPClientRejectsPrivateRedirect(t *testing.T) {
	var calls atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{"http://127.0.0.1/private"}},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})
	client := newPublicHTTPClientWithTransportForSurface(time.Second, "source", true, true, transport)

	resp, err := client.Get("http://public.example/source")
	if resp != nil {
		resp.Body.Close()
	}
	require.ErrorContains(t, err, "not public")
	require.Equal(t, int32(1), calls.Load())
}

func TestPublicHTTPClientRejectsInsecureInitialRequest(t *testing.T) {
	transport := &rejectingRoundTripper{err: errors.New("unexpected request")}
	client := newPublicHTTPClientWithTransportForSurface(time.Second, "network", false, false, transport)

	_, err := client.Get("http://example.com/token")
	require.ErrorContains(t, err, "scheme")
	require.Zero(t, transport.calls.Load())
}

type rejectingRoundTripper struct {
	err   error
	calls atomic.Int32
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestWriteTemporaryProbeFileRejectsOversizedInputAndCleansUp(t *testing.T) {
	before, err := filepath.Glob(filepath.Join(os.TempDir(), "public-probe-*"))
	require.NoError(t, err)

	stream, err := boundedFetchStream(io.NopCloser(strings.NewReader("oversized")), -1, false, 4, nil)
	require.NoError(t, err)
	result, err := fetchStreamToTemp(stream, nil)
	require.Error(t, err)
	require.True(t, catErrs.IsUnretriable(err))
	require.Nil(t, result)

	after, globErr := filepath.Glob(filepath.Join(os.TempDir(), "public-probe-*"))
	require.NoError(t, globErr)
	require.ElementsMatch(t, before, after)
}

func TestPublicFetcherClassifiesRangeFailures(t *testing.T) {
	source, err := ParsePublicImportURL("https://public.example/media.mp4")
	require.NoError(t, err)
	for _, test := range []struct {
		status      int
		unretriable bool
		notFound    bool
	}{
		{http.StatusNotFound, true, true},
		{http.StatusRequestedRangeNotSatisfiable, true, false},
		{http.StatusServiceUnavailable, false, false},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			fetcher := PublicFetcher{HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: test.status, Header: make(http.Header), Body: http.NoBody, Request: req}, nil
			})}}
			result, err := fetcher.FetchToTemp(context.Background(), "request", source, &ByteRange{Offset: 2, Length: 4}, 10)
			require.Nil(t, result)
			require.Equal(t, test.unretriable, catErrs.IsUnretriable(err))
			require.Equal(t, test.notFound, catErrs.IsObjectNotFound(err))
			code, ok := catErrs.PublicCode(err)
			require.True(t, ok)
			require.Equal(t, catErrs.PublicErrorFileInaccessible, code)
		})
	}
}

func (r *rejectingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls.Add(1)
	return nil, r.err
}

func TestDStorageDownloadUsesSuppliedHTTPClient(t *testing.T) {
	gateways := config.ImportIPFSGatewayURLs
	config.ImportIPFSGatewayURLs = nil
	t.Cleanup(func() { config.ImportIPFSGatewayURLs = gateways })

	blocked := errors.New("blocked by import policy")
	transport := &rejectingRoundTripper{err: blocked}
	dStorage := NewDStorageDownload(&http.Client{Transport: transport})

	_, err := dStorage.DownloadDStorageFromGatewayList(context.Background(), "https://example.com/ipfs/cid", "requestID")
	require.ErrorIs(t, err, blocked)
	require.Equal(t, int32(1), transport.calls.Load())
}

func TestImportObjectStoreUsesSuppliedHTTPClient(t *testing.T) {
	blocked := errors.New("blocked by import policy")
	transport := &rejectingRoundTripper{err: blocked}

	_, err := getOSURL(
		context.Background(),
		"s3+http://access:secret@example.com/bucket/source.mp4",
		"",
		&http.Client{Transport: transport},
	)
	require.ErrorContains(t, err, blocked.Error())
	require.Positive(t, transport.calls.Load())
}

func TestGetSourceSegmentURLsRejectsPrivateAbsoluteURL(t *testing.T) {
	playlist, err := m3u8.NewMediaPlaylist(1, 1)
	require.NoError(t, err)
	require.NoError(t, playlist.Append("http://169.254.169.254/latest/meta-data", 1, ""))

	_, err = GetSourceSegmentURLs("https://example.com/index.m3u8", *playlist)
	require.ErrorContains(t, err, "invalid source segment URL")
}

func TestPublicHLSProbeStagesSelfContainedPlaylist(t *testing.T) {
	originalClient := retryableHttpClient
	var requestedPaths []string
	var requestedRanges []string
	retryableHttpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestedPaths = append(requestedPaths, req.URL.Path)
		requestedRanges = append(requestedRanges, req.Header.Get("Range"))
		contents := map[string]string{
			"/key.bin":     "0123456789abcdef",
			"/init.mp4":    "init",
			"/segment.m4s": "fragment",
		}[req.URL.Path]
		status := http.StatusOK
		header := make(http.Header)
		if req.URL.Path == "/init.mp4" {
			status = http.StatusPartialContent
			header.Set("Content-Range", "bytes 2-5/20")
		}
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: header, Body: io.NopCloser(strings.NewReader(contents)), Request: req}, nil
	})}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	playlist := decodeMediaPlaylist(t, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:12\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x0000000000000000000000000000000c\n#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"4@2\"\n#EXTINF:4,\nsegment.m4s\n#EXT-X-ENDLIST\n")
	manifestPath, cleanup, err := DownloadPublicHLSSegmentProbeToTemporary(context.Background(), "request", "http://public.example/index.m3u8", playlist, 0)
	require.NoError(t, err)
	require.NotNil(t, cleanup)

	manifest, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	_, playlistType, err := m3u8.Decode(*bytes.NewBuffer(manifest), true)
	require.NoError(t, err)
	require.Equal(t, m3u8.MEDIA, playlistType)
	require.NotContains(t, string(manifest), "public.example")
	require.Contains(t, string(manifest), "#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"")
	require.Contains(t, string(manifest), "#EXT-X-MAP:URI=\"init.mp4\"")
	require.NotContains(t, string(manifest), "BYTERANGE")
	require.Contains(t, string(manifest), "\nsegment.m4s\n")
	require.Equal(t, []string{"/key.bin", "/init.mp4", "/segment.m4s"}, requestedPaths)
	require.Equal(t, []string{"", "bytes=2-5", ""}, requestedRanges)

	dir := filepath.Dir(manifestPath)
	for name, expected := range map[string]string{"key.bin": "0123456789abcdef", "init.mp4": "init", "segment.m4s": "fragment"} {
		contents, readErr := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, readErr)
		require.Equal(t, expected, string(contents))
	}
	cleanup()
	_, err = os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPublicHLSProbeRejectsPrivateDependenciesBeforeDownload(t *testing.T) {
	originalClient := retryableHttpClient
	transport := &rejectingRoundTripper{err: errors.New("unexpected download")}
	retryableHttpClient = &http.Client{Transport: transport}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	for _, test := range []struct {
		name     string
		manifest string
	}{
		{
			name:     "key",
			manifest: "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-KEY:METHOD=AES-128,URI=\"http://169.254.169.254/key\"\n#EXTINF:4,\nsegment.ts\n#EXT-X-ENDLIST\n",
		},
		{
			name:     "initialization section",
			manifest: "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-MAP:URI=\"http://169.254.169.254/init.mp4\"\n#EXTINF:4,\nsegment.m4s\n#EXT-X-ENDLIST\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			playlist := decodeMediaPlaylist(t, test.manifest)
			_, cleanup, err := DownloadPublicHLSSegmentProbeToTemporary(context.Background(), "request", "https://public.example/index.m3u8", playlist, 0)
			require.Error(t, err)
			require.True(t, IsDestinationPolicyError(err))
			require.Nil(t, cleanup)
		})
	}
	require.Zero(t, transport.calls.Load())
}

func TestPublicHLSProbeDoesNotApplyLaterKeyToEarlierSegment(t *testing.T) {
	originalClient := retryableHttpClient
	var requestedPaths []string
	retryableHttpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestedPaths = append(requestedPaths, req.URL.Path)
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader("media")), Request: req}, nil
	})}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	playlist := decodeMediaPlaylist(t, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\n0.ts\n#EXT-X-KEY:METHOD=AES-128,URI=\"http://169.254.169.254/key\"\n#EXTINF:4,\n1.ts\n#EXT-X-ENDLIST\n")
	manifestPath, cleanup, err := DownloadPublicHLSSegmentProbeToTemporary(context.Background(), "request", "https://public.example/index.m3u8", playlist, 0)
	require.NoError(t, err)
	defer cleanup()
	require.Equal(t, []string{"/0.ts"}, requestedPaths)
	manifest, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.NotContains(t, string(manifest), "EXT-X-KEY")
}

func TestPublicHLSProbeFetchesSelectedSegmentDependenciesAndByteRange(t *testing.T) {
	originalClient := retryableHttpClient
	var requests []string
	retryableHttpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests = append(requests, req.URL.Path+" "+req.Header.Get("Range"))
		header := make(http.Header)
		status := http.StatusOK
		body := "media-data"
		if req.Header.Get("Range") != "" {
			status = http.StatusPartialContent
			header.Set("Content-Range", "bytes 4-7/10")
			body = "data"
		}
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	playlist := decodeMediaPlaylist(t, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-KEY:METHOD=AES-128,URI=\"key-0.bin\"\n#EXT-X-MAP:URI=\"init-0.mp4\"\n#EXT-X-BYTERANGE:4@0\n#EXTINF:4,\nmedia.mp4\n#EXT-X-KEY:METHOD=AES-128,URI=\"key-1.bin\"\n#EXT-X-MAP:URI=\"init-1.mp4\"\n#EXT-X-BYTERANGE:4\n#EXTINF:4,\nmedia.mp4\n#EXT-X-ENDLIST\n")
	manifestPath, cleanup, err := DownloadPublicHLSSegmentProbeToTemporary(context.Background(), "request", "https://public.example/index.m3u8", playlist, 1)
	require.NoError(t, err)
	defer cleanup()
	require.Equal(t, []string{"/key-1.bin ", "/init-1.mp4 ", "/media.mp4 bytes=4-7"}, requests)

	manifest, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.NotContains(t, string(manifest), "EXT-X-BYTERANGE")
	require.NotContains(t, string(manifest), "key-0")
	require.NotContains(t, string(manifest), "init-0")
}

func TestPublicHLSProbePreservesRangeWhenServerIgnoresIt(t *testing.T) {
	originalClient := retryableHttpClient
	retryableHttpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, "bytes=4-7", req.Header.Get("Range"))
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader("0123456789")), Request: req}, nil
	})}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	playlist := decodeMediaPlaylist(t, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-BYTERANGE:4@4\n#EXTINF:4,\nmedia.mp4\n#EXT-X-ENDLIST\n")
	manifestPath, cleanup, err := DownloadPublicHLSSegmentProbeToTemporary(context.Background(), "request", "https://public.example/index.m3u8", playlist, 0)
	require.NoError(t, err)
	defer cleanup()
	manifest, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.Contains(t, string(manifest), "#EXT-X-BYTERANGE:4@4")
}

func TestPublicHLSProbeCleansPackageWhenDependencyDownloadFails(t *testing.T) {
	probeTempDir := t.TempDir()
	t.Setenv("TMPDIR", probeTempDir)

	originalClient := retryableHttpClient
	retryableHttpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/segment.m4s" {
			return nil, errors.New("segment unavailable")
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader("dependency")), Request: req}, nil
	})}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	playlist := decodeMediaPlaylist(t, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4,\nsegment.m4s\n#EXT-X-ENDLIST\n")
	filename, cleanup, err := DownloadPublicHLSSegmentProbeToTemporary(context.Background(), "request", "https://public.example/index.m3u8", playlist, 0)
	require.ErrorContains(t, err, "segment unavailable")
	require.Empty(t, filename)
	require.Nil(t, cleanup)
	requireNoTemporaryProbeArtifacts(t, probeTempDir)
}

func decodeMediaPlaylist(t *testing.T, raw string) m3u8.MediaPlaylist {
	t.Helper()
	playlist, playlistType, err := m3u8.Decode(*bytes.NewBufferString(raw), true)
	require.NoError(t, err)
	require.Equal(t, m3u8.MEDIA, playlistType)
	media, ok := playlist.(*m3u8.MediaPlaylist)
	require.True(t, ok)
	return *media
}
