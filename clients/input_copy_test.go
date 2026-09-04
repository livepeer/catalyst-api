package clients

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
)

type recordingProbe struct {
	url     string
	options []string
	calls   int
}

type fileReadingProbe struct {
	path     string
	contents []byte
}

type failingProbe struct {
	path string
}

func (p *failingProbe) ProbeFile(_ string, path string, _ ...string) (video.InputVideo, error) {
	p.path = path
	return video.InputVideo{}, errors.New("probe failed")
}

func (p *failingProbe) CheckFirstFrame(string) (string, error) {
	return "", errors.New("probe failed")
}

type cancelingReadCloser struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (r *cancelingReadCloser) Read([]byte) (int, error) {
	r.cancel()
	return 0, r.ctx.Err()
}

func (r *cancelingReadCloser) Close() error {
	return nil
}

func (p *fileReadingProbe) ProbeFile(_ string, path string, _ ...string) (video.InputVideo, error) {
	p.path = path
	contents, err := os.ReadFile(path)
	if err != nil {
		return video.InputVideo{}, err
	}
	p.contents = contents
	return video.InputVideo{Tracks: []video.InputTrack{{Type: video.TrackTypeVideo, DurationSec: 1, VideoTrack: video.VideoTrack{FPS: 30}}}}, nil
}

func (p *fileReadingProbe) CheckFirstFrame(string) (string, error) {
	return "I", nil
}

func (p *recordingProbe) ProbeFile(_ string, rawURL string, options ...string) (video.InputVideo, error) {
	p.url = rawURL
	p.options = append([]string{}, options...)
	p.calls++
	return video.InputVideo{Tracks: []video.InputTrack{{Type: video.TrackTypeVideo, DurationSec: 1, VideoTrack: video.VideoTrack{FPS: 30}}}}, nil
}

func (p *recordingProbe) CheckFirstFrame(string) (string, error) {
	return "I", nil
}

func Test_isHLSInput(t *testing.T) {
	tests := []struct {
		name      string
		inputFile string
		want      bool
	}{
		{
			name:      "valid manifest",
			inputFile: "https://lp-us-vod-com.storage.googleapis.com/directUpload/2697c12g97x2sxn4/index.m3u8",
			want:      true,
		},
		{
			name:      "invalid manifest",
			inputFile: "https://lp-us-vod-com.storage.googleapis.com/2697c12g97x2sxn4",
			want:      false,
		},
		{
			name:      "invalid manifest",
			inputFile: "https://lp-us-vod-com.storage.HELLO.com/2697c12g97x2sxn4/video.mp4",
			want:      false,
		},
		{
			name:      "invalid manifest",
			inputFile: "s3+https://lp-us-vod-com.storage.googleapis.com/directUpload/2697c12g97x2sxn4/output.m3u",
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputURL, err := url.Parse(tt.inputFile)
			require.NoError(t, err)
			require.Equal(t, tt.want, IsHLSInput(inputURL))
		})
	}
}

func Test_getSegmentTransferLocation(t *testing.T) {
	tests := []struct {
		name                   string
		srcManifestUrl         string
		srcSegmentUrl          string
		dstManifestTransferUrl string
		want                   string
	}{
		{
			name:                   "m3u8 manifest and segments in same root",
			srcManifestUrl:         "https://storage.googleapis.com/monster/hls/123456/789/output.m3u8",
			srcSegmentUrl:          "https://storage.googleapis.com/monster/hls/123456/789/0.ts",
			dstManifestTransferUrl: "s3+https://USER:PASS@storage.googleapis.com/monster/hls/source/abcdef/transfer/output.m3u8",
			want:                   "s3+https://USER:PASS@storage.googleapis.com/monster/hls/source/abcdef/transfer/0.ts",
		},
		{
			name:                   "segments one folder deep",
			srcManifestUrl:         "https://storage.googleapis.com/monster/hls/123456/789/output.m3u8",
			srcSegmentUrl:          "https://storage.googleapis.com/monster/hls/123456/789/segments/0.ts",
			dstManifestTransferUrl: "s3+https://USER:PASS@storage.googleapis.com/monster/hls/source/abcdef/transfer/output.m3u8",
			want:                   "s3+https://USER:PASS@storage.googleapis.com/monster/hls/source/abcdef/transfer/segments/0.ts",
		},
		{
			name:                   "m3u8 manifest and segments in different hosts but same path",
			srcManifestUrl:         "https://storage.googleapis.com/monster/hls/123456/789/output.m3u8",
			srcSegmentUrl:          "https://storage.hello.com/monster/hls/123456/789/segments/0.ts",
			dstManifestTransferUrl: "s3+https://USER:PASS@storage.googleapis.com/monster/hls/source/abcdef/transfer/output.m3u8",
			want:                   "s3+https://USER:PASS@storage.googleapis.com/monster/hls/source/abcdef/transfer/segments/0.ts",
		},
		{
			name:                   "m3u8 manifest and segments in different hosts and path",
			srcManifestUrl:         "https://storage.googleapis.com/monster/1234/output.m3u8",
			srcSegmentUrl:          "https://storage.hello.com/live/hls/123456/789/segments/0.ts",
			dstManifestTransferUrl: "s3+https://USER:PASS@storage.googleapis.com/monster/hls/source/abcdef/transfer/output.m3u8",
			want:                   "s3+https://USER:PASS@storage.googleapis.com/monster/hls/source/abcdef/transfer/live/hls/123456/789/segments/0.ts",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcManifest, err := url.Parse(tt.srcManifestUrl)
			require.NoError(t, err)
			srcSegment, err := url.Parse(tt.srcSegmentUrl)
			require.NoError(t, err)
			dstManifestTransfer, err := url.Parse(tt.dstManifestTransferUrl)
			require.NoError(t, err)
			url, err := getSegmentTransferLocation(srcManifest, dstManifestTransfer, srcSegment.String())
			require.NoError(t, err)
			require.Equal(t, tt.want, url)
		})
	}
}

func TestHLSDurationSet(t *testing.T) {
	i := InputCopy{
		Probe: video.Probe{},
	}
	inputFile, _ := url.Parse("../test/fixtures/tiny.m3u8")
	iv, _, err := i.CopyInputToS3("requestID", inputFile, &url.URL{}, nil)
	require.NoError(t, err)
	videoTrack, _ := iv.GetTrack(video.TrackTypeVideo)
	require.Equal(t, 30.0, videoTrack.DurationSec)
}

func TestPublicHLSProbeUsesGuardedDownloadAndLocalFile(t *testing.T) {
	originalClient := retryableHttpClient
	retryableHttpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := "media"
		if strings.HasSuffix(req.URL.Path, ".m3u8") {
			body = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nhttps://public.example/0.ts\n#EXT-X-ENDLIST\n"
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	probe := &recordingProbe{}
	input := &InputCopy{Probe: probe}
	manifest, err := url.Parse("https://public.example/index.m3u8")
	require.NoError(t, err)
	result, _, err := input.CopyInputToS3("request", manifest, &url.URL{}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, probe.calls)
	require.NotContains(t, probe.url, "public.example")
	require.Contains(t, probe.options, "file,crypto")
	require.Equal(t, "hls", result.Format)
	require.Equal(t, 4.0, result.Duration)
	_, err = os.Stat(probe.url)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestClippedHLSProbeStagesRelativeSegment(t *testing.T) {
	originalClient := retryableHttpClient
	var requestedPaths []string
	retryableHttpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestedPaths = append(requestedPaths, req.URL.Path)
		body := "media"
		if strings.HasSuffix(req.URL.Path, ".m3u8") {
			body = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\n../segments/0.ts\n#EXT-X-ENDLIST\n"
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	probe := &recordingProbe{}
	input := &InputCopy{Probe: probe}
	manifest, err := url.Parse("https://public.example/clips/job/clip.m3u8")
	require.NoError(t, err)
	result, _, err := input.CopyInputToS3("request", manifest, &url.URL{}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"/clips/job/clip.m3u8", "/clips/segments/0.ts"}, requestedPaths)
	require.Equal(t, 1, probe.calls)
	require.NotContains(t, probe.url, "public.example")
	require.Contains(t, probe.options, "file,crypto")
	require.Equal(t, "hls", result.Format)
	require.Equal(t, 4.0, result.Duration)
	_, err = os.Stat(probe.url)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPublicHLSProbeRejectsPrivateSegmentBeforeProber(t *testing.T) {
	originalClient := retryableHttpClient
	retryableHttpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nhttps://169.254.169.254/latest/meta-data\n#EXT-X-ENDLIST\n"
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	probe := &recordingProbe{}
	input := &InputCopy{Probe: probe}
	manifest, err := url.Parse("https://public.example/index.m3u8")
	require.NoError(t, err)
	_, _, err = input.CopyInputToS3("request", manifest, &url.URL{}, nil)
	require.ErrorContains(t, err, "invalid source segment URL")
	require.Zero(t, probe.calls)
}

func TestPublicSourceCopyProbesBytesStagedDuringUpload(t *testing.T) {
	originalClient := retryableHttpClient
	var requestedURLs []string
	retryableHttpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestedURLs = append(requestedURLs, req.URL.String())
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("source media")),
			Request:    req,
		}, nil
	})}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	probe := &fileReadingProbe{}
	input := &InputCopy{Probe: probe}
	source, err := url.Parse("https://public.example/video.mp4")
	require.NoError(t, err)
	destinationPath := filepath.Join(t.TempDir(), "video.mp4")
	destination := &url.URL{Scheme: "file", Path: destinationPath}

	_, _, err = input.CopyInputToS3("request", source, destination, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"https://public.example/video.mp4"}, requestedURLs)
	require.Equal(t, []byte("source media"), probe.contents)
	require.NotEqual(t, destinationPath, probe.path)
	destinationContents, err := os.ReadFile(destinationPath)
	require.NoError(t, err)
	require.Equal(t, []byte("source media"), destinationContents)
	_, err = os.Stat(probe.path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestTemporaryProbeFileRemovedAfterCopyFailures(t *testing.T) {
	for _, test := range []struct {
		name        string
		transport   http.RoundTripper
		destination func(*testing.T) *url.URL
	}{
		{
			name: "download failure",
			transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("download failed")
			}),
			destination: temporaryOutputURL,
		},
		{
			name: "upload failure",
			transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("source media")),
					Request:    req,
				}, nil
			}),
			destination: func(t *testing.T) *url.URL {
				t.Helper()
				path := filepath.Join(t.TempDir(), "existing-directory")
				require.NoError(t, os.Mkdir(path, 0o700))
				return &url.URL{Scheme: "file", Path: path}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			probeTempDir := t.TempDir()
			t.Setenv("TMPDIR", probeTempDir)

			originalClient := retryableHttpClient
			retryableHttpClient = &http.Client{Transport: test.transport}
			t.Cleanup(func() { retryableHttpClient = originalClient })

			source, err := url.Parse("https://public.example/video.mp4")
			require.NoError(t, err)
			filename, cleanup, err := copyInputFileToOutputAndTemporary("request", source, test.destination(t), nil)
			require.Error(t, err)
			require.Empty(t, filename)
			require.Nil(t, cleanup)
			requireNoTemporaryProbeArtifacts(t, probeTempDir)
		})
	}
}

func TestTemporaryProbeFileRemovedAfterProbeFailure(t *testing.T) {
	originalClient := retryableHttpClient
	retryableHttpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("source media")),
			Request:    req,
		}, nil
	})}
	t.Cleanup(func() { retryableHttpClient = originalClient })

	probe := &failingProbe{}
	input := &InputCopy{Probe: probe}
	source, err := url.Parse("https://public.example/video.mp4")
	require.NoError(t, err)
	destination := temporaryOutputURL(t)

	_, _, err = input.CopyInputToS3("request", source, destination, nil)
	require.ErrorContains(t, err, "probe failed")
	require.NotEmpty(t, probe.path)
	_, err = os.Stat(probe.path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestTemporaryProbeFileRemovedAfterCancellation(t *testing.T) {
	probeTempDir := t.TempDir()
	t.Setenv("TMPDIR", probeTempDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source, err := ParsePublicImportURL("https://public.example/video.mp4")
	require.NoError(t, err)
	fetcher := PublicFetcher{HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       &cancelingReadCloser{ctx: req.Context(), cancel: cancel},
			Request:    req,
		}, nil
	})}}

	_, err = fetcher.FetchToTemp(ctx, "request", source, nil, MaxTemporaryProbeFileSizeBytes)
	require.ErrorIs(t, err, context.Canceled)
	requireNoTemporaryProbeArtifacts(t, probeTempDir)
}

func temporaryOutputURL(t *testing.T) *url.URL {
	t.Helper()
	return &url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "video.mp4")}
}

func requireNoTemporaryProbeArtifacts(t *testing.T, dir string) {
	t.Helper()
	for _, pattern := range []string{"public-probe-*", "public-hls-probe-*"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		require.NoError(t, err)
		require.Empty(t, matches)
	}
}
