package pipeline

import (
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
)

func TestItCleansUpLocalFiles(t *testing.T) {
	// Create some temporary files
	f1, err := os.CreateTemp(os.TempDir(), "tempfile1")
	require.NoError(t, err)
	f2, err := os.CreateTemp(os.TempDir(), "tempfile_2")
	require.NoError(t, err)
	f3, err := os.CreateTemp(os.TempDir(), "tempfilethree")
	require.NoError(t, err)
	f4, err := os.CreateTemp(os.TempDir(), "do_not_delete")
	require.NoError(t, err)

	// Try to clean them up
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, cleanUpLocalTmpFiles(os.TempDir(), "tempfile*", time.Microsecond))

	// Confirm that the ones we expected to be deleted are gone
	_, err = os.Stat(f1.Name())
	require.Error(t, err)
	_, err = os.Stat(f2.Name())
	require.Error(t, err)
	_, err = os.Stat(f3.Name())
	require.Error(t, err)

	// Confirm that the ones we expected to not be deleted isn't gone
	_, err = os.Stat(f4.Name())
	require.NoError(t, err)
}

type mockCallbackClient struct {
	tsm clients.TranscodeStatusMessage
}

func (s *mockCallbackClient) SendTranscodeStatus(tsm clients.TranscodeStatusMessage) error {
	s.tsm = tsm
	return nil
}

func Test_sendSourcePlayback(t *testing.T) {
	mustParseUrl := func(u string, t *testing.T) *url.URL {
		parsed, err := url.Parse(u)
		require.NoError(t, err)
		return parsed
	}
	ff := ffmpeg{
		sourcePlaybackHosts: map[string]string{
			"http://lp-us-catalyst-recordings-monster.storage.googleapis.com/foo":                         "//recordings-cdn.lp-playback.monster/hls",
			"https://link.storjshare.io/raw/jvnqoncawzmc3lb7tstb5ut3d7va/catalyst-recordings-monster/hls": "//link.storjshare.io/raw/jvnqoncawzmc3lb7tstb5ut3d7va/catalyst-recordings-monster/hls",
			"https://link.storjshare.io/raw/jvnqoncawzmc3lb7tstb5ut3d7vb/catalyst-recordings-monster/hls": "//recordings-cdn.lp-playback.monster/hls",
		},
	}

	const (
		requestID           = "requestID"
		segmentingTargetURL = "https://google.com/bucket/path"
	)
	var (
		inputVideo = video.InputVideo{
			Tracks: []video.InputTrack{
				{
					Type:    "video",
					Bitrate: 123,
					VideoTrack: video.VideoTrack{
						Width:  10,
						Height: 10,
					},
				},
			},
		}
	)
	tests := []struct {
		name                      string
		job                       *JobInfo
		shouldWriteSourcePlaylist bool
		expectedRendition         string
	}{
		{
			name: "happy",
			job: &JobInfo{
				SegmentingTargetURL: segmentingTargetURL,
				UploadJobPayload: UploadJobPayload{
					HlsTargetURL: mustParseUrl("/bucket/foo", t),
				},
			},
			shouldWriteSourcePlaylist: true,
			expectedRendition:         "/path",
		},
		{
			name: "host mapping",
			job: &JobInfo{
				SegmentingTargetURL: segmentingTargetURL,
				UploadJobPayload: UploadJobPayload{
					SourceFile:   "http://lp-us-catalyst-recordings-monster.storage.googleapis.com/foo/bar/output.m3u8",
					HlsTargetURL: mustParseUrl("/bucket/foo", t),
				},
			},
			shouldWriteSourcePlaylist: true,
			expectedRendition:         "//recordings-cdn.lp-playback.monster/hls/bar/output.m3u8",
		},
		{
			name: "host mapping - storj",
			job: &JobInfo{
				SegmentingTargetURL: "https://link.storjshare.io/raw/jvnqoncawzmc3lb7tstb5ut3d7va/catalyst-recordings-monster/hls/e88briv8dl7rzg8o-test/3c446cbe-3ca9-4eba-84a9-68b38305d67a/output.m3u8",
				UploadJobPayload: UploadJobPayload{
					SourceFile:   "https://link.storjshare.io/raw/jvnqoncawzmc3lb7tstb5ut3d7va/catalyst-recordings-monster/hls/e88briv8dl7rzg8o-test/3c446cbe-3ca9-4eba-84a9-68b38305d67a/output.m3u8",
					HlsTargetURL: mustParseUrl("/bucket/foo", t),
				},
			},
			shouldWriteSourcePlaylist: true,
			expectedRendition:         "//link.storjshare.io/raw/jvnqoncawzmc3lb7tstb5ut3d7va/catalyst-recordings-monster/hls/e88briv8dl7rzg8o-test/3c446cbe-3ca9-4eba-84a9-68b38305d67a/output.m3u8",
		},
		{
			name: "host mapping - storj cdn",
			job: &JobInfo{
				SegmentingTargetURL: "https://link.storjshare.io/raw/jvnqoncawzmc3lb7tstb5ut3d7vb/catalyst-recordings-monster/hls/e88briv8dl7rzg8o-test/3c446cbe-3ca9-4eba-84a9-68b38305d67a/output.m3u8",
				UploadJobPayload: UploadJobPayload{
					SourceFile:   "https://link.storjshare.io/raw/jvnqoncawzmc3lb7tstb5ut3d7vb/catalyst-recordings-monster/hls/e88briv8dl7rzg8o-test/3c446cbe-3ca9-4eba-84a9-68b38305d67a/output.m3u8",
					HlsTargetURL: mustParseUrl("/bucket/foo", t),
				},
			},
			shouldWriteSourcePlaylist: true,
			expectedRendition:         "//recordings-cdn.lp-playback.monster/hls/e88briv8dl7rzg8o-test/3c446cbe-3ca9-4eba-84a9-68b38305d67a/output.m3u8",
		},
		{
			name: "not standard bucket - no source playback",
			job: &JobInfo{
				SegmentingTargetURL: segmentingTargetURL,
				UploadJobPayload: UploadJobPayload{
					HlsTargetURL: mustParseUrl("/bucketNotMatch/foo", t),
				},
			},
			shouldWriteSourcePlaylist: false,
		},
		{
			name: "private bucket for access control",
			job: &JobInfo{
				SegmentingTargetURL: "https://google.com/lp-us-catalyst-vod-pvt-monster/path",
				UploadJobPayload: UploadJobPayload{
					HlsTargetURL: mustParseUrl("/lp-us-catalyst-vod-pvt-monster/foo", t),
				},
			},
			shouldWriteSourcePlaylist: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "source-pb-test")
			require.NoError(t, err)
			defer os.RemoveAll(tmpDir)

			tt.job.RequestID = requestID
			tt.job.HlsTargetURL = mustParseUrl(tmpDir, t).JoinPath(tt.job.HlsTargetURL.Path)
			tt.job.InputFileInfo = inputVideo
			callbackClient := &mockCallbackClient{}
			tt.job.statusClient = callbackClient
			ff.sendSourcePlayback(tt.job)

			if tt.shouldWriteSourcePlaylist {
				hlsTarget := tt.job.HlsTargetURL.JoinPath("index.m3u8")
				contents, err := os.ReadFile(hlsTarget.String())
				require.NoError(t, err)
				contentsString := string(contents)
				require.Contains(t, contentsString, "#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=123,RESOLUTION=10x10,NAME=\"10p\"\n")
				require.Contains(t, contentsString, tt.expectedRendition)

				require.Equal(t, hlsTarget.String(), callbackClient.tsm.SourcePlayback.Manifest)
			} else {
				entries, err := os.ReadDir(tmpDir)
				require.NoError(t, err)
				require.Empty(t, entries)
			}
		})
	}
}

type stubProbe struct {
	probedUrls []string
}

func (p *stubProbe) ProbeFile(requestID string, url string, ffProbeOptions ...string) (video.InputVideo, error) {
	p.probedUrls = append(p.probedUrls, url)
	return video.InputVideo{}, nil
}

func Test_probeSegments(t *testing.T) {
	probe := stubProbe{}
	f := ffmpeg{
		probe: &probe,
	}
	job := &JobInfo{
		UploadJobPayload: UploadJobPayload{
			RequestID: "requestID",
		},
	}

	// each segment should be probed twice, an extra call is made with loglevel warning
	err := f.probeSourceSegments(job, []*m3u8.MediaSegment{{URI: "0.ts"}})
	require.NoError(t, err)
	require.Equal(t, []string{"/0.ts", "/0.ts"}, probe.probedUrls)

	probe.probedUrls = []string{}
	_ = f.probeSourceSegments(job, []*m3u8.MediaSegment{{URI: "0.ts"}, {URI: "1.ts"}})
	require.Equal(t, []string{"/0.ts", "/0.ts", "/1.ts", "/1.ts"}, probe.probedUrls)

	probe.probedUrls = []string{}
	_ = f.probeSourceSegments(job, []*m3u8.MediaSegment{{URI: "0.ts"}, {URI: "1.ts"}, {URI: "2.ts"}, {URI: "3.ts"}})
	require.Equal(t, []string{"/0.ts", "/0.ts", "/1.ts", "/1.ts", "/2.ts", "/2.ts", "/3.ts", "/3.ts"}, probe.probedUrls)

	probe.probedUrls = []string{}
	_ = f.probeSourceSegments(job, []*m3u8.MediaSegment{{URI: "0.ts"}, {URI: "1.ts"}, {URI: "2.ts"}, {URI: "3.ts"}, {URI: "4.ts"}, {URI: "5.ts"}})
	require.Equal(t, []string{"/0.ts", "/0.ts", "/1.ts", "/1.ts", "/4.ts", "/4.ts", "/5.ts", "/5.ts"}, probe.probedUrls)
}
