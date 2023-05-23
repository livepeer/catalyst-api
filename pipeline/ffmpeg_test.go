package pipeline

import (
	"net/url"
	"os"
	"testing"
	"time"

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
			sendSourcePlayback(tt.job)

			if tt.shouldWriteSourcePlaylist {
				hlsTarget := tt.job.HlsTargetURL.JoinPath("index.m3u8")
				contents, err := os.ReadFile(hlsTarget.String())
				require.NoError(t, err)
				contentsString := string(contents)
				require.Contains(t, contentsString, "#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=123,RESOLUTION=10x10,NAME=\"10p\"\n")
				require.Contains(t, contentsString, "/path")

				require.Equal(t, hlsTarget.String(), callbackClient.tsm.SourcePlayback.Manifest)
			} else {
				entries, err := os.ReadDir(tmpDir)
				require.NoError(t, err)
				require.Empty(t, entries)
			}
		})
	}
}
