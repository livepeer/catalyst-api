package clients

import (
	"net/url"
	"testing"

	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
)

func Test_isDirectUpload(t *testing.T) {
	tests := []struct {
		name      string
		inputFile string
		want      bool
	}{
		{
			name:      "direct upload w/ directUpload in path",
			inputFile: "https://lp-us-vod-com.storage.googleapis.com/directUpload/2697c12g97x2sxn4",
			want:      true,
		},
		{
			name:      "direct upload w/o directUpload in path",
			inputFile: "https://lp-us-vod-com.storage.googleapis.com/2697c12g97x2sxn4",
			want:      false,
		},
		{
			name:      "not direct upload w/ directUpload in path",
			inputFile: "https://lp-us-vod-com.storage.HELLO.com/2697c12g97x2sxn4",
			want:      false,
		},
		{
			name:      "not direct upload",
			inputFile: "s3+https://lp-us-vod-com.storage.googleapis.com/directUpload/2697c12g97x2sxn4",
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputURL, err := url.Parse(tt.inputFile)
			require.NoError(t, err)
			require.Equal(t, tt.want, isDirectUpload(inputURL))
		})
	}
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
