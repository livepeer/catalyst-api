package pipeline

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

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
