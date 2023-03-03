package pipeline

import (
	"net/url"
	"testing"

	"github.com/livepeer/catalyst-api/config"
	"github.com/stretchr/testify/require"
)

func Test_inSameDirectory(t *testing.T) {
	type args struct {
		base  string
		paths []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "happy",
			args: args{base: "https://foo.bar/a/b/c.mp4", paths: []string{"source", "file.mp4"}},
			want: "https://foo.bar/a/b/source/file.mp4",
		},
		{
			name: "short path",
			args: args{base: "https://foo.bar/c.mp4", paths: []string{"file.mp4"}},
			want: "https://foo.bar/file.mp4",
		},
		{
			name: "no path",
			args: args{base: "https://foo.bar", paths: []string{"file.mp4"}},
			want: "https://foo.bar/file.mp4",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			base, err := url.Parse(tc.args.base)
			require.NoError(t, err)
			got, err := inSameDirectory(*base, tc.args.paths...)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.String())
		})
	}
}

func TestItConvertsS3TargetURLToMistTargetURLCorrectly(t *testing.T) {
	initialTargetURL, err := url.Parse("s3+https://abc:def@storage.googleapis.com/a/b/c/index.m3u8")
	require.NoError(t, err)

	mistTargetURL, err := targetURLToMistTargetURL(*initialTargetURL, config.DefaultSegmentSizeSecs)
	require.NoError(t, err)

	require.Equal(
		t,
		"s3+https://abc:def@storage.googleapis.com/a/b/c/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10",
		mistTargetURL,
	)
}

func TestItConvertsLocalPathToMistTargetCorrectly(t *testing.T) {
	initialTargetURL, err := url.Parse("/a/b/c/index.m3u8")
	require.NoError(t, err)

	mistTargetURL, err := targetURLToMistTargetURL(*initialTargetURL, config.DefaultSegmentSizeSecs)
	require.NoError(t, err)

	require.Equal(
		t,
		"/a/b/c/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10",
		mistTargetURL,
	)
}
