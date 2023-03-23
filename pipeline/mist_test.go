package pipeline

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/livepeer/catalyst-api/clients"
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

func TestSegmentedVersusSourceDurationComparison(t *testing.T) {
	type TestCase struct {
		Name           string
		SourceDuration int64
		OutputDuration int64
		ShouldSucceed  bool
	}

	testCases := []TestCase{
		{
			Name:           "Segmented Output Is Shorter Than Source",
			SourceDuration: 1000,
			OutputDuration: 1,
			ShouldSucceed:  false,
		},
		{
			Name:           "Segmented Output Is Longer Than Source",
			SourceDuration: 1000,
			OutputDuration: 1900,
			ShouldSucceed:  true,
		},
		{
			Name:           "Segmented Output Is Much Longer Than Source",
			SourceDuration: 1000,
			OutputDuration: 2500,
			ShouldSucceed:  false,
		},
		{
			Name:           "Segmented Output Is Slightly Shorter Than Source",
			SourceDuration: 1000,
			OutputDuration: 990,
			ShouldSucceed:  true,
		},
		{
			Name:           "Segmented Output Is Equal To Source",
			SourceDuration: 1000,
			OutputDuration: 1000,
			ShouldSucceed:  true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			err := CheckSegmentedDurationWithinBounds(
				clients.MistStreamInfo{
					Meta: clients.MistStreamInfoMetadata{
						Tracks: map[string]clients.MistStreamInfoTrack{
							"video": {
								Lastms: testCase.SourceDuration,
							},
						},
					},
				},
				testCase.OutputDuration,
			)

			if testCase.ShouldSucceed {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, fmt.Sprintf("input video duration (%dms) does not match segmented video duration (%dms)", testCase.SourceDuration, testCase.OutputDuration))
			}

		})
	}
}
