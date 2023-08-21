package video

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetDefaultPlaybackProfiles(t *testing.T) {
	tests := []struct {
		name  string
		track InputTrack
		want  []EncodedProfile
	}{
		{
			name: "360p input",
			track: InputTrack{
				Type:    "video",
				Bitrate: 1_000_001,
				VideoTrack: VideoTrack{
					Width:  640,
					Height: 360,
				},
			},
			want: []EncodedProfile{
				{Name: "low-bitrate", Width: 640, Height: 360, Bitrate: 500_000},
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 1_000_001},
			},
		},
		{
			name: "low bitrate 360p input",
			track: InputTrack{
				Type:    "video",
				Bitrate: 500_000,
				VideoTrack: VideoTrack{
					Width:  640,
					Height: 360,
				},
			},
			want: []EncodedProfile{
				{Name: "low-bitrate", Width: 640, Height: 360, Bitrate: 250_000},
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 500_000},
			},
		},
		{
			name: "720p input",
			track: InputTrack{
				Type:    "video",
				Bitrate: 4_000_001,
				VideoTrack: VideoTrack{
					Width:  1280,
					Height: 720,
				},
			},
			want: []EncodedProfile{
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 1_000_000},
				{Name: "720p0", Width: 1280, Height: 720, Bitrate: 4_000_001},
			},
		},
		{
			name: "low bitrate 720p input",
			track: InputTrack{
				Type:    "video",
				Bitrate: 1_000_001,
				VideoTrack: VideoTrack{
					Width:  1200,
					Height: 720,
				},
			},
			want: []EncodedProfile{
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 266_666},
				{Name: "720p0", Width: 1200, Height: 720, Bitrate: 1_000_001},
			},
		},
		{
			name: "1080p input",
			track: InputTrack{
				Type:    "video",
				Bitrate: 5_000_000,
				VideoTrack: VideoTrack{
					Width:  1920,
					Height: 1080,
				},
			},
			want: []EncodedProfile{
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 555_555},
				{Name: "720p0", Width: 1280, Height: 720, Bitrate: 2_222_222},
				{Name: "1080p0", Width: 1920, Height: 1080, Bitrate: 5_000_000},
			},
		},
		{
			name: "240p input with odd number resolution",
			track: InputTrack{
				Type:    "video",
				Bitrate: 517_099,
				VideoTrack: VideoTrack{
					Width:  400,
					Height: 239,
				},
			},
			want: []EncodedProfile{
				{Name: "low-bitrate", Width: 400, Height: 240, Bitrate: 258549},
				{Name: "240p0", Width: 400, Height: 240, Bitrate: 517099},
			},
		},
		{
			name: "input with excessively high bitrate",
			track: InputTrack{
				Type:    "video",
				Bitrate: 500_000_000,
				VideoTrack: VideoTrack{
					Width:  1920,
					Height: 1080,
				},
			},
			want: []EncodedProfile{
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 1_000_000},
				{Name: "720p0", Width: 1280, Height: 720, Bitrate: 4_000_000},
				{Name: "1080p0", Width: 1920, Height: 1080, Bitrate: MaxVideoBitrate},
			},
		},
		{
			name: "low bitrate 1080p", // https://linear.app/livepeer/issue/VID-228/streameth-recording-uploaded-assets-returns-bad-quality
			track: InputTrack{
				Type:    "video",
				Bitrate: 1_100_000,
				VideoTrack: VideoTrack{
					Width:  1920,
					Height: 1080,
				},
			},
			want: []EncodedProfile{
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 122_222},
				{Name: "1080p0", Width: 1920, Height: 1080, Bitrate: 1_100_000},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetDefaultPlaybackProfiles(tt.track)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCheckUpdatedAlgo(t *testing.T) {
	type ProfilesTest struct {
		Width          int64
		Height         int64
		Bitrate        int64
		ExpectedOutput []EncodedProfile
		CurrentOutput  []EncodedProfile
	}
	dir := "./fixtures/profiles_tests"
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(dir, file.Name()))
		require.NoError(t, err)
		var testCase ProfilesTest
		err = json.Unmarshal(contents, &testCase)
		require.NoError(t, err)
		t.Run(file.Name(), func(t *testing.T) {
			require.NoError(t, err)
			iv := InputVideo{
				Tracks: []InputTrack{{
					Type:    "video",
					Bitrate: testCase.Bitrate,
					VideoTrack: VideoTrack{
						Width:  testCase.Width,
						Height: testCase.Height,
					},
				}},
			}
			oldAlgorithmOutput := testCase.ExpectedOutput
			vt, err := iv.GetTrack(TrackTypeVideo)
			require.NoError(t, err)
			current, err := GetDefaultPlaybackProfiles(vt)
			require.NoError(t, err)
			require.Equal(t, testCase.CurrentOutput, current)
			require.Equal(t, len(oldAlgorithmOutput), len(current))
			for i, profile := range oldAlgorithmOutput {
				currentProfile := current[i]
				// check that they're equal other than the new bitrates being lower
				require.LessOrEqual(t, currentProfile.Bitrate, profile.Bitrate)
				profile.Bitrate = currentProfile.Bitrate
				require.Equal(t, profile, currentProfile)
			}
		})
	}
}

func TestPopulateOutput(t *testing.T) {
	out, err := PopulateOutput("requestID", Probe{}, "fixtures/bbb-180rotated.mov", OutputVideoFile{})
	require.NoError(t, err)
	require.Equal(t, OutputVideoFile{
		SizeBytes: 123542,
		Width:     416,
		Height:    240,
		Bitrate:   414661,
	}, out)
}
