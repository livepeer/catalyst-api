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
				{Name: "low-bitrate", Width: 640, Height: 360, Bitrate: 500_000, Quality: defaultQuality},
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 1_200_001, Quality: defaultQuality},
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
				{Name: "low-bitrate", Width: 640, Height: 360, Bitrate: 250_000, Quality: defaultQuality},
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 600_000, Quality: defaultQuality},
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
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 1_000_000, Quality: defaultQuality},
				{Name: "720p0", Width: 1280, Height: 720, Bitrate: 4_800_001, Quality: defaultQuality},
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
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 320_000, Quality: defaultQuality},
				{Name: "720p0", Width: 1200, Height: 720, Bitrate: 1_200_001, Quality: defaultQuality},
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
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 666_666, Quality: defaultQuality},
				{Name: "720p0", Width: 1280, Height: 720, Bitrate: 2_666_666, Quality: defaultQuality},
				{Name: "1080p0", Width: 1920, Height: 1080, Bitrate: 6_000_000, Quality: defaultQuality},
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
				{Name: "low-bitrate", Width: 400, Height: 240, Bitrate: 258549, Quality: defaultQuality},
				{Name: "240p0", Width: 400, Height: 240, Bitrate: 620518, Quality: defaultQuality},
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
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 1_000_000, Quality: defaultQuality},
				{Name: "720p0", Width: 1280, Height: 720, Bitrate: 4_000_000, Quality: defaultQuality},
				{Name: "1080p0", Width: 1920, Height: 1080, Bitrate: MaxVideoBitrate, Quality: defaultQuality},
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
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 146_666, Quality: defaultQuality},
				{Name: "1080p0", Width: 1920, Height: 1080, Bitrate: 1_320_000, Quality: defaultQuality},
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
				{Name: "360p0", Width: 640, Height: 360, Bitrate: 146_666, Quality: defaultQuality},
				{Name: "1080p0", Width: 1920, Height: 1080, Bitrate: 1_320_000, Quality: defaultQuality},
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

func TestGetDefaultPlaybackProfilesFixtures(t *testing.T) {
	type ProfilesTest struct {
		Width         int64
		Height        int64
		Bitrate       int64
		CurrentOutput []EncodedProfile
	}
	dir := "./fixtures/profiles_tests"
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fileName := filepath.Join(dir, file.Name())
		contents, err := os.ReadFile(fileName)
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
			vt, err := iv.GetTrack(TrackTypeVideo)
			require.NoError(t, err)
			current, err := GetDefaultPlaybackProfiles(vt)
			require.NoError(t, err)

			if os.Getenv("REGENERATE_FIXTURES") != "" {
				testCase.CurrentOutput = current
				bs, err := json.Marshal(testCase)
				require.NoError(t, err)
				err = os.WriteFile(fileName, bs, 0644)
				require.NoError(t, err)
			}
			require.Equal(t, testCase.CurrentOutput, current)
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
