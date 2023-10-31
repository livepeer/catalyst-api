package thumbnails

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
	"gopkg.in/vansante/go-ffprobe.v2"
)

func TestGenerateThumbs(t *testing.T) {
	u, err := url.Parse("../test/fixtures/tiny.mp4")
	require.NoError(t, err)

	outDir, err := os.MkdirTemp(os.TempDir(), "thumbs*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)

	out, err := url.Parse(outDir)
	require.NoError(t, err)
	err = GenerateThumbs(u, out, video.InputVideo{
		Tracks: []video.InputTrack{{
			Type:       video.TrackTypeVideo,
			VideoTrack: video.VideoTrack{FPS: 30},
		}},
	})
	require.NoError(t, err)

	expectedVtt := `WEBVTT
00:00:00.000 --> 00:00:10.000
keyframes_0.jpg

00:00:10.000 --> 00:00:20.000
keyframes_300.jpg

00:00:20.000 --> 00:00:30.000
keyframes_600.jpg

`

	vtt, err := os.ReadFile(filepath.Join(outDir, "thumbnails/thumbnails.vtt"))
	require.NoError(t, err)
	require.Equal(t, expectedVtt, string(vtt))

	files, err := filepath.Glob(filepath.Join(outDir, "thumbnails", "*.jpg"))
	require.NoError(t, err)
	require.Len(t, files, 3)

	for _, file := range files {
		data, err := ffprobe.ProbeURL(context.Background(), file)
		require.NoError(t, err)
		require.Equal(t, "image2", data.Format.FormatName)
		require.NotNil(t, data.FirstVideoStream())
		require.Equal(t, 320, data.FirstVideoStream().Width)
		require.Equal(t, 180, data.FirstVideoStream().Height)
	}
}
