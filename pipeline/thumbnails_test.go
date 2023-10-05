package pipeline

import (
	"context"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/vansante/go-ffprobe.v2"
)

func TestGenerateThumbs(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "tiny",
			input: "../test/fixtures/tiny.mp4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.input)
			require.NoError(t, err)

			outDir, err := os.MkdirTemp(os.TempDir(), "thumbs*")
			require.NoError(t, err)
			defer os.RemoveAll(outDir)
			log.Println(outDir)

			out, err := url.Parse(outDir)
			require.NoError(t, err)
			err = GenerateThumbs(u, out)
			require.NoError(t, err)

			expectedVtt := `WEBVTT
00:00:00.000 --> 00:00:05.000
keyframes_001.jpg

00:00:05.000 --> 00:00:10.000
keyframes_002.jpg

00:00:10.000 --> 00:00:15.000
keyframes_003.jpg

00:00:15.000 --> 00:00:20.000
keyframes_004.jpg

00:00:20.000 --> 00:00:25.000
keyframes_005.jpg

00:00:25.000 --> 00:00:30.000
keyframes_006.jpg

`

			vtt, err := os.ReadFile(filepath.Join(outDir, "thumbnails/thumbnails.vtt"))
			require.NoError(t, err)
			require.Equal(t, expectedVtt, string(vtt))

			files, err := filepath.Glob(filepath.Join(outDir, "thumbnails", "*.jpg"))
			require.NoError(t, err)
			require.Len(t, files, 6)

			for _, file := range files {
				data, err := ffprobe.ProbeURL(context.Background(), file)
				require.NoError(t, err)
				require.Equal(t, "image2", data.Format.FormatName)
				require.NotNil(t, data.FirstVideoStream())
				require.Equal(t, 480, data.FirstVideoStream().Width)
				require.Equal(t, 270, data.FirstVideoStream().Height)
			}
		})
	}
}
