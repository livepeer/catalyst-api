package thumbnails

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/vansante/go-ffprobe.v2"
)

func generateThumb(t *testing.T, filename string, out *url.URL) {
	bs, err := os.ReadFile(filename)
	require.NoError(t, err)
	err = GenerateThumb(path.Base(filename), bs, out, 0)
	require.NoError(t, err)
}

func TestGenerateThumbs(t *testing.T) {
	segmentPrefix = append(segmentPrefix, "seg-")
	wd, err := os.Getwd()
	require.NoError(t, err)

	// Test the non-recording flow where GenerateThumb is called by handlers/ffmpeg/ffmpeg.go
	outDir, err := os.MkdirTemp(os.TempDir(), "thumbs*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)
	out, err := url.Parse(outDir)
	require.NoError(t, err)

	generateThumb(t, path.Join(wd, "..", "test/fixtures/seg-0.ts"), out)
	generateThumb(t, path.Join(wd, "..", "test/fixtures/seg-1.ts"), out)
	generateThumb(t, path.Join(wd, "..", "test/fixtures/seg-2.ts"), out)

	testGenerateThumbsRun(t, outDir, path.Join(wd, "..", "test/fixtures/tiny.m3u8"))

	// Test the recording flow
	outDir, err = os.MkdirTemp(os.TempDir(), "thumbs*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)
	out, err = url.Parse(outDir)
	require.NoError(t, err)

	err = GenerateThumbsFromManifest("req ID", path.Join(wd, "..", "test/fixtures/tiny.m3u8"), out)
	require.NoError(t, err)

	testGenerateThumbsRun(t, outDir, path.Join(wd, "..", "test/fixtures/tiny.m3u8"))
}

func TestGenerateThumbsOffset(t *testing.T) {
	// Test manifest with an segment offset i.e. the first segment has index >0

	wd, err := os.Getwd()
	require.NoError(t, err)
	outDir, err := os.MkdirTemp(os.TempDir(), "thumbs*")
	require.NoError(t, err)
	defer os.RemoveAll(outDir)
	out, err := url.Parse(outDir)
	require.NoError(t, err)
	err = os.Mkdir(path.Join(outDir, "in"), 0755)
	require.NoError(t, err)

	// Generate a manifest with an offset
	inputFile := path.Join(outDir, "in", "index.m3u8")
	manifest := `
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-TARGETDURATION:10
#EXTINF:10.000000,
clip_100.ts
#EXTINF:10.000000,
clip_101.ts
#EXTINF:10.000000,
clip_102.ts
#EXT-X-ENDLIST
`
	err = os.WriteFile(inputFile, []byte(manifest), 0644)
	require.NoError(t, err)
	// copy segments
	for i := 0; i < 3; i++ {
		b, err := os.ReadFile(path.Join(wd, "..", fmt.Sprintf("test/fixtures/seg-%d.ts", i)))
		require.NoError(t, err)
		err = os.WriteFile(path.Join(outDir, "in", fmt.Sprintf("clip_%d.ts", i+100)), b, 0644)
		require.NoError(t, err)
	}

	err = GenerateThumbsFromManifest("req ID", inputFile, out)
	require.NoError(t, err)

	testGenerateThumbsRun(t, outDir, inputFile)
}

func testGenerateThumbsRun(t *testing.T, outDir, input string) {
	out, err := url.Parse(outDir)
	require.NoError(t, err)

	err = GenerateThumbsVTT("req ID", input, out)
	require.NoError(t, err)

	expectedVtt := `WEBVTT

00:00:00.000 --> 00:00:10.000
keyframes_0.png

00:00:10.000 --> 00:00:20.000
keyframes_1.png

00:00:20.000 --> 00:00:30.000
keyframes_2.png

`

	vtt, err := os.ReadFile(filepath.Join(outDir, "thumbnails/thumbnails.vtt"))
	require.NoError(t, err)
	require.Equal(t, expectedVtt, string(vtt))

	files, err := filepath.Glob(filepath.Join(outDir, "thumbnails", "*.png"))
	require.NoError(t, err)
	require.Len(t, files, 3)

	for _, file := range files {
		data, err := ffprobe.ProbeURL(context.Background(), file)
		require.NoError(t, err)
		require.Equal(t, "png_pipe", data.Format.FormatName)
		require.NotNil(t, data.FirstVideoStream())
		require.Equal(t, 640, data.FirstVideoStream().Width)
		require.Equal(t, 360, data.FirstVideoStream().Height)
	}
}

func Test_thumbFilename(t *testing.T) {
	tests := []struct {
		name          string
		segmentURI    string
		segmentOffset int64
		want          string
	}{
		{
			name:          "index",
			segmentURI:    "index0.ts",
			segmentOffset: 0,
			want:          "keyframes_0.png",
		},
		{
			name:          "clip",
			segmentURI:    "clip_1.ts",
			segmentOffset: 0,
			want:          "keyframes_1.png",
		},
		{
			name:          "clip",
			segmentURI:    "clip_101.ts",
			segmentOffset: 100,
			want:          "keyframes_1.png",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := thumbFilename(tt.segmentURI, tt.segmentOffset)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_segmentIndex(t *testing.T) {
	tests := []struct {
		name       string
		segmentURI string
		want       int64
	}{
		{
			name:       "normal index",
			segmentURI: "index0.ts",
			want:       0,
		},
		{
			name:       "clip",
			segmentURI: "clip_0.ts",
			want:       0,
		},
		{
			name:       "mediaconvert",
			segmentURI: "index360p0_001.ts",
			want:       1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := segmentIndex(tt.segmentURI)
			require.NoError(t, err)
			if got != tt.want {
				t.Errorf("segmentIndex() got = %v, want %v", got, tt.want)
			}
		})
	}
}
