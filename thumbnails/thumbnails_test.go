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
seg-100.ts
#EXTINF:10.000000,
seg-101.ts
#EXTINF:10.000000,
seg-102.ts
#EXT-X-ENDLIST
`
	err = os.WriteFile(inputFile, []byte(manifest), 0644)
	require.NoError(t, err)
	// copy segments
	for i := 0; i < 3; i++ {
		b, err := os.ReadFile(path.Join(wd, "..", fmt.Sprintf("test/fixtures/seg-%d.ts", i)))
		require.NoError(t, err)
		err = os.WriteFile(path.Join(outDir, "in", fmt.Sprintf("seg-%d.ts", i+100)), b, 0644)
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
keyframes_0.jpg

00:00:10.000 --> 00:00:20.000
keyframes_1.jpg

00:00:20.000 --> 00:00:30.000
keyframes_2.jpg

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
		require.Equal(t, 853, data.FirstVideoStream().Width)
		require.Equal(t, 480, data.FirstVideoStream().Height)
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
			want:          "keyframes_0.jpg",
		},
		{
			name:          "clip",
			segmentURI:    "clip_1.ts",
			segmentOffset: 0,
			want:          "keyframes_1.jpg",
		},
		{
			name:          "clip",
			segmentURI:    "clip_101.ts",
			segmentOffset: 100,
			want:          "keyframes_1.jpg",
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
