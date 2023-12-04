package video

import (
	"github.com/grafov/m3u8"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	rendition = "rendition-1080p0"
	request   = "request-test"
)

const normalManifest = `#EXTM3U
 #EXT-X-VERSION:3
 #EXT-X-PLAYLIST-TYPE:VOD
 #EXT-X-TARGETDURATION:5
 #EXT-X-MEDIA-SEQUENCE:0
 #EXTINF:10.4160000000,
 0.ts
 #EXTINF:5.3340000000,
 1.ts
 #EXTINF:2.3340000000,
 2.ts
 #EXT-X-ENDLIST`

const weirdManifest = `#EXTM3U
 #EXT-X-VERSION:3
 #EXT-X-PLAYLIST-TYPE:VOD
 #EXT-X-TARGETDURATION:5
 #EXT-X-MEDIA-SEQUENCE:0
 #EXTINF:10000.00,
 0.ts
 #EXTINF:15000.00,
 1.ts
 #EXTINF:10000.00,
 2.ts
 #EXT-X-ENDLIST`

func TestItConcatsStreams(t *testing.T) {
	// setup pre-reqs for testing stream concatenation
	concatDir, err := os.MkdirTemp(os.TempDir(), "concat_stage_")
	require.NoError(t, err)
	concatTsFileName := filepath.Join(concatDir, request+"_"+rendition+".ts")
	tr := populateRenditionSegmentList(t, concatDir)
	segmentList := tr.GetSegmentList(rendition)
	// setup a fake playlist
	sourceManifest, _, err := m3u8.DecodeFrom(strings.NewReader(normalManifest), true)
	require.NoError(t, err)
	pl := *sourceManifest.(*m3u8.MediaPlaylist)

	require.NoError(t, err)

	// verify stream-based concatenation
	totalBytesW, err := ConcatTS(concatTsFileName, segmentList, pl, true)
	require.NoError(t, err)
	require.Equal(t, int64(594644), totalBytesW)

}

func TestItConcatsFiles(t *testing.T) {
	// setup pre-reqs for testing stream concatenation
	concatDir, err := os.MkdirTemp(os.TempDir(), "concat_stage_")
	require.NoError(t, err)
	concatTsFileName := filepath.Join(concatDir, request+"_"+rendition+".ts")
	tr := populateRenditionSegmentList(t, concatDir)
	segmentList := tr.GetSegmentList(rendition)
	// setup a fake playlist
	sourceManifest, _, err := m3u8.DecodeFrom(strings.NewReader(normalManifest), true)
	require.NoError(t, err)
	pl := *sourceManifest.(*m3u8.MediaPlaylist)

	require.NoError(t, err)
	// verify file-based concatenation
	totalBytesWritten, err := ConcatTS(concatTsFileName, segmentList, pl, false)
	require.NoError(t, err)
	require.Equal(t, int64(594644), totalBytesWritten)

}

func TestItConcatsFilesOnlyUptoMP4DurationLimit(t *testing.T) {
	// setup pre-reqs for testing stream concatenation
	concatDir, err := os.MkdirTemp(os.TempDir(), "concat_stage_")
	require.NoError(t, err)
	concatTsFileName := filepath.Join(concatDir, request+"_"+rendition+".ts")
	tr := populateRenditionSegmentList(t, concatDir)
	segmentList := tr.GetSegmentList(rendition)
	// setup a fake playlist
	sourceManifest, _, err := m3u8.DecodeFrom(strings.NewReader(weirdManifest), true)
	require.NoError(t, err)
	pl := *sourceManifest.(*m3u8.MediaPlaylist)
	require.NoError(t, err)
	// verify file-based concatenation
	totalBytesW, err := ConcatTS(concatTsFileName, segmentList, pl, false)
	require.NoError(t, err)
	// Only first two segments are written since duration exceeded Mp4DurationLimit
	//206612 seg-0.ts
	//199656 seg-1.ts
	//188376 seg-2.ts
	require.Equal(t, int64(406268), totalBytesW)
}

func TestItConcatsStreamsOnlyUptoMP4DurationLimit(t *testing.T) {
	// setup pre-reqs for testing stream concatenation
	concatDir, err := os.MkdirTemp(os.TempDir(), "concat_stage_")
	require.NoError(t, err)
	concatTsFileName := filepath.Join(concatDir, request+"_"+rendition+".ts")
	tr := populateRenditionSegmentList(t, concatDir)
	segmentList := tr.GetSegmentList(rendition)
	// setup a fake playlist
	sourceManifest, _, err := m3u8.DecodeFrom(strings.NewReader(weirdManifest), true)
	require.NoError(t, err)
	pl := *sourceManifest.(*m3u8.MediaPlaylist)
	require.NoError(t, err)
	// verify stream-based concatenation
	totalBytesW, err := ConcatTS(concatTsFileName, segmentList, pl, true)
	require.NoError(t, err)
	// Only first two segments are written since duration exceeded Mp4DurationLimit
	//206612 seg-0.ts
	//199656 seg-1.ts
	//188376 seg-2.ts
	require.Equal(t, int64(406268), totalBytesW)
}

func populateRenditionSegmentList(t *testing.T, concatDir string) *TRenditionList {
	segmentFiles := []string{"../test/fixtures/seg-0.ts", "../test/fixtures/seg-1.ts", "../test/fixtures/seg-2.ts"}

	renditionList := &TRenditionList{
		RenditionSegmentTable: make(map[string]*TSegmentList),
	}
	segmentList := &TSegmentList{}

	for i, filePath := range segmentFiles {
		data := readSegmentData(filePath)
		segmentList.AddSegment(i)

		segmentFilename := filepath.Join(concatDir, request+"_"+rendition+"_"+strconv.Itoa(i)+".ts")
		segmentFile, err := os.Create(segmentFilename)
		require.NoError(t, err)
		defer segmentFile.Close()
		_, err = segmentFile.Write(data)
		require.NoError(t, err)
	}

	renditionList.AddRenditionSegment(rendition, segmentList)

	return renditionList
}

func readSegmentData(filePath string) []byte {
	data, _ := os.ReadFile(filePath)
	return data
}
