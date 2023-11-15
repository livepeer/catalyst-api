package video

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestItConcatsStreams(t *testing.T) {
	// test pre-reqs
	tr := populateRenditionSegmentList()
	segmentList := tr.GetSegmentList("rendition-1080p0")
	concatDir, err := os.MkdirTemp(os.TempDir(), "concat_stage_")
	require.NoError(t, err)
	// verify file-based concatenation
	totalBytesWritten, err := ConcatTS(concatDir+"test.ts", segmentList, false)
	require.NoError(t, err)
	require.Equal(t, int64(594644), totalBytesWritten)
	// verify stream-based concatenation
	totalBytesW, err := ConcatTS(concatDir+"test.ts", segmentList, true)
	require.NoError(t, err)
	require.Equal(t, int64(594644), totalBytesW)
}

func populateRenditionSegmentList() *TRenditionList {
	segmentFiles := []string{"../test/fixtures/seg-0.ts", "../test/fixtures/seg-1.ts", "../test/fixtures/seg-2.ts"}

	renditionList := &TRenditionList{
		RenditionSegmentTable: make(map[string]*TSegmentList),
	}
	segmentList := &TSegmentList{
		SegmentDataTable: make(map[int][]byte),
	}

	for i, filePath := range segmentFiles {
		data := readSegmentData(filePath)
		segmentList.AddSegmentData(i, data)
	}

	renditionList.AddRenditionSegment("rendition-1080p0", segmentList)

	return renditionList
}

func readSegmentData(filePath string) []byte {
	data, _ := os.ReadFile(filePath)
	return data
}
