package video

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

const (
	rendition = "rendition-1080p0"
	request   = "request-test"
)

func TestItConcatsStreams(t *testing.T) {
	// setup pre-reqs for testing stream concatenation
	tr := populateRenditionSegmentList()
	segmentList := tr.GetSegmentList(rendition)
	concatDir, err := os.MkdirTemp(os.TempDir(), "concat_stage_")
	require.NoError(t, err)
	concatTsFileName := filepath.Join(concatDir, request+"_"+rendition+".ts")
	// setup a fake struct to simulate what will be sent in the channel
	sb := []TranscodedSegmentInfo{
		{
			RequestID:     request,
			RenditionName: rendition,
			SegmentIndex:  0,
		},
		{
			RequestID:     request,
			RenditionName: rendition,
			SegmentIndex:  1,
		},
		{
			RequestID:     request,
			RenditionName: rendition,
			SegmentIndex:  2,
		},
	}

	// verify file-based concatenation
	totalBytesWritten, err := ConcatTS(concatDir+"test.ts", segmentList, false)
	require.NoError(t, err)
	require.Equal(t, int64(594644), totalBytesWritten)

	// write segments to disk to test stream-based concatenation
	err = WriteSegmentsToDisk(concatDir, tr, sb)
	require.NoError(t, err)
	// verify segments are not held in memory anymore
	for _, v := range segmentList.SegmentDataTable {
		require.Equal(t, int(0), len(v))
	}
	// verify stream-based concatenation
	totalBytesW, err := ConcatTS(concatTsFileName, segmentList, true)
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

	renditionList.AddRenditionSegment(rendition, segmentList)

	return renditionList
}

func readSegmentData(filePath string) []byte {
	data, _ := os.ReadFile(filePath)
	return data
}
