package misttriggers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineId(t *testing.T) {
	records := []StreamSample{
		{"catalyst_vod_110442dc-5b7d-4725-a92f-231677ac6167", Segmenting},
		{"bigBucksBunny1080p", Unrelated},
		{"tr_rend_+10a40c88-dcf7-4d77-8ac2-4ef07cb23807", Transcoding},
		{"video2b1e43cd-f0df-4fc9-be6f-8bd91f9758a9", Recording},
	}
	for _, record := range records {
		require.Equal(t, record.expected, streamNameToPipeline(record.streamName), record.streamName)
	}
}

type StreamSample struct {
	streamName string
	expected   PipelineId
}
