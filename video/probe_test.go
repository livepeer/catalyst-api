package video

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/vansante/go-ffprobe.v2"
)

func TestItRejectsWhenNoVideoTrackPresent(t *testing.T) {
	_, err := parseProbeOutput(&ffprobe.ProbeData{
		Streams: []*ffprobe.Stream{
			{
				CodecType: "audio",
			},
		},
	})
	require.ErrorContains(t, err, "no video stream found")
}

func TestItRejectsWhenMJPEGVideoTrackPresent(t *testing.T) {
	_, err := parseProbeOutput(&ffprobe.ProbeData{
		Streams: []*ffprobe.Stream{
			{
				CodecType: "video",
				CodecName: "mjpeg",
			},
		},
	})
	require.ErrorContains(t, err, "mjpeg is not supported")

	_, err = parseProbeOutput(&ffprobe.ProbeData{
		Streams: []*ffprobe.Stream{
			{
				CodecType: "video",
				CodecName: "jpeg",
			},
		},
	})
	require.ErrorContains(t, err, "jpeg is not supported")
}

func TestItRejectsWhenFormatMissing(t *testing.T) {
	_, err := parseProbeOutput(&ffprobe.ProbeData{
		Streams: []*ffprobe.Stream{
			{
				CodecType: "video",
			},
		},
	})
	require.ErrorContains(t, err, "format information missing")
}

func TestItRejectsWhenVideoBitrateMissing(t *testing.T) {
	_, err := parseProbeOutput(&ffprobe.ProbeData{
		Streams: []*ffprobe.Stream{
			{
				CodecType: "video",
			},
		},
		Format: &ffprobe.Format{},
	})
	require.ErrorContains(t, err, "bitrate field not found")
}
