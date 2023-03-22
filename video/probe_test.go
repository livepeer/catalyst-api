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

func TestDefaultBitrate(t *testing.T) {
	iv, err := parseProbeOutput(&ffprobe.ProbeData{
		Streams: []*ffprobe.Stream{
			{
				CodecType: "video",
				BitRate:   "",
			},
		},
		Format: &ffprobe.Format{
			Size: "1",
		},
	})
	require.NoError(t, err)
	track, err := iv.GetTrack(TrackTypeVideo)
	require.NoError(t, err)
	require.Equal(t, DefaultProfile720p.Bitrate, track.Bitrate)
}
