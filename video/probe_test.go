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

func TestProbe(t *testing.T) {
	require := require.New(t)
	probe := Probe{}
	iv, err := probe.ProbeFile("../clients/fixtures/mediaconvert_payloads/sample.mp4")
	require.NoError(err)

	expectedInput := InputVideo{
		Format:   "mov,mp4,m4a,3gp,3g2,mj2",
		Duration: 16.254,
		Tracks: []InputTrack{
			{
				Type:    TrackTypeVideo,
				Codec:   "h264",
				Bitrate: 1234521,
				VideoTrack: VideoTrack{
					Width:  576,
					Height: 1024,
					FPS:    30,
				},
			},
			{
				Type:    TrackTypeAudio,
				Codec:   "aac",
				Bitrate: 128248,
				AudioTrack: AudioTrack{
					Channels: 2,
				},
			},
		},
		SizeBytes: 2779520,
	}
	require.Equal(expectedInput, iv)
}

func TestProbe_VideoRotation(t *testing.T) {
	probe := Probe{}
	iv, err := probe.ProbeFile("./fixtures/bbb-180rotated.mov")
	require.NoError(t, err)
	track, err := iv.GetTrack("video")
	require.NoError(t, err)
	require.Equal(t, int64(-180), track.Rotation)
}
