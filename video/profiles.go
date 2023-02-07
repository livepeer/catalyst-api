package video

import (
	"fmt"
	"strconv"
)

const (
	MIN_VIDEO_BITRATE          = 100_000
	ABSOLUTE_MIN_VIDEO_BITRATE = 5_000
)

type InputVideo struct {
	Format    string       `json:"format,omitempty"`
	Tracks    []InputTrack `json:"tracks,omitempty"`
	Duration  float64      `json:"duration,omitempty"`
	SizeBytes int64        `json:"size,omitempty"`
}

// Finds the video track from the list of input video tracks
// If multiple video tracks present, returns the first one
// If no video tracks present, returns an error
func (i InputVideo) GetVideoTrack() (InputTrack, error) {
	for _, t := range i.Tracks {
		if t.Type == "video" {
			return t, nil
		}
	}
	return InputTrack{}, fmt.Errorf("no video tracks found")
}

type VideoTrack struct {
	Width       int64   `json:"width,omitempty"`
	Height      int64   `json:"height,omitempty"`
	PixelFormat string  `json:"pixel_format,omitempty"`
	FPS         float64 `json:"fps,omitempty"`
}

type AudioTrack struct {
	Channels   int `json:"channels,omitempty"`
	SampleRate int `json:"sample_rate,omitempty"`
	SampleBits int `json:"sample_bits,omitempty"`
}

type InputTrack struct {
	Type         string  `json:"type"`
	Codec        string  `json:"codec"`
	Bitrate      int64   `json:"bitrate"`
	DurationSec  float64 `json:"duration"`
	SizeBytes    int64   `json:"size"`
	StartTimeSec float64 `json:"start_time"`

	// Fields only used if this is a Video Track
	VideoTrack

	// Fields only used if this is an Audio Track
	AudioTrack
}

// DefaultTranscodeProfiles defines the default set of encoding profiles to use when none are specified
var DefaultTranscodeProfiles = []EncodedProfile{
	{
		Name:    "360p0",
		FPS:     0,
		Bitrate: 1_000_000,
		Width:   640,
		Height:  360,
	},
	{
		Name:    "720p0",
		FPS:     0,
		Bitrate: 4_000_000,
		Width:   1280,
		Height:  720,
	},
}

func GetPlaybackProfiles(iv InputVideo) ([]EncodedProfile, error) {
	video, err := iv.GetVideoTrack()
	if err != nil {
		return nil, fmt.Errorf("no video track found in input video: %w", err)
	}
	profiles := make([]EncodedProfile, 0, len(DefaultTranscodeProfiles)+1)
	for _, profile := range DefaultTranscodeProfiles {
		// transcoding job will adjust the width to match aspect ratio. no need to
		// check it here.
		lowerQualityThanSrc := profile.Height < video.Height && profile.Bitrate < video.Bitrate
		if lowerQualityThanSrc {
			profiles = append(profiles, profile)
		}
	}
	if len(profiles) == 0 {
		profiles = []EncodedProfile{lowBitrateProfile(video)}
	}
	profiles = append(profiles, EncodedProfile{
		Name:    strconv.FormatInt(video.Height, 10) + "p0",
		Bitrate: video.Bitrate,
		FPS:     0,
		Width:   video.Width,
		Height:  video.Height,
	})
	return profiles, nil
}

func lowBitrateProfile(video InputTrack) EncodedProfile {
	bitrate := int64(float64(video.Bitrate) * (1.0 / 2.0))
	if bitrate < MIN_VIDEO_BITRATE && video.Bitrate > MIN_VIDEO_BITRATE {
		bitrate = MIN_VIDEO_BITRATE
	} else if bitrate < ABSOLUTE_MIN_VIDEO_BITRATE {
		bitrate = ABSOLUTE_MIN_VIDEO_BITRATE
	}
	return EncodedProfile{
		Name:    "low-bitrate",
		FPS:     0,
		Bitrate: bitrate,
		Width:   video.Width,
		Height:  video.Height,
	}
}

type EncodedProfile struct {
	Name         string `json:"name,omitempty"`
	Width        int64  `json:"width,omitempty"`
	Height       int64  `json:"height,omitempty"`
	Bitrate      int64  `json:"bitrate,omitempty"`
	FPS          int64  `json:"fps"`
	FPSDen       int64  `json:"fpsDen,omitempty"`
	Profile      string `json:"profile,omitempty"`
	GOP          string `json:"gop,omitempty"`
	Encoder      string `json:"encoder,omitempty"`
	ColorDepth   int64  `json:"colorDepth,omitempty"`
	ChromaFormat int64  `json:"chromaFormat,omitempty"`
}

type OutputVideo struct {
	Type       string            `json:"type"`
	Manifest   string            `json:"manifest,omitempty"`
	Videos     []OutputVideoFile `json:"videos"`
	MP4Outputs []OutputVideoFile `json:"mp4_outputs,omitempty"`
}

type OutputVideoFile struct {
	Type      string `json:"type"`
	SizeBytes int64  `json:"size,omitempty"`
	Location  string `json:"location"`
	Width     int64  `json:"width,omitempty"`
	Height    int64  `json:"height,omitempty"`
	Bitrate   int64  `json:"bitrate,omitempty"`
}
