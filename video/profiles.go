package video

import (
	"fmt"
	"math"
	"strconv"
)

const (
	MinVideoBitrate         = 100_000
	AbsoluteMinVideoBitrate = 5_000
	MaxVideoBitrate         = 288_000_000
	TrackTypeVideo          = "video"
	TrackTypeAudio          = "audio"
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
func (i InputVideo) GetTrack(trackType string) (InputTrack, error) {
	if trackType != TrackTypeVideo && trackType != TrackTypeAudio {
		return InputTrack{}, fmt.Errorf("invalid track type - must be '%s' or '%s'", TrackTypeVideo, TrackTypeAudio)
	}
	for _, t := range i.Tracks {
		if t.Type == trackType {
			return t, nil
		}
	}
	return InputTrack{}, fmt.Errorf("no '%s' tracks found", trackType)
}

type VideoTrack struct {
	Width              int64   `json:"width,omitempty"`
	Height             int64   `json:"height,omitempty"`
	PixelFormat        string  `json:"pixel_format,omitempty"`
	FPS                float64 `json:"fps,omitempty"`
	Rotation           int64   `json:"rotation,omitempty"`
	DisplayAspectRatio string  `json:"display_aspect_ratio,omitempty"`
}

type AudioTrack struct {
	Channels   int `json:"channels,omitempty"`
	SampleRate int `json:"sample_rate,omitempty"`
	SampleBits int `json:"sample_bits,omitempty"`
	BitDepth   int `json:"bit_depth,omitempty"`
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

var DefaultProfile360p = EncodedProfile{
	Name:    "360p0",
	FPS:     0,
	Bitrate: 1_000_000,
	Width:   640,
	Height:  360,
}
var DefaultProfile720p = EncodedProfile{
	Name:    "720p0",
	FPS:     0,
	Bitrate: 4_000_000,
	Width:   1280,
	Height:  720,
}

// DefaultTranscodeProfiles defines the default set of encoding profiles to use when none are specified
var DefaultTranscodeProfiles = []EncodedProfile{DefaultProfile360p, DefaultProfile720p}

func SetTranscodeProfiles(inputVideoStats InputVideo, reqTranscodeProfiles []EncodedProfile) ([]EncodedProfile, error) {

	var transcodeProfiles []EncodedProfile
	videoTrack, err := inputVideoStats.GetTrack(TrackTypeVideo)
	if err != nil {
		return nil, fmt.Errorf("no video track found in input video: %w", err)
	}
	// If Profiles haven't been overridden, use the default set
	if len(reqTranscodeProfiles) == 0 {
		transcodeProfiles, err = GetDefaultPlaybackProfiles(videoTrack)
		if err != nil {
			return nil, fmt.Errorf("failed to get default transcode profiles: %w", err)
		}
		return transcodeProfiles, nil
		// Otherwise, if it's a special case where only the bitrate is set, then we generate
		// a single profile that matches the input video's specs with the target bitrate
	} else if len(reqTranscodeProfiles) == 1 {
		if reqTranscodeProfiles[0].Width == 0 && reqTranscodeProfiles[0].Height == 0 && reqTranscodeProfiles[0].Bitrate != 0 {
			transcodeProfiles = GenerateSingleProfileWithTargetBitrate(videoTrack, reqTranscodeProfiles[0].Bitrate)
			return transcodeProfiles, nil
		}
	}
	return reqTranscodeProfiles, nil
}

func GenerateSingleProfileWithTargetBitrate(videoTrack InputTrack, videoBitrate int64) []EncodedProfile {
	profiles := make([]EncodedProfile, 0)

	profiles = append(profiles, EncodedProfile{
		Name:    strconv.FormatInt(videoTrack.Height, 10) + "p0",
		Bitrate: videoBitrate,
		FPS:     0,
		Width:   videoTrack.Width,
		Height:  videoTrack.Height,
	})
	return profiles
}

func GetDefaultPlaybackProfiles(video InputTrack) ([]EncodedProfile, error) {
	videoBitrate := video.Bitrate
	if videoBitrate > MaxVideoBitrate {
		videoBitrate = MaxVideoBitrate
	}
	profiles := make([]EncodedProfile, 0, len(DefaultTranscodeProfiles)+1)
	for _, profile := range DefaultTranscodeProfiles {
		// transcoding job will adjust the width to match aspect ratio. no need to
		// check it here.
		lowerQualityThanSrc := profile.Height < video.Height && profile.Bitrate < video.Bitrate
		if lowerQualityThanSrc {
			relativeBitrate := float64(profile.Width*profile.Height) * (float64(videoBitrate) / float64(video.Width*video.Height))
			br := math.Min(relativeBitrate, float64(profile.Bitrate))
			profile.Bitrate = int64(br)
			profiles = append(profiles, profile)
		}
	}
	if len(profiles) == 0 {
		profiles = []EncodedProfile{lowBitrateProfile(video)}
	}
	profiles = append(profiles, EncodedProfile{
		Name:    strconv.FormatInt(nearestEven(video.Height), 10) + "p0",
		Bitrate: videoBitrate,
		FPS:     0,
		Width:   nearestEven(video.Width),
		Height:  nearestEven(video.Height),
	})
	return profiles, nil
}

// When the input video's height and bitrate combo is too low to meet the default ABR ladder we output a low bitrate
// profile as well as the profile matching the input video to achieve at least some ABR playback.
// 50% of the input video's bitrate was found to give a decent experience. We also then check that this isn't below
// some sensible minimum values.
func lowBitrateProfile(video InputTrack) EncodedProfile {
	bitrate := int64(float64(video.Bitrate) * (1.0 / 2.0))
	if bitrate < MinVideoBitrate && video.Bitrate > MinVideoBitrate {
		bitrate = MinVideoBitrate
	} else if bitrate < AbsoluteMinVideoBitrate {
		bitrate = AbsoluteMinVideoBitrate
	}
	return EncodedProfile{
		Name:    "low-bitrate",
		FPS:     0,
		Bitrate: bitrate,
		Width:   nearestEven(video.Width),
		Height:  nearestEven(video.Height),
	}
}

func nearestEven(input int64) int64 {
	return input + (input & 1)
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

func PopulateOutput(requestID string, probe Prober, outputURL string, videoFile OutputVideoFile) (OutputVideoFile, error) {
	outputVideoProbe, err := probe.ProbeFile(requestID, outputURL, "-analyzeduration", "15000000")
	if err != nil {
		return OutputVideoFile{}, fmt.Errorf("error probing output file from S3: %w", err)
	}
	videoFile.SizeBytes = outputVideoProbe.SizeBytes
	videoTrack, err := outputVideoProbe.GetTrack(TrackTypeVideo)
	if err != nil {
		return OutputVideoFile{}, fmt.Errorf("no video track found in output video: %w", err)
	}
	videoFile.Height = videoTrack.Height
	videoFile.Width = videoTrack.Width
	videoFile.Bitrate = videoTrack.Bitrate
	return videoFile, nil
}
