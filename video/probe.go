package video

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"gopkg.in/vansante/go-ffprobe.v2"
)

type Prober interface {
	ProbeFile(url string, ffProbeOptions ...string) (InputVideo, error)
}

type Probe struct{}

func (p Probe) ProbeFile(url string, ffProbeOptions ...string) (iv InputVideo, err error) {
	if len(ffProbeOptions) == 0 {
		ffProbeOptions = []string{"-loglevel", "error"}
	}
	var data *ffprobe.ProbeData
	operation := func() error {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer probeCancel()
		data, err = ffprobe.ProbeURL(probeCtx, url, ffProbeOptions...)
		return err
	}

	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 500 * time.Millisecond
	backOff.MaxInterval = 2 * time.Second
	err = backoff.Retry(operation, backoff.WithMaxRetries(backOff, 3))
	if err != nil {
		return InputVideo{}, fmt.Errorf("error probing: %w", err)
	}
	return parseProbeOutput(data)
}

func parseProbeOutput(probeData *ffprobe.ProbeData) (InputVideo, error) {
	// check for a valid video stream
	videoStream := probeData.FirstVideoStream()
	if videoStream == nil {
		return InputVideo{}, errors.New("error checking for video: no video stream found")
	}
	// check for unsupported video stream(s)
	if strings.ToLower(videoStream.CodecName) == "mjpeg" || strings.ToLower(videoStream.CodecName) == "jpeg" {
		return InputVideo{}, fmt.Errorf("error checking for video: %s is not supported", videoStream.CodecName)
	}
	// We rely on this being present to get required information about the input video, so error out if it isn't
	if probeData.Format == nil {
		return InputVideo{}, fmt.Errorf("error parsing input video: format information missing")
	}
	// parse bitrate
	bitRateValue := videoStream.BitRate
	if bitRateValue == "" {
		bitRateValue = probeData.Format.BitRate
	}
	var (
		bitrate int64
		err     error
	)
	if bitRateValue == "" {
		bitrate = DefaultProfile720p.Bitrate
	} else {
		bitrate, err = strconv.ParseInt(bitRateValue, 10, 64)
		if err != nil {
			return InputVideo{}, fmt.Errorf("error parsing bitrate from probed data: %w", err)
		}
	}
	fileFormat := probeData.Format.FormatName
	if fileFormat == "hls" {
		// correct bitrates cannot be probed for hls manifests, so override with default bitrate
		bitrate = DefaultProfile720p.Bitrate
	}
	// parse filesize
	size, err := strconv.ParseInt(probeData.Format.Size, 10, 64)
	if err != nil {
		return InputVideo{}, fmt.Errorf("error parsing filesize from probed data: %w", err)
	}
	// parse fps
	fps, err := parseFps(videoStream.AvgFrameRate)
	if err != nil {
		return InputVideo{}, fmt.Errorf("error parsing avg fps numerator from probed data: %w", err)
	}
	// if fps is 0, try parsing the RFrameRate in the probed data which can be valid for hls files
	if fps == 0 {
		fps, err = parseFps(videoStream.RFrameRate)
		if err != nil {
			return InputVideo{}, fmt.Errorf("error parsing real fps numerator from probed data: %w", err)
		}
	}

	var rotation float64
	for _, sideData := range videoStream.SideDataList {
		r := getSideData[float64](sideData, "rotation")
		if r != nil {
			rotation = *r
			break
		}
	}

	// format file stats into InputVideo
	iv := InputVideo{
		Format: probeData.Format.FormatName,
		Tracks: []InputTrack{
			{
				Type:    TrackTypeVideo,
				Codec:   videoStream.CodecName,
				Bitrate: bitrate,
				VideoTrack: VideoTrack{
					Width:    int64(videoStream.Width),
					Height:   int64(videoStream.Height),
					FPS:      fps,
					Rotation: rotation,
				},
			},
		},
		Duration:  probeData.Format.Duration().Seconds(),
		SizeBytes: size,
	}
	iv = addAudioTrack(probeData, iv)

	return iv, nil
}

func addAudioTrack(probeData *ffprobe.ProbeData, iv InputVideo) InputVideo {
	audioTrack := probeData.FirstAudioStream()
	if audioTrack == nil {
		return iv
	}
	bitrate, _ := strconv.ParseInt(audioTrack.BitRate, 10, 64)
	iv.Tracks = append(iv.Tracks, InputTrack{
		Type:    TrackTypeAudio,
		Codec:   audioTrack.CodecName,
		Bitrate: bitrate,
		AudioTrack: AudioTrack{
			Channels:   audioTrack.Channels,
			SampleBits: audioTrack.BitsPerSample,
		},
	})

	return iv
}

// function taken from task-runner task/probe.go
func parseFps(framerate string) (float64, error) {
	if framerate == "" {
		return 0, nil
	}
	parts := strings.SplitN(framerate, "/", 2)
	if len(parts) < 2 {
		fps, err := strconv.ParseFloat(framerate, 64)
		if err != nil {
			return 0, fmt.Errorf("error parsing framerate: %w", err)
		}
		return fps, nil
	}
	num, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("error parsing framerate numerator: %w", err)
	}
	den, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("error parsing framerate denominator: %w", err)
	}

	if den == 0 {
		// If numerator and denominator are 0 return 0.0 for the FPS
		// 0/0 can be valid for a video track i.e. mjpeg
		if num == 0 {
			return 0, nil
		}

		// If only denominator is 0 then the framerate is invalid
		return 0, errors.New("invalid framerate denominator 0")
	}

	return float64(num) / float64(den), nil
}

func getSideData[T any](sideData ffprobe.SideData, key string) *T {
	if value, ok := sideData[key]; ok {
		if res, ok := value.(T); ok {
			return &res
		}
	}
	return nil
}
