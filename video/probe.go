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
	ProbeFile(url string) (InputVideo, error)
}

type Probe struct {
}

func (p Probe) ProbeFile(url string) (iv InputVideo, err error) {
	var data *ffprobe.ProbeData
	operation := func() error {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer probeCancel()
		data, err = ffprobe.ProbeURL(probeCtx, url)
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
	// parse bitrate
	videoStream := probeData.FirstVideoStream()
	if videoStream == nil {
		return InputVideo{}, errors.New("error probing mp4 input file from s3: no video stream found")
	}
	bitRateValue := videoStream.BitRate
	if bitRateValue == "" {
		bitRateValue = probeData.Format.BitRate
	}
	bitrate, err := strconv.ParseInt(bitRateValue, 10, 64)
	if err != nil {
		return InputVideo{}, fmt.Errorf("error parsing bitrate from probed data: %w", err)
	}
	// parse filesize
	size, err := strconv.ParseInt(probeData.Format.Size, 10, 64)
	if err != nil {
		return InputVideo{}, fmt.Errorf("error parsing filesize from probed data: %w", err)
	}
	// parse fps
	fps, err := parseFps(videoStream.AvgFrameRate)
	if err != nil {
		return InputVideo{}, fmt.Errorf("error parsing fps numerator from probed data: %w", err)
	}
	// format file stats into InputVideo
	iv := InputVideo{
		Tracks: []InputTrack{
			{
				Type:    "video",
				Codec:   videoStream.CodecName,
				Bitrate: bitrate,
				VideoTrack: VideoTrack{
					Width:  int64(videoStream.Width),
					Height: int64(videoStream.Height),
					FPS:    fps,
				},
			},
		},
		Duration:  probeData.Format.Duration().Seconds(),
		SizeBytes: size,
	}

	return iv, nil
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
