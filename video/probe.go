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
	frameRate := videoStream.AvgFrameRate
	parts := strings.Split(frameRate, "/")
	var fps float64
	if len(parts) > 1 {
		x, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			return InputVideo{}, fmt.Errorf("error parsing fps numerator from probed data: %w", err)
		}
		y, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return InputVideo{}, fmt.Errorf("error parsing fps denominator from probed data: %w", err)
		}
		fps = x / y
	} else {
		fps, err = strconv.ParseFloat(frameRate, 64)
		if err != nil {
			return InputVideo{}, fmt.Errorf("error parsing fps from probed data: %w", err)
		}
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
