package video

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/vansante/go-ffprobe.v2"
)

type Prober interface {
	ProbeFile(url string) (InputVideo, error)
}

type Probe struct {
}

func (p Probe) ProbeFile(url string) (InputVideo, error) {
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer probeCancel()
	data, err := ffprobe.ProbeURL(probeCtx, url)
	if err != nil {
		return InputVideo{}, fmt.Errorf("error probing: %w", err)
	}
	return parseProbeOutput(data)
}

func parseProbeOutput(probeData *ffprobe.ProbeData) (InputVideo, error) {
	// parse bitrate
	bitRateValue := probeData.FirstVideoStream().BitRate
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
	frameRate := probeData.FirstVideoStream().AvgFrameRate
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
				Type:      "video",
				Codec:     probeData.FirstVideoStream().CodecName,
				Bitrate:   bitrate,
				SizeBytes: size,
				VideoTrack: VideoTrack{
					Width:  int64(probeData.FirstVideoStream().Width),
					Height: int64(probeData.FirstVideoStream().Height),
					FPS:    fps,
				},
			},
		},
		Duration: probeData.Format.Duration().Seconds(),
	}

	return iv, nil
}
