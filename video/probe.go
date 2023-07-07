package video

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/log"
	"gopkg.in/vansante/go-ffprobe.v2"
)

var unsupportedVideoCodecList = []string{"mjpeg", "jpeg", "png"}

type Prober interface {
	ProbeFile(requestID, url string, ffProbeOptions ...string) (InputVideo, error)
}

type Probe struct {
	IgnoreErrMessages []string
}

func (p Probe) ProbeFile(requestID string, url string, ffProbeOptions ...string) (InputVideo, error) {
	iv, err := p.runProbe(url, ffProbeOptions...)
	if err == nil {
		return iv, nil
	}

	// ignore these probing errors if found and re-run with fatal loglevel to obtain the probe data
	errMsg := strings.ToLower(err.Error())
	for _, ignoreMsg := range p.IgnoreErrMessages {
		if strings.Contains(errMsg, ignoreMsg) {
			log.Log(requestID, "ignoring probe error", "err", err)
			return p.runProbe(url, "-loglevel", "fatal")
		}
	}
	return InputVideo{}, err
}

func (p Probe) runProbe(url string, ffProbeOptions ...string) (iv InputVideo, err error) {
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
	backOff.MaxElapsedTime = 0 // don't impose a timeout as part of the retries
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
	for _, codec := range unsupportedVideoCodecList {
		if strings.ToLower(videoStream.CodecName) == codec {
			return InputVideo{}, fmt.Errorf("error checking for video: %s is not supported", videoStream.CodecName)
		}
	}
	if strings.ToLower(videoStream.CodecName) == "vp9" && strings.Contains(probeData.Format.FormatName, "mp4") {
		return InputVideo{}, fmt.Errorf("error checking for video: VP9 in an MP4 container is not supported")
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

	duration, err := strconv.ParseFloat(videoStream.Duration, 64)
	if err != nil {
		duration = probeData.Format.DurationSeconds
	}

	var rotation int64
	displaySideData, err := videoStream.SideDataList.GetSideData("Display Matrix")
	if err == nil {
		r, err := displaySideData.GetInt("rotation")
		if err == nil {
			rotation = r
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
					Width:              int64(videoStream.Width),
					Height:             int64(videoStream.Height),
					FPS:                fps,
					Rotation:           rotation,
					DisplayAspectRatio: videoStream.DisplayAspectRatio,
					PixelFormat:        videoStream.PixFmt,
				},
			},
		},
		Duration:  duration,
		SizeBytes: size,
	}
	iv, err = addAudioTrack(probeData, iv)
	if err != nil {
		return InputVideo{}, err
	}

	return iv, nil
}

func addAudioTrack(probeData *ffprobe.ProbeData, iv InputVideo) (InputVideo, error) {
	audioTrack := probeData.FirstAudioStream()
	if audioTrack == nil {
		return iv, nil
	}

	sampleRate, err := strconv.Atoi(audioTrack.SampleRate)
	if audioTrack.SampleRate != "" && err != nil {
		return iv, fmt.Errorf("error parsing sample rate from track %d: %w", audioTrack.Index, err)
	}
	bitDepth, err := strconv.Atoi(audioTrack.BitsPerRawSample)
	if audioTrack.BitsPerRawSample != "" && err != nil {
		return iv, fmt.Errorf("error parsing bit depth (bits_per_raw_sample) from track %d: %w", audioTrack.Index, err)
	}

	bitrate, _ := strconv.ParseInt(audioTrack.BitRate, 10, 64)
	iv.Tracks = append(iv.Tracks, InputTrack{
		Type:    TrackTypeAudio,
		Codec:   audioTrack.CodecName,
		Bitrate: bitrate,
		AudioTrack: AudioTrack{
			Channels:   audioTrack.Channels,
			SampleBits: audioTrack.BitsPerSample,
			SampleRate: sampleRate,
			BitDepth:   bitDepth,
		},
	})

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
