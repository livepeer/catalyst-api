package thumbnails

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
	ffmpeg "github.com/u2takey/ffmpeg-go"
)

const resolution = "320:240"

func GenerateThumbs(input *url.URL, output *url.URL, info video.InputVideo) error {
	videoTrack, err := info.GetTrack(video.TrackTypeVideo)
	if err != nil {
		return nil
	}
	if videoTrack.FPS <= 0 {
		return fmt.Errorf("fps was invalid %v", videoTrack.FPS)
	}

	in, err := clients.SignURL(input)
	if err != nil {
		return fmt.Errorf("presigning failed: %w", err)
	}
	tempDir, err := os.MkdirTemp(os.TempDir(), "thumbs-*")
	if err != nil {
		return fmt.Errorf("failed to make temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	err = ffmpeg.
		Input(in, ffmpeg.KwArgs{"skip_frame": "nokey"}). // only extract key frames
		Output(
			path.Join(tempDir, "/keyframes_%d.jpg"),
			ffmpeg.KwArgs{
				"vsync":     "0",
				"frame_pts": "true", // PTS will be used as the filename suffix
				// video filter to resize
				"vf": fmt.Sprintf("scale=%s:force_original_aspect_ratio=decrease", resolution),
			},
		).OverWriteOutput().ErrorToStdOut().Run()
	if err != nil {
		return fmt.Errorf("error running ffmpeg for thumbnails %w", err)
	}

	// generate the webvtt file
	files, err := filepath.Glob(path.Join(tempDir, "keyframes*"))
	if err != nil {
		return fmt.Errorf("listing keyframes files failed: %w", err)
	}
	builder := &bytes.Buffer{}
	_, err = builder.WriteString("WEBVTT\n")
	if err != nil {
		return err
	}
	timestamp := time.Time{}
	outputLocation := output.JoinPath("thumbnails").String()
	for i, file := range files {
		// we skip the first entry because we need to calculate the end timestamp of each frame by looking
		// at the PTS of the next thumbnail, we then refer to the previous file when uploading at the bottom of the loop.
		// e.g. keyframe_0.jpg we know starts at 00:00:00 but we need to check the PTS of the next file to know the end time.
		if i == 0 {
			continue
		}
		// extract the PTS value from the filename to be able to calculate the timestamp of the frame
		filename := path.Base(file)                                        // e.g. keyframes_0.jpg
		withoutExt := strings.TrimSuffix(filename, filepath.Ext(filename)) // e.g. keyframes_0
		parts := strings.Split(withoutExt, "_")
		if len(parts) < 2 {
			return fmt.Errorf("thumbnail filename should contain an underscore %s", filename)
		}
		pts, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return fmt.Errorf("couldn't parse float for thumbnails %w", err)
		}
		seconds := pts / videoTrack.FPS

		const layout = "15:04:05.000"
		start := timestamp.Format(layout)
		timestamp = time.Time{}.Add(time.Duration(seconds) * time.Second)
		end := timestamp.Format(layout)

		previousFile := files[i-1]
		_, err = builder.WriteString(fmt.Sprintf("%s --> %s\n%s\n\n", start, end, path.Base(previousFile)))
		if err != nil {
			return err
		}

		fileReader, err := os.Open(previousFile)
		if err != nil {
			return err
		}
		defer fileReader.Close()
		err = clients.UploadToOSURL(outputLocation, path.Base(previousFile), fileReader, time.Minute)
		if err != nil {
			return fmt.Errorf("failed to upload thumbnail: %w", err)
		}
	}

	err = clients.UploadToOSURLFields(outputLocation, "thumbnails.vtt", builder, time.Minute, &drivers.FileProperties{ContentType: "text/vtt"})
	if err != nil {
		return fmt.Errorf("failed to upload vtt: %w", err)
	}
	return nil
}
