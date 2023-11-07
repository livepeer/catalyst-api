package thumbnails

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"io"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/go-tools/drivers"
	ffmpeg "github.com/u2takey/ffmpeg-go"
)

const resolution = "320:240"

func GenerateThumbs(requestID, input string, output *url.URL) error {
	log.Log(requestID, "generate thumbs start", "input", input)
	inputURL, err := url.Parse(input)
	if err != nil {
		return err
	}
	var rc io.ReadCloser
	err = backoff.Retry(func() error {
		rc, err = clients.GetFile(context.Background(), requestID, input, nil)
		return err
	}, clients.DownloadRetryBackoff())
	if err != nil {
		return fmt.Errorf("error downloading manifest: %w", err)
	}
	manifest, playlistType, err := m3u8.DecodeFrom(rc, true)
	if err != nil {
		return fmt.Errorf("failed to decode manifest: %w", err)
	}

	if playlistType != m3u8.MEDIA {
		return fmt.Errorf("received non-Media manifest, but currently only Media playlists are supported")
	}
	mediaPlaylist, ok := manifest.(*m3u8.MediaPlaylist)
	if !ok || mediaPlaylist == nil {
		return fmt.Errorf("failed to parse playlist as MediaPlaylist")
	}

	tempDir, err := os.MkdirTemp(os.TempDir(), "thumbs-*")
	if err != nil {
		return fmt.Errorf("failed to make temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)
	err = os.Mkdir(path.Join(tempDir, "segs"), 0755)
	if err != nil {
		return fmt.Errorf("failed to make segs dir: %w", err)
	}

	const layout = "15:04:05.000"
	outputLocation := output.JoinPath("thumbnails").String()
	builder := &bytes.Buffer{}
	_, err = builder.WriteString("WEBVTT\n")
	if err != nil {
		return err
	}
	var currentTime time.Time
	for _, segment := range mediaPlaylist.GetAllSegments() {
		thumbOut, err := processSegment(requestID, inputURL, segment, tempDir, outputLocation)
		if err != nil {
			return err
		}

		start := currentTime.Format(layout)
		currentTime = currentTime.Add(time.Duration(segment.Duration) * time.Second)
		end := currentTime.Format(layout)
		_, err = builder.WriteString(fmt.Sprintf("%s --> %s\n%s\n\n", start, end, path.Base(thumbOut)))
		if err != nil {
			return err
		}
	}

	err = clients.UploadToOSURLFields(outputLocation, "thumbnails.vtt", builder, time.Minute, &drivers.FileProperties{ContentType: "text/vtt"})
	if err != nil {
		return fmt.Errorf("failed to upload vtt: %w", err)
	}
	log.Log(requestID, "generate thumbs end", "input", input)
	return nil
}

func processSegment(requestID string, inputURL *url.URL, segment *m3u8.MediaSegment, tempDir string, outputLocation string) (string, error) {
	segURL := inputURL.JoinPath("..", segment.URI)
	var (
		rc  io.ReadCloser
		err error
	)
	err = backoff.Retry(func() error {
		rc, err = clients.GetFile(context.Background(), requestID, segURL.String(), nil)
		return err
	}, clients.DownloadRetryBackoff())
	if err != nil {
		return "", fmt.Errorf("error downloading segment %s: %w", segURL, err)
	}
	bs, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("error reading segment %s: %w", segURL, err)
	}

	tmpSegFile := path.Join(tempDir, "segs", path.Base(segment.URI))
	err = os.WriteFile(tmpSegFile, bs, 0644)
	if err != nil {
		return "", fmt.Errorf("error saving segment %s: %w", segURL, err)
	}

	thumbOut := path.Join(tempDir, fmt.Sprintf("keyframes_%d.jpg", segment.SeqId))
	var ffmpegErr bytes.Buffer
	err = ffmpeg.
		Input(tmpSegFile, ffmpeg.KwArgs{"skip_frame": "nokey"}). // only extract key frames
		Output(
			thumbOut,
			ffmpeg.KwArgs{
				"ss":      "00:00:00",
				"vframes": "1",
				// video filter to resize
				"vf": fmt.Sprintf("scale=%s:force_original_aspect_ratio=decrease", resolution),
			},
		).OverWriteOutput().WithErrorOutput(&ffmpegErr).Run()
	if err != nil {
		return "", fmt.Errorf("error running ffmpeg for thumbnails %s [%s]: %w", segURL, ffmpegErr.String(), err)
	}
	err = os.Remove(tmpSegFile)
	if err != nil {
		return "", fmt.Errorf("failed to remove temp file %s: %w", segURL, err)
	}

	fileReader, err := os.Open(thumbOut)
	if err != nil {
		return "", err
	}
	defer fileReader.Close()
	err = clients.UploadToOSURL(outputLocation, path.Base(thumbOut), fileReader, time.Minute)
	if err != nil {
		return "", fmt.Errorf("failed to upload thumbnail %s: %w", segURL, err)
	}
	return thumbOut, nil
}
