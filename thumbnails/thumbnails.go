package thumbnails

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/go-tools/drivers"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"golang.org/x/sync/errgroup"
)

const resolution = "854:480"
const vttFilename = "thumbnails.vtt"
const outputDir = "thumbnails"

func GenerateThumbs(requestID, input string, output *url.URL) error {
	inputURL, err := url.Parse(input)
	if err != nil {
		return err
	}
	// download and parse the manifest
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

	const layout = "15:04:05.000"
	outputLocation := output.JoinPath(outputDir).String()
	builder := &bytes.Buffer{}
	_, err = builder.WriteString("WEBVTT\n")
	if err != nil {
		return err
	}
	var (
		currentTime time.Time
		segments    = mediaPlaylist.GetAllSegments()
		thumbOuts   = make([]string, len(segments))
	)
	// loop through each segment, generate a thumbnail image and upload it to storage
	for i, segment := range segments {
		thumbOut, err := processSegment(inputURL, segment, tempDir, outputLocation)
		if err != nil {
			return err
		}
		thumbOuts[i] = thumbOut

		start := currentTime.Format(layout)
		currentTime = currentTime.Add(time.Duration(segment.Duration) * time.Second)
		end := currentTime.Format(layout)
		_, err = builder.WriteString(fmt.Sprintf("%s --> %s\n%s\n\n", start, end, path.Base(thumbOut)))
		if err != nil {
			return err
		}
	}

	// parallelise the thumb uploads
	uploadGroup, _ := errgroup.WithContext(context.Background())
	uploadGroup.SetLimit(5)
	for _, thumbOut := range thumbOuts {
		thumbOut := thumbOut
		uploadGroup.Go(func() error {
			return backoff.Retry(func() error {
				// upload thumbnail to storage
				fileReader, err := os.Open(thumbOut)
				if err != nil {
					return err
				}
				defer fileReader.Close()
				err = clients.UploadToOSURL(outputLocation, path.Base(thumbOut), fileReader, 2*time.Minute)
				if err != nil {
					return fmt.Errorf("failed to upload thumbnail %s: %w", thumbOut, err)
				}
				return nil
			}, clients.UploadRetryBackoff())
		})
	}
	err = uploadGroup.Wait()
	if err != nil {
		return err
	}

	err = clients.UploadToOSURLFields(outputLocation, vttFilename, builder, time.Minute, &drivers.FileProperties{ContentType: "text/vtt"})
	if err != nil {
		return fmt.Errorf("failed to upload vtt: %w", err)
	}
	return nil
}

func processSegment(inputURL *url.URL, segment *m3u8.MediaSegment, tempDir string, outputLocation string) (string, error) {
	segURL := inputURL.JoinPath("..", segment.URI)
	signed, err := clients.SignURL(segURL)
	if err != nil {
		return "", fmt.Errorf("error signing segment url %s: %w", segURL, err)
	}

	// generate thumbnail
	var ffmpegErr bytes.Buffer
	thumbOut := path.Join(tempDir, fmt.Sprintf("keyframes_%d.jpg", segment.SeqId))
	err = backoff.Retry(func() error {
		ffmpegErr = bytes.Buffer{}
		return ffmpeg.
			Input(signed, ffmpeg.KwArgs{"skip_frame": "nokey"}). // only extract key frames
			Output(
				thumbOut,
				ffmpeg.KwArgs{
					"ss":      "00:00:00",
					"vframes": "1",
					// video filter to resize
					"vf": fmt.Sprintf("scale=%s:force_original_aspect_ratio=decrease", resolution),
				},
			).OverWriteOutput().WithErrorOutput(&ffmpegErr).Run()
	}, clients.DownloadRetryBackoff())
	if err != nil {
		return "", fmt.Errorf("error running ffmpeg for thumbnails %s [%s]: %w", segURL, ffmpegErr.String(), err)
	}

	return thumbOut, nil
}

// Wait a maximum of 5 mins for thumbnails to finish
var vttBackoff = backoff.WithMaxRetries(backoff.NewConstantBackOff(30*time.Second), 10)

func WaitForThumbs(requestID string, output *url.URL) error {
	vtt := output.JoinPath(outputDir, vttFilename).String()
	return backoff.Retry(func() error {
		_, err := clients.GetFile(context.Background(), requestID, vtt, nil)
		return err
	}, vttBackoff)
}
