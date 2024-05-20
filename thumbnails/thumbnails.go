package thumbnails

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
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

// Wait a maximum of 5 mins for thumbnails to finish
var thumbWaitBackoff = backoff.WithMaxRetries(backoff.NewConstantBackOff(30*time.Second), 10)

func getMediaManifest(requestID string, input string) (*m3u8.MediaPlaylist, error) {
	var (
		rc  io.ReadCloser
		err error
	)
	err = backoff.Retry(func() error {
		rc, err = clients.GetFile(context.Background(), requestID, input, nil)
		return err
	}, clients.DownloadRetryBackoff())
	if err != nil {
		return nil, fmt.Errorf("error downloading manifest: %w", err)
	}
	manifest, playlistType, err := m3u8.DecodeFrom(rc, true)
	if err != nil {
		return nil, fmt.Errorf("failed to decode manifest: %w", err)
	}

	if playlistType != m3u8.MEDIA {
		return nil, fmt.Errorf("received non-Media manifest, but currently only Media playlists are supported")
	}
	mediaPlaylist, ok := manifest.(*m3u8.MediaPlaylist)
	if !ok || mediaPlaylist == nil {
		return nil, fmt.Errorf("failed to parse playlist as MediaPlaylist")
	}
	return mediaPlaylist, nil
}

func getSegmentOffset(mediaPlaylist *m3u8.MediaPlaylist) (int64, error) {
	segments := mediaPlaylist.GetAllSegments()
	if len(segments) < 1 {
		return 0, fmt.Errorf("no segments found for")
	}
	segmentOffset, err := segmentIndex(path.Base(segments[0].URI))
	if err != nil {
		return 0, fmt.Errorf("failed to get segment index: %w", err)
	}
	return segmentOffset, nil
}

func GenerateThumbsVTT(requestID string, input string, output *url.URL) error {
	// download and parse the manifest
	mediaPlaylist, err := getMediaManifest(requestID, input)
	if err != nil {
		return err
	}

	const layout = "15:04:05.000"
	outputLocation := output.JoinPath(outputDir)
	builder := &bytes.Buffer{}
	_, err = builder.WriteString("WEBVTT\n")
	if err != nil {
		return err
	}
	segmentOffset, err := getSegmentOffset(mediaPlaylist)
	if err != nil {
		return err
	}

	var currentTime time.Time
	// loop through each segment, generate a vtt entry for it
	for _, segment := range mediaPlaylist.GetAllSegments() {
		filename, err := thumbFilename(path.Base(segment.URI), segmentOffset)
		if err != nil {
			return err
		}
		// check thumbnail file exists on storage
		err = backoff.Retry(func() error {
			_, err := clients.GetFile(context.Background(), requestID, outputLocation.JoinPath(filename).String(), nil)
			return err
		}, thumbWaitBackoff)
		if err != nil {
			return fmt.Errorf("failed to find thumb %s: %w", filename, err)
		}

		start := currentTime.Format(layout)
		currentTime = currentTime.Add(time.Duration(segment.Duration) * time.Second)
		end := currentTime.Format(layout)
		_, err = builder.WriteString(fmt.Sprintf("%s --> %s\n%s\n\n", start, end, filename))
		if err != nil {
			return err
		}
	}

	// upload VTT file
	vttContent := builder.Bytes()
	err = backoff.Retry(func() error {
		return clients.UploadToOSURLFields(outputLocation.String(), vttFilename, bytes.NewReader(vttContent), time.Minute, &drivers.FileProperties{ContentType: "text/vtt"})
	}, clients.UploadRetryBackoff())
	if err != nil {
		return fmt.Errorf("failed to upload vtt: %w", err)
	}
	return nil
}

func GenerateThumb(segmentURI string, input []byte, output *url.URL, segmentOffset int64) error {
	tempDir, err := os.MkdirTemp(os.TempDir(), "thumbs-*")
	if err != nil {
		return fmt.Errorf("failed to make temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)
	outputLocation := output.JoinPath(outputDir)

	inFilename := filepath.Join(tempDir, segmentURI)
	if err := os.WriteFile(inFilename, input, 0644); err != nil {
		return err
	}

	filename, err := thumbFilename(segmentURI, segmentOffset)
	if err != nil {
		return err
	}

	thumbOut := path.Join(tempDir, filename)
	if err := processSegment(inFilename, thumbOut); err != nil {
		return err
	}

	err = backoff.Retry(func() error {
		// upload thumbnail to storage
		fileReader, err := os.Open(thumbOut)
		if err != nil {
			return err
		}
		defer fileReader.Close()
		err = clients.UploadToOSURL(outputLocation.String(), path.Base(thumbOut), fileReader, 2*time.Minute)
		if err != nil {
			return fmt.Errorf("failed to upload thumbnail %s: %w", thumbOut, err)
		}
		return nil
	}, clients.UploadRetryBackoff())
	if err != nil {
		return err
	}

	return nil
}

func GenerateThumbsFromManifest(requestID, input string, output *url.URL) error {
	// parse manifest and generate one thumbnail per segment
	mediaPlaylist, err := getMediaManifest(requestID, input)
	if err != nil {
		return err
	}
	inputURL, err := url.Parse(input)
	if err != nil {
		return err
	}
	segmentOffset, err := getSegmentOffset(mediaPlaylist)
	if err != nil {
		return err
	}

	// parallelise the thumb uploads
	uploadGroup, _ := errgroup.WithContext(context.Background())
	uploadGroup.SetLimit(5)
	for _, segment := range mediaPlaylist.GetAllSegments() {
		segment := segment
		uploadGroup.Go(func() error {
			segURL := inputURL.JoinPath("..", segment.URI)
			var (
				rc  io.ReadCloser
				err error
			)
			// save the segment to memory
			err = backoff.Retry(func() error {
				rc, err = clients.GetFile(context.Background(), requestID, segURL.String(), nil)
				return err
			}, clients.DownloadRetryBackoff())
			if err != nil {
				return fmt.Errorf("error downloading manifest: %w", err)
			}
			bs, err := io.ReadAll(rc)
			if err != nil {
				return err
			}

			// generate thumbnail for the segment
			return GenerateThumb(path.Base(segment.URI), bs, output, segmentOffset)
		})
	}
	return uploadGroup.Wait()
}

func processSegment(input string, thumbOut string) error {
	// generate thumbnail
	var ffmpegErr bytes.Buffer

	err := backoff.Retry(func() error {
		ffmpegErr = bytes.Buffer{}
		return ffmpeg.
			Input(input, ffmpeg.KwArgs{"skip_frame": "nokey"}). // only extract key frames
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
		return fmt.Errorf("error running ffmpeg for thumbnails %s [%s]: %w", input, ffmpegErr.String(), err)
	}

	return nil
}

var segmentPrefix = []string{"index", "clip_"}

func segmentIndex(segmentURI string) (int64, error) {
	// segmentURI will be indexX.ts or clip_X.ts
	for _, prefix := range segmentPrefix {
		segmentURI = strings.TrimPrefix(segmentURI, prefix)
	}
	index := strings.TrimSuffix(segmentURI, ".ts")
	i, err := strconv.ParseInt(index, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("thumbFilename failed for %s: %w", segmentURI, err)
	}
	return i, nil
}

func thumbFilename(segmentURI string, segmentOffset int64) (string, error) {
	i, err := segmentIndex(segmentURI)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("keyframes_%d.jpg", i-segmentOffset), nil
}
