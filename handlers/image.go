package handlers

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/grafov/m3u8"
	"github.com/julienschmidt/httprouter"
	caterrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/playback"
	ffmpeg "github.com/u2takey/ffmpeg-go"
)

type ImageHandler struct {
	PublicBucketURLs []*url.URL
}

func NewImageHandler(urls []*url.URL) *ImageHandler {
	return &ImageHandler{
		PublicBucketURLs: urls,
	}
}

func (p *ImageHandler) Handle(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
	timeString := req.URL.Query().Get("time")
	time, err := strconv.ParseFloat(timeString, 64)
	if err != nil {
		log.LogNoRequestID("image API error", "err", err)
		caterrs.WriteHTTPBadRequest(w, "failed to parse time", nil)
		return
	}
	width, err := parseResolution(req, "width", 320)
	if err != nil {
		log.LogNoRequestID("image API error", "err", err)
		caterrs.WriteHTTPBadRequest(w, "failed to parse width", nil)
		return
	}
	height, err := parseResolution(req, "height", 240)
	if err != nil {
		log.LogNoRequestID("image API error", "err", err)
		caterrs.WriteHTTPBadRequest(w, "failed to parse height", nil)
		return
	}

	playbackID := params.ByName("playbackID")
	if playbackID == "" {
		caterrs.WriteHTTPBadRequest(w, "playbackID was empty", nil)
		return
	}

	err = p.handle(w, playbackID, time, width, height)
	if err != nil {
		log.LogNoRequestID("image API error", "err", err)
		switch {
		case errors.Is(err, caterrs.ObjectNotFoundError):
			caterrs.WriteHTTPNotFound(w, "not found", nil)
		default:
			caterrs.WriteHTTPInternalServerError(w, "internal server error", nil)
		}
	}
}

func parseResolution(req *http.Request, key string, defaultVal int64) (int64, error) {
	val := req.URL.Query().Get(key)
	if val == "" {
		return defaultVal, nil
	}
	return strconv.ParseInt(val, 10, 32)
}

func (p *ImageHandler) handle(w http.ResponseWriter, playbackID string, t float64, width int64, height int64) error {
	var (
		err         error
		segmentFile string
		dir         = playbackID
		start       = time.Now()
	)

	// download master playlist
	fileInfoReader, err := playback.OsFetch(p.PublicBucketURLs, dir, "index.m3u8", "")
	if err != nil {
		return fmt.Errorf("failed to read manifest: %w", err)
	}

	manifest, _, err := m3u8.DecodeFrom(fileInfoReader.Body, true)
	if err != nil {
		return fmt.Errorf("failed to decode manifest: %w", err)
	}
	masterPlaylist, ok := manifest.(*m3u8.MasterPlaylist)
	if !ok || masterPlaylist == nil {
		return fmt.Errorf("failed to parse playlist as MasterPlaylist")
	}
	if len(masterPlaylist.Variants) < 1 {
		return fmt.Errorf("no renditions found")
	}

	// download rendition playlist
	renditionUri := masterPlaylist.Variants[0].URI
	fileInfoReader, err = playback.OsFetch(p.PublicBucketURLs, dir, renditionUri, "")
	if err != nil {
		return fmt.Errorf("failed to read manifest: %w", err)
	}
	dir = filepath.Join(dir, filepath.Dir(renditionUri))
	manifest, _, err = m3u8.DecodeFrom(fileInfoReader.Body, true)
	if err != nil {
		return fmt.Errorf("failed to decode manifest: %w", err)
	}
	mediaPlaylist, ok := manifest.(*m3u8.MediaPlaylist)
	if !ok || mediaPlaylist == nil {
		return fmt.Errorf("failed to parse playlist as MediaPlaylist")
	}

	// find the segment required
	currentTime := 0.0
	extractTime := 0.0
	for _, segment := range mediaPlaylist.GetAllSegments() {
		currentTime += segment.Duration
		if currentTime > t {
			segmentFile = segment.URI
			extractTime = t - currentTime + segment.Duration
			break
		}
	}
	if segmentFile == "" {
		return fmt.Errorf("playbackID media not found: %w", caterrs.ObjectNotFoundError)
	}

	tmpDir, err := os.MkdirTemp(os.TempDir(), "image-api-*")
	if err != nil {
		return fmt.Errorf("temp file creation failed: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// download the segment
	fileInfoReader, err = playback.OsFetch(p.PublicBucketURLs, dir, segmentFile, "")
	if err != nil {
		return fmt.Errorf("failed to get media: %w", err)
	}
	segBytes, err := io.ReadAll(fileInfoReader.Body)
	if err != nil {
		return fmt.Errorf("failed to get bytes: %w", err)
	}

	inputFile := path.Join(tmpDir, "in.ts")
	if err = os.WriteFile(inputFile, segBytes, 0644); err != nil {
		return fmt.Errorf("failed to write input file: %w", err)
	}
	outputFile := path.Join(tmpDir, "out.jpg")

	metrics.Metrics.ImageAPIDownloadDurationSec.WithLabelValues().Observe(time.Since(start).Seconds())

	// extract image
	extractStart := time.Now()
	defer metrics.Metrics.ImageAPIExtractDurationSec.WithLabelValues().Observe(time.Since(extractStart).Seconds())

	var ffmpegErr bytes.Buffer
	err = ffmpeg.
		Input(inputFile).
		Output(
			outputFile,
			ffmpeg.KwArgs{
				"ss":      fmt.Sprintf("00:00:%d", int64(extractTime)),
				"vframes": "1",
				"vf":      fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease", width, height),
			},
		).OverWriteOutput().WithErrorOutput(&ffmpegErr).Run()
	if err != nil {
		return fmt.Errorf("ffmpeg failed [%s]: %w", ffmpegErr.String(), err)
	}

	bs, err := os.ReadFile(outputFile)
	if err != nil {
		return err
	}

	w.Header().Set("content-type", "image/jpg")
	w.Header().Set("content-length", strconv.Itoa(len(bs)))
	w.WriteHeader(http.StatusOK)
	count, err := w.Write(bs)
	if err != nil {
		log.LogNoRequestID("image handler failed to write response", "err", err)
	} else {
		log.LogNoRequestID("image handler wrote response", "count", count)
	}
	return nil
}
