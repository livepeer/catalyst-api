package clients

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/config"
	xerrors "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
)

const MaxCopyFileDuration = 2 * time.Hour
const PresignDuration = 24 * time.Hour

type InputCopier interface {
	CopyInputToS3(requestID string, inputFile *url.URL) (video.InputVideo, string, *url.URL, error)
}

type InputCopy struct {
	S3              S3
	Probe           video.Prober
	SourceOutputUrl string
}

// CopyInputToS3 copies the input video to our S3 transfer bucket and probes the file.
func (s *InputCopy) CopyInputToS3(requestID string, inputFile *url.URL) (inputVideoProbe video.InputVideo, signedURL string, osTransferURL *url.URL, err error) {
	if strings.HasSuffix(inputFile.Host, "storage.googleapis.com") && strings.HasPrefix(inputFile.Path, "/directUpload") {
		log.Log(requestID, "Direct upload detected", "source", inputFile.String())
		signedURL = inputFile.String()
		osTransferURL = inputFile
	} else {
		var (
			size            int64
			sourceOutputUrl *url.URL
		)
		sourceOutputUrl, err = url.Parse(s.SourceOutputUrl)
		if err != nil {
			err = fmt.Errorf("cannot create sourceOutputUrl: %w", err)
			return
		}
		osTransferURL = sourceOutputUrl.JoinPath(requestID, "transfer", path.Base(inputFile.Path))
		log.Log(requestID, "Copying input file to S3", "source", inputFile.String(), "dest", osTransferURL.String())
		size, err = CopyFile(context.Background(), inputFile.String(), osTransferURL.String(), "", requestID)
		if err != nil {
			err = fmt.Errorf("error copying input file to S3: %w", err)
			return
		}
		if size <= 0 {
			err = fmt.Errorf("zero bytes found for source: %s", inputFile)
			return
		}
		log.Log(requestID, "Copied", "bytes", size, "source", inputFile.String(), "dest", osTransferURL.String())

		signedURL, err = SignURL(osTransferURL)
		if err != nil {
			return
		}
	}

	log.Log(requestID, "starting probe", "source", inputFile.String(), "dest", osTransferURL.String())
	inputVideoProbe, err = s.Probe.ProbeFile(signedURL)
	if err != nil {
		log.Log(requestID, "probe failed", "err", err, "source", inputFile.String(), "dest", osTransferURL.String())
		err = fmt.Errorf("error probing MP4 input file from S3: %w", err)
		return
	}
	log.Log(requestID, "probe succeeded", "source", inputFile.String(), "dest", osTransferURL.String())
	videoTrack, err := inputVideoProbe.GetTrack(video.TrackTypeVideo)
	if err != nil {
		err = fmt.Errorf("no video track found in input video: %w", err)
		return
	}
	audioTrack, _ := inputVideoProbe.GetTrack(video.TrackTypeAudio)
	if videoTrack.FPS <= 0 {
		// unsupported, includes things like motion jpegs
		err = fmt.Errorf("invalid framerate: %f", videoTrack.FPS)
		return
	}
	if inputVideoProbe.SizeBytes > config.MaxInputFileSizeBytes {
		err = fmt.Errorf("input file %d bytes was greater than %d bytes", inputVideoProbe.SizeBytes, config.MaxInputFileSizeBytes)
		return
	}
	log.Log(requestID, "probed video track:", "codec", videoTrack.Codec, "bitrate", videoTrack.Bitrate, "duration", videoTrack.DurationSec, "w", videoTrack.Width, "h", videoTrack.Height, "pix-format", videoTrack.PixelFormat, "FPS", videoTrack.FPS)
	log.Log(requestID, "probed audio track", "codec", audioTrack.Codec, "bitrate", audioTrack.Bitrate, "duration", audioTrack.DurationSec, "channels", audioTrack.Channels)
	return
}

func CopyFile(ctx context.Context, sourceURL, destOSBaseURL, filename, requestID string) (writtenBytes int64, err error) {
	err = backoff.Retry(func() error {
		// currently this timeout is only used for http downloads in the getFileHTTP function when it calls http.NewRequestWithContext
		ctx, cancel := context.WithTimeout(ctx, MaxCopyFileDuration)
		defer cancel()

		byteAccWriter := ByteAccumulatorWriter{count: 0}
		defer func() { writtenBytes = byteAccWriter.count }()

		c, err := getFile(ctx, sourceURL, requestID)
		if err != nil {
			return fmt.Errorf("download error: %w", err)
		}
		defer c.Close()

		content := io.TeeReader(c, &byteAccWriter)

		err = UploadToOSURL(destOSBaseURL, filename, content, MaxCopyFileDuration)
		if err != nil {
			log.Log(requestID, "Copy attempt failed", "source", sourceURL, "dest", path.Join(destOSBaseURL, filename), "err", err)
		}
		return err
	}, UploadRetryBackoff())
	return
}

func getFile(ctx context.Context, url, requestID string) (io.ReadCloser, error) {
	_, err := drivers.ParseOSURL(url, true)
	if err == nil {
		return DownloadOSURL(url)
	} else if IsDStorageResource(url) {
		return DownloadDStorageFromGatewayList(url, requestID)
	} else {
		return getFileHTTP(ctx, url)
	}
}

var retryableHttpClient = newRetryableHttpClient()

func newRetryableHttpClient() *http.Client {
	client := retryablehttp.NewClient()
	client.RetryMax = 5                          // Retry a maximum of this+1 times
	client.RetryWaitMin = 200 * time.Millisecond // Wait at least this long between retries
	client.RetryWaitMax = 5 * time.Second        // Wait at most this long between retries (exponential backoff)
	client.HTTPClient = &http.Client{
		// Give up on requests that take more than this long - the file is probably too big for us to process locally if it takes this long
		// or something else has gone wrong and the request is hanging
		Timeout: MaxCopyFileDuration,
	}

	return client.StandardClient()
}

func getFileHTTP(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, xerrors.Unretriable(fmt.Errorf("error creating http request: %w", err))
	}
	resp, err := retryableHttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error on import request: %w", err)
	}
	if resp.StatusCode >= 300 {
		resp.Body.Close()
		err := fmt.Errorf("bad status code from import request: %d %s", resp.StatusCode, resp.Status)
		if resp.StatusCode < 500 {
			err = xerrors.Unretriable(err)
		}
		return nil, err
	}
	return resp.Body, nil
}

type StubInputCopy struct{}

func (s *StubInputCopy) CopyInputToS3(requestID string, inputFile *url.URL) (video.InputVideo, string, *url.URL, error) {
	return video.InputVideo{}, "", &url.URL{}, nil
}
