package clients

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	xerrors "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
)

const MAX_COPY_FILE_DURATION = 30 * time.Minute
const MaxInputFileSizeBytes = 10 * 1024 * 1024 * 1024 // 10 GiB
const PresignDuration = 10 * time.Minute

var RETRY_BACKOFF = backoff.WithMaxRetries(newExponentialBackOffExecutor(), 5)

type InputCopier interface {
	CopyInputToS3(requestID string, inputFile, osTransferURL *url.URL) (video.InputVideo, string, error)
}

type InputCopy struct {
	S3    S3
	Probe video.Prober
}

// CopyInputToS3 copies the input video to our S3 transfer bucket and probes the file.
func (s *InputCopy) CopyInputToS3(requestID string, inputFile, osTransferURL *url.URL) (inputVideoProbe video.InputVideo, signedURL string, err error) {
	if osTransferURL == nil {
		err = errors.New("osTransferURL was nil")
		return
	}

	log.Log(requestID, "Copying input file to S3", "source", inputFile.String(), "dest", osTransferURL.String())
	size, err := CopyFile(context.Background(), inputFile.String(), osTransferURL.String(), "", requestID)
	if err != nil {
		err = fmt.Errorf("error copying input file to S3: %w", err)
		return
	}
	if size <= 0 {
		err = fmt.Errorf("zero bytes found for source: %s", inputFile)
		return
	}
	log.Log(requestID, "Copied", "bytes", size, "source", inputFile.String(), "dest", osTransferURL.String())

	signedURL, err = signURL(osTransferURL)
	if err != nil {
		return
	}

	log.Log(requestID, "starting probe", "source", inputFile.String(), "dest", osTransferURL.String())
	inputVideoProbe, err = s.Probe.ProbeFile(signedURL)
	if err != nil {
		log.Log(requestID, "probe failed", "err", err, "source", inputFile.String(), "dest", osTransferURL.String())
		err = fmt.Errorf("error probing MP4 input file from S3: %w", err)
		return
	}
	log.Log(requestID, "probe succeeded", "source", inputFile.String(), "dest", osTransferURL.String())
	videoTrack, err := inputVideoProbe.GetVideoTrack()
	if err != nil {
		err = fmt.Errorf("no video track found in input video: %w", err)
		return
	}
	if videoTrack.FPS <= 0 {
		// unsupported, includes things like motion jpegs
		err = fmt.Errorf("invalid framerate: %f", videoTrack.FPS)
		return
	}

	if inputVideoProbe.SizeBytes > MaxInputFileSizeBytes {
		err = fmt.Errorf("input file %d bytes was greater than %d bytes", inputVideoProbe.SizeBytes, MaxInputFileSizeBytes)
		return
	}
	return
}

func signURL(u *url.URL) (string, error) {
	if u.Scheme == "" || u.Scheme == "file" { // not compatible with presigning
		return u.String(), nil
	}
	driver, err := drivers.ParseOSURL(u.String(), true)
	if err != nil {
		return "", fmt.Errorf("failed to parse OS url: %w", err)
	}

	sess := driver.NewSession("")
	signedURL, err := sess.Presign("", PresignDuration)
	if err != nil {
		return "", fmt.Errorf("failed to generate signed url: %w", err)
	}
	return signedURL, nil
}

func CopyFile(ctx context.Context, sourceURL, destOSBaseURL, filename, requestID string) (writtenBytes int64, err error) {
	err = backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()

		byteAccWriter := ByteAccumulatorWriter{count: 0}
		defer func() { writtenBytes = byteAccWriter.count }()

		c, err := getFile(ctx, sourceURL, requestID)
		if err != nil {
			return fmt.Errorf("download error: %w", err)
		}
		defer c.Close()

		content := io.TeeReader(c, &byteAccWriter)

		return UploadToOSURL(destOSBaseURL, filename, content, MAX_COPY_FILE_DURATION)
	}, RETRY_BACKOFF)
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

func (s *StubInputCopy) CopyInputToS3(requestID string, inputFile, osTransferURL *url.URL) (inputVideoProbe video.InputVideo, signedURL string, err error) {
	return video.InputVideo{}, "", nil
}
