package clients

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	xerrors "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
)

const MAX_COPY_FILE_DURATION = 30 * time.Minute

type InputCopy struct {
	S3    S3
	Probe video.Prober
}

// CopyInputToS3 copies the input video to our S3 transfer bucket and probes the file.
func (s *InputCopy) CopyInputToS3(args TranscodeJobArgs, s3HTTPTransferURL *url.URL) (TranscodeJobArgs, error) {
	if s3HTTPTransferURL == nil {
		return TranscodeJobArgs{}, errors.New("s3HTTPTransferURL was nil")
	}
	s3URL, err := url.Parse("s3://" + s3HTTPTransferURL.Path)
	if err != nil {
		return TranscodeJobArgs{}, fmt.Errorf("failed to parse s3 url: %w", err)
	}

	log.Log(args.RequestID, "Copying input file to S3", "source", args.InputFile, "dest", s3URL)
	size, err := CopyFile(context.Background(), args.InputFile.String(), s3HTTPTransferURL.String(), "", args.RequestID)
	if err != nil {
		return TranscodeJobArgs{}, fmt.Errorf("error copying input file to S3: %w", err)
	}
	if size <= 0 {
		return TranscodeJobArgs{}, fmt.Errorf("zero bytes found for source: %s", args.InputFile)
	}
	log.Log(args.RequestID, "Copied", "bytes", size, "source", args.InputFile, "dest", s3URL)
	args.CollectSourceSize(size)

	presignedInputFileURL, err := s.S3.PresignS3(s3URL.Host, s3URL.Path)
	if err != nil {
		return TranscodeJobArgs{}, fmt.Errorf("error creating s3 url: %w", err)
	}

	log.Log(args.RequestID, "starting probe", "s3url", s3URL)
	inputVideoProbe, err := s.Probe.ProbeFile(presignedInputFileURL)
	if err != nil {
		log.Log(args.RequestID, "probe failed", "s3url", s3URL, "err", err)
		return TranscodeJobArgs{}, fmt.Errorf("error probing MP4 input file from S3: %w", err)
	}
	log.Log(args.RequestID, "probe succeeded", "s3url", s3URL)
	videoTrack, err := inputVideoProbe.GetVideoTrack()
	if err != nil {
		return TranscodeJobArgs{}, fmt.Errorf("no video track found in input video: %w", err)
	}
	if videoTrack.FPS <= 0 {
		// unsupported, includes things like motion jpegs
		return TranscodeJobArgs{}, fmt.Errorf("invalid framerate: %f", videoTrack.FPS)
	}

	if inputVideoProbe.SizeBytes > maxInputFileSizeBytes {
		return TranscodeJobArgs{}, fmt.Errorf("input file %d bytes was greater than %d bytes", inputVideoProbe.SizeBytes, maxInputFileSizeBytes)
	}
	args.InputFileInfo = inputVideoProbe
	args.InputFile = s3URL
	return args, nil
}

func CopyFile(ctx context.Context, sourceURL, destOSBaseURL, filename, requestID string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	writtenBytes := ByteAccumulatorWriter{count: 0}
	c, err := getFile(ctx, sourceURL, requestID)
	if err != nil {
		return writtenBytes.count, fmt.Errorf("download error: %w", err)
	}
	defer c.Close()

	content := io.TeeReader(c, &writtenBytes)

	err = UploadToOSURL(destOSBaseURL, filename, content, MAX_COPY_FILE_DURATION)
	if err != nil {
		return writtenBytes.count, fmt.Errorf("upload error: %w", err)
	}

	return writtenBytes.count, nil
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
