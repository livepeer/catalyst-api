package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/go-tools/drivers"
)

type mediaconvert struct {
	s3InputBucket  *url.URL
	s3OutputBucket *url.URL
}

func (mc *mediaconvert) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()

	if !strings.HasPrefix(job.SourceFile, "s3://") {
		content, err := getFile(ctx, job.SourceFile)
		if err != nil {
			return nil, fmt.Errorf("error getting source file: %w", err)
		}
		filename := "input_" + job.RequestID
		err = clients.UploadToOSURL(mc.s3InputBucket.String(), filename, content)
		if err != nil {
			return nil, fmt.Errorf("error uploading source file to S3: %w", err)
		}
		job.SourceFile = mc.s3InputBucket.JoinPath(filename).String()
	}

	return nil, errors.New("not implemented")
}

func (m *mediaconvert) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected RECORDING_END trigger on MediaConvert pipeline")
}

func (m *mediaconvert) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected PUSH_END trigger on MediaConvert pipeline")
}

func getFile(ctx context.Context, url string) (io.ReadCloser, error) {
	_, err := drivers.ParseOSURL(url, true)
	if err == nil {
		return clients.DownloadOSURL(url)
	} else {
		return getFileHTTP(ctx, url)
	}
}

func getFileHTTP(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, UnretriableError{fmt.Errorf("error creating http request: %w", err)}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error on import request: %w", err)
	}
	if resp.StatusCode >= 300 {
		resp.Body.Close()
		err := fmt.Errorf("bad status code from import request: %d %s", resp.StatusCode, resp.Status)
		if resp.StatusCode < 500 {
			err = UnretriableError{err}
		}
		return nil, err
	}
	return resp.Body, nil
}
