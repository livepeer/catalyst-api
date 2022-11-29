package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/go-tools/drivers"
	"golang.org/x/sync/errgroup"
)

type mediaconvert struct {
	s3InputBucket  *url.URL
	s3OutputBucket *url.URL
}

func (mc *mediaconvert) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	if !strings.HasPrefix(job.SourceFile, "s3://") {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
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

	if path.Base(job.TargetURL.Path) != "index.m3u8" {
		return nil, fmt.Errorf("target URL must be an `index.m3u8` file")
	}
	originalTargetURL := job.TargetURL
	job.TargetURL = mc.s3OutputBucket.JoinPath(job.RequestID, "index")

	// MediaConvert core pipeline:
	// 1. Create job
	// 2. Poll MediaConvert job status and update the local job status with the
	// corresponding stages / completion ratio
	// 3. Profit

	mcOutputBaseDir := mc.s3OutputBucket.JoinPath("..")
	ourOutputBaseDir := originalTargetURL.JoinPath("..")
	err := copyDir(mcOutputBaseDir, ourOutputBaseDir)
	if err != nil {
		return nil, fmt.Errorf("error copying output files: %w", err)
	}

	return nil, errors.New("stil not implemented")
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

func copyDir(source, dest *url.URL) error {
	// TODO: Create a list OS helper in clients package
	osDriver, err := drivers.ParseOSURL(source.String(), true)
	if err != nil {
		return fmt.Errorf("unexpected error parsing internal driver URL: %w", err)
	}
	os := osDriver.NewSession("")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)
	files := make(chan drivers.FileInfo, 10)
	eg.Go(func() error {
		defer close(files)
		page, err := os.ListFiles(ctx, "", "")
		if err != nil {
			return fmt.Errorf("error listing output files: %w", err)
		}
		for {
			for _, f := range page.Files() {
				select {
				case files <- f:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			if !page.HasNextPage() {
				break
			}
			page, err = page.NextPage()
			if err != nil {
				return fmt.Errorf("error fetching files next page: %w", err)
			}
		}
		return nil
	})

	for i := 0; i < 10; i++ {
		eg.Go(func() error {
			for f := range files {
				err := copyFile(source.JoinPath(f.Name).String(), dest.String(), f.Name)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	return eg.Wait()
}

func copyFile(sourceOsURL, destBaseURL, filename string) error {
	content, err := clients.DownloadOSURL(sourceOsURL)
	if err != nil {
		return fmt.Errorf("error reading mediaconvert output file %q: %w", filename, err)
	}
	defer content.Close()

	err = clients.UploadToOSURL(destBaseURL, filename, content)
	if err != nil {
		return fmt.Errorf("error uploading final output file %q: %w", filename, err)
	}
	return nil
}

// Boilerlplate to implement the Handler interface

func (mc *mediaconvert) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected RECORDING_END trigger on MediaConvert pipeline")
}

func (mc *mediaconvert) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected PUSH_END trigger on MediaConvert pipeline")
}
