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
	mediaconvert   clients.TranscodeProvider
}

func (mc *mediaconvert) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	inputFile := job.SourceFile
	if !strings.HasPrefix(inputFile, "s3://") {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		content, err := getFile(ctx, inputFile)
		if err != nil {
			return nil, fmt.Errorf("error getting source file: %w", err)
		}

		filename := "input_" + job.RequestID
		err = clients.UploadToOSURL(mc.s3InputBucket.String(), filename, content)
		if err != nil {
			return nil, fmt.Errorf("error uploading source file to S3: %w", err)
		}

		inputFile = mc.s3InputBucket.JoinPath(filename).String()
	}

	if path.Base(job.TargetURL.Path) != "index.m3u8" {
		return nil, fmt.Errorf("target URL must be an `index.m3u8` file")
	}
	// AWS MediaConvert adds the .m3u8 to the end of the output file name
	hlsOutputFile := mc.s3OutputBucket.JoinPath(job.RequestID, "index").String()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()
	err := mc.mediaconvert.Transcode(ctx, clients.TranscodeJobInput{
		InputFile:     inputFile,
		HLSOutputFile: hlsOutputFile,
	})
	if err != nil {
		return nil, fmt.Errorf("mediaconvert error: %w", err)
	}

	mcOutputBaseDir := mc.s3OutputBucket.JoinPath("..")
	ourOutputBaseDir := job.TargetURL.JoinPath("..")
	err = copyDir(mcOutputBaseDir, ourOutputBaseDir)
	if err != nil {
		return nil, fmt.Errorf("error copying output files: %w", err)
	}

	return &HandlerOutput{
		Result: &UploadJobResult{
			InputVideo: clients.InputVideo{
				// TODO: Figure out what to do here. Studio doesn't use these anyway.
			},
			Outputs: []clients.OutputVideo{
				{
					Type:     "manifest",
					Manifest: job.TargetURL.String(),
					Videos:   []clients.OutputVideoFile{
						// TODO: Figure out what to do here. Studio doesn't use these anyway.
					},
				},
			},
		},
	}, nil
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)

	files := make(chan drivers.FileInfo, 10)
	eg.Go(func() error {
		defer close(files)
		page, err := clients.ListOSURL(ctx, source.String())
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
				if err := ctx.Err(); err != nil {
					return err
				}
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
