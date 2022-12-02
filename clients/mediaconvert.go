package clients

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/mediaconvert"
	xerrors "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/go-tools/drivers"
	"golang.org/x/sync/errgroup"
)

const pollDelay = 10 * time.Second

type MediaConvertOptions struct {
	Endpoint, Region, Role       string
	AccessKeyID, AccessKeySecret string
	// Bucket that will be used for direct input/output files from MediaConvert.
	// The actual input/output files will be copied to/from this bucket.
	S3AuxBucket *url.URL
}

type MediaConvert struct {
	opts   MediaConvertOptions
	client *mediaconvert.MediaConvert
}

func NewMediaConvertClient(opts MediaConvertOptions) (TranscodeProvider, error) {
	config := aws.NewConfig().
		WithRegion(opts.Region).
		WithCredentials(credentials.NewStaticCredentials(opts.AccessKeyID, opts.AccessKeySecret, "")).
		WithEndpoint(opts.Endpoint)
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("error creating AWS session: %w", err)
	}

	client := mediaconvert.New(sess)
	return &MediaConvert{opts, client}, nil
}

// This does the whole transcode job, including the moving of the input file to
// S3, creating the AWS job and polling until its completed, and finally copying
// the output from S3 back to the final destination.
//
// It calls the input.ReportProgress function to report the progress of the job
// during the polling loop.
func (mc *MediaConvert) Transcode(ctx context.Context, args TranscodeJobArgs) error {
	if path.Base(args.HLSOutputFile.Path) != "index.m3u8" {
		return fmt.Errorf("target URL must be an `index.m3u8` file, found %s", args.HLSOutputFile)
	}
	targetDir := path.Dir(args.HLSOutputFile.Path)

	s3InputDir := mc.opts.S3AuxBucket.JoinPath("input", targetDir)
	s3OutputDir := mc.opts.S3AuxBucket.JoinPath("output", targetDir)

	mcArgs := args
	if mcArgs.InputFile.Scheme != "s3" {
		mcArgs.InputFile = s3InputDir.JoinPath("video")
		err := copyFile(ctx, args.InputFile.String(), mcArgs.InputFile.String(), "")
		if err != nil {
			return fmt.Errorf("error copying input file to S3: %w", err)
		}
	}

	// AWS MediaConvert adds the .m3u8 to the end of the output file name
	mcArgs.HLSOutputFile = s3OutputDir.JoinPath("index")
	// Input/Output URLs that we send to AWS should not have any creds
	mcArgs.InputFile.User = nil
	mcArgs.HLSOutputFile.User = nil

	if err := mc.coreAwsTranscode(ctx, mcArgs); err != nil {
		return err
	}

	mcOutputBaseDir := mcArgs.HLSOutputFile.JoinPath("..")
	ourOutputBaseDir := args.HLSOutputFile.JoinPath("..")
	err := copyDir(mcOutputBaseDir, ourOutputBaseDir)
	if err != nil {
		return fmt.Errorf("error copying output files: %w", err)
	}
	return nil
}

// This is the function that does the core AWS workflow for transcoding a file.
// It expects args to be directly compatible with AWS (i.e. S3-only files).
func (mc *MediaConvert) coreAwsTranscode(ctx context.Context, args TranscodeJobArgs) error {
	payload := createJobPayload(args.InputFile.String(), args.HLSOutputFile.String(), mc.opts.Role)
	job, err := mc.client.CreateJob(payload)
	if err != nil {
		return fmt.Errorf("error creting mediaconvert job: %w", err)
	}
	jobID := job.Job.Id
	log.AddContext("mediaconvert_job_id", aws.StringValue(jobID))
	log.Log(args.RequestID, "Created MediaConvert job")

	// poll the job until completion or error
	ticker := time.NewTicker(pollDelay)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// don't cancel the job let it finish on AWS
			return ctx.Err()
		case <-ticker.C:
			// continue below
		}
		job, err := mc.client.GetJob(&mediaconvert.GetJobInput{Id: jobID})
		if err != nil {
			return fmt.Errorf("error getting job status: %w", err)
		}

		status := aws.StringValue(job.Job.Status)
		switch status {
		case mediaconvert.JobStatusSubmitted, mediaconvert.JobStatusProgressing:
			progress := float64(aws.Int64Value(job.Job.JobPercentComplete)) / 100
			log.Log(args.RequestID, "Got mediaconvert job progress", "progress", progress, "status")
			if args.ReportProgress != nil {
				args.ReportProgress(progress)
			}
		case mediaconvert.JobStatusComplete:
			log.Log(args.RequestID, "Mediaconvert job completed successfully")
			return nil
		case mediaconvert.JobStatusError:
			errMsg := aws.StringValue(job.Job.ErrorMessage)
			log.Log(args.RequestID, "Mediaconvert job failed", "error", errMsg)
			return fmt.Errorf("job failed: %s", errMsg)
		case mediaconvert.JobStatusCanceled:
			log.Log(args.RequestID, "Mediaconvert job unexpectedly canceled")
			return errors.New("job unexpectedly canceled")
		}
	}
}

func createJobPayload(inputFile, hlsOutputFile, role string) *mediaconvert.CreateJobInput {
	return &mediaconvert.CreateJobInput{
		Settings: &mediaconvert.JobSettings{
			Inputs: []*mediaconvert.Input{
				{
					AudioSelectors: map[string]*mediaconvert.AudioSelector{
						"Audio Selector 1": {
							DefaultSelection: aws.String("DEFAULT"),
						},
					},
					FileInput:      aws.String(inputFile),
					TimecodeSource: aws.String("ZEROBASED"),
					VideoSelector:  &mediaconvert.VideoSelector{},
				},
			},
			OutputGroups: []*mediaconvert.OutputGroup{
				{
					Name: aws.String("Apple HLS"),
					OutputGroupSettings: &mediaconvert.OutputGroupSettings{
						HlsGroupSettings: &mediaconvert.HlsGroupSettings{
							Destination:      aws.String(hlsOutputFile),
							MinSegmentLength: aws.Int64(0),
							SegmentLength:    aws.Int64(10),
						},
						Type: aws.String("HLS_GROUP_SETTINGS"),
					},
					Outputs: []*mediaconvert.Output{
						{
							VideoDescription: &mediaconvert.VideoDescription{
								CodecSettings: &mediaconvert.VideoCodecSettings{
									Codec: aws.String("H_264"),
									H264Settings: &mediaconvert.H264Settings{
										RateControlMode:    aws.String("QVBR"),
										SceneChangeDetect:  aws.String("TRANSITION_DETECTION"),
										QualityTuningLevel: aws.String("MULTI_PASS_HQ"),
										FramerateControl:   aws.String("INITIALIZE_FROM_SOURCE"),
									}}},
							AudioDescriptions: []*mediaconvert.AudioDescription{
								{
									CodecSettings: &mediaconvert.AudioCodecSettings{
										Codec: aws.String("AAC"),
										AacSettings: &mediaconvert.AacSettings{
											Bitrate:    aws.Int64(96000),
											CodingMode: aws.String("CODING_MODE_2_0"),
											SampleRate: aws.Int64(48000),
										},
									},
								},
							},
							OutputSettings: &mediaconvert.OutputSettings{
								HlsSettings: &mediaconvert.HlsSettings{},
							},
							ContainerSettings: &mediaconvert.ContainerSettings{
								Container:    aws.String("M3U8"),
								M3u8Settings: &mediaconvert.M3u8Settings{},
							},
						},
					},
					CustomName: aws.String("hls"),
					AutomatedEncodingSettings: &mediaconvert.AutomatedEncodingSettings{
						AbrSettings: &mediaconvert.AutomatedAbrSettings{
							MaxAbrBitrate: aws.Int64(8000000),
						},
					},
				},
			},
			TimecodeConfig: &mediaconvert.TimecodeConfig{
				Source: aws.String("ZEROBASED"),
			},
		},
		Role: aws.String(role),
		AccelerationSettings: &mediaconvert.AccelerationSettings{
			Mode: aws.String("ENABLED"),
		},
	}
}

func getFile(ctx context.Context, url string) (io.ReadCloser, error) {
	_, err := drivers.ParseOSURL(url, true)
	if err == nil {
		return DownloadOSURL(url)
	} else {
		return getFileHTTP(ctx, url)
	}
}

func getFileHTTP(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, xerrors.Unretriable(fmt.Errorf("error creating http request: %w", err))
	}
	resp, err := http.DefaultClient.Do(req)
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

func copyFile(ctx context.Context, sourceURL, destOSBaseURL, filename string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	content, err := getFile(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("error reading mediaconvert output file %q: %w", filename, err)
	}
	defer content.Close()

	err = UploadToOSURL(destOSBaseURL, filename, content, 1*time.Minute)
	if err != nil {
		return fmt.Errorf("error uploading final output file %q: %w", filename, err)
	}
	return nil
}

func copyDir(source, dest *url.URL) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)

	files := make(chan drivers.FileInfo, 10)
	eg.Go(func() error {
		defer close(files)
		page, err := ListOSURL(ctx, source.String())
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
				err := copyFile(ctx, source.JoinPath(f.Name).String(), dest.String(), f.Name)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	return eg.Wait()
}
