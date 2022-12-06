package clients

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

// https://docs.aws.amazon.com/mediaconvert/latest/ug/mediaconvert_error_codes.html
var errCodesAcceleration = []int64{
	1041, // Acceleration Settings Error
	1042, // Job Doesn't Require Enough Processing Power for Accelerated Transcoding
	1043, // Secret Undocumented Error. Returned for this error msg: "Your input files aren't compatible with accelerated transcoding for the following reasons: [You can't use accelerated transcoding with input files that have empty edit lists as in this input: [0].] Disable accelerated transcoding and resubmit your job."
}
var ErrJobAcceleration = errors.New("job should not have acceleration")

type MediaConvertOptions struct {
	Endpoint, Region, Role       string
	AccessKeyID, AccessKeySecret string
	// Bucket that will be used for direct input/output files from MediaConvert.
	// The actual input/output files will be copied to/from this bucket.
	//
	// This should be a regular s3:// URL with only the bucket name (and/or sub
	// path) and the OS URL will be created internally from it using the same
	// region and credentials above.
	S3AuxBucket *url.URL
}

type MediaConvert struct {
	opts                MediaConvertOptions
	client              *mediaconvert.MediaConvert
	osTransferBucketURL *url.URL
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
	osTransferBucket := &url.URL{
		Scheme: "s3",
		User:   url.UserPassword(opts.AccessKeyID, opts.AccessKeySecret),
		Host:   opts.Region, // weird but compatible with drivers.ParseOSURL
		Path:   path.Join(opts.S3AuxBucket.Host, opts.S3AuxBucket.Path),
	}

	client := mediaconvert.New(sess)
	return &MediaConvert{opts, client, osTransferBucket}, nil
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
	targetDir := getTargetDir(args.HLSOutputFile)

	mcInputRelPath := path.Join("input", targetDir, "video")
	// AWS MediaConvert adds the .m3u8 to the end of the output file name
	mcOutputRelPath := path.Join("output", targetDir, "index")

	log.Log(args.RequestID, "Copying input file to S3", "source", args.InputFile, "dest", mc.opts.S3AuxBucket.JoinPath(mcInputRelPath))
	err := copyFile(ctx, args.InputFile.String(), mc.osTransferBucketURL.String(), mcInputRelPath)
	if err != nil {
		return fmt.Errorf("error copying input file to S3: %w", err)
	}

	mcArgs := args
	mcArgs.InputFile = mc.opts.S3AuxBucket.JoinPath(mcInputRelPath)
	mcArgs.HLSOutputFile = mc.opts.S3AuxBucket.JoinPath(mcOutputRelPath)
	err = mc.coreAwsTranscode(ctx, mcArgs, true)
	if err == ErrJobAcceleration {
		err = mc.coreAwsTranscode(ctx, mcArgs, false)
	}
	if err != nil {
		return err
	}

	mcOutputBaseDir := mc.osTransferBucketURL.JoinPath(mcOutputRelPath, "..")
	ourOutputBaseDir := args.HLSOutputFile.JoinPath("..")
	log.Log(args.RequestID, "Copying output files from S3", "source", mcOutputBaseDir, ourOutputBaseDir)
	if err := copyDir(mcOutputBaseDir, ourOutputBaseDir); err != nil {
		return fmt.Errorf("error copying output files: %w", err)
	}
	return nil
}

// This is the function that does the core AWS workflow for transcoding a file.
// It expects args to be directly compatible with AWS (i.e. S3-only files).
func (mc *MediaConvert) coreAwsTranscode(ctx context.Context, args TranscodeJobArgs, accelerated bool) error {
	log.Log(args.RequestID, "Creating AWS MediaConvert job", "input", args.InputFile, "output", args.HLSOutputFile, "accelerated", accelerated)
	payload := createJobPayload(args.InputFile.String(), args.HLSOutputFile.String(), mc.opts.Role, accelerated)
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
			args.ReportProgress(1)
			log.Log(args.RequestID, "Mediaconvert job completed successfully")
			return nil
		case mediaconvert.JobStatusError:
			errMsg := aws.StringValue(job.Job.ErrorMessage)
			code := aws.Int64Value(job.Job.ErrorCode)
			log.Log(args.RequestID, "Mediaconvert job failed", "error", errMsg, "code", code)
			if contains(code, errCodesAcceleration) {
				return ErrJobAcceleration
			}
			return fmt.Errorf("job failed: %s", errMsg)
		case mediaconvert.JobStatusCanceled:
			log.Log(args.RequestID, "Mediaconvert job unexpectedly canceled")
			return errors.New("job unexpectedly canceled")
		}
	}
}

func createJobPayload(inputFile, hlsOutputFile, role string, accelerated bool) *mediaconvert.CreateJobInput {
	var acceleration *mediaconvert.AccelerationSettings
	if accelerated {
		acceleration = &mediaconvert.AccelerationSettings{
			Mode: aws.String("ENABLED"),
		}
	}

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
					VideoSelector: &mediaconvert.VideoSelector{
						Rotate: aws.String(mediaconvert.InputRotateAuto),
					},
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
							MaxRenditions: aws.Int64(3),
						},
					},
				},
			},
			TimecodeConfig: &mediaconvert.TimecodeConfig{
				Source: aws.String("ZEROBASED"),
			},
		},
		Role:                 aws.String(role),
		AccelerationSettings: acceleration,
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
		return fmt.Errorf("download error: %w", err)
	}
	defer content.Close()

	err = UploadToOSURL(destOSBaseURL, filename, content, 1*time.Minute)
	if err != nil {
		return fmt.Errorf("upload error: %w", err)
	}
	return nil
}

func copyDir(source, dest *url.URL) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)

	files := make(chan string, 10)
	eg.Go(func() error {
		defer close(files)
		page, err := ListOSURL(ctx, source.String())
		if err != nil {
			return fmt.Errorf("error listing files: %w", err)
		}
		for {
			for _, f := range page.Files() {
				select {
				case files <- trimBaseDir(source.String(), f.Name):
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			if !page.HasNextPage() {
				break
			}
			page, err = page.NextPage()
			if err != nil {
				return fmt.Errorf("error fetching next page: %w", err)
			}
		}
		return nil
	})

	for i := 0; i < 10; i++ {
		eg.Go(func() error {
			for file := range files {
				if err := ctx.Err(); err != nil {
					return err
				}
				err := copyFile(ctx, source.JoinPath(file).String(), dest.String(), file)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	return eg.Wait()
}

// The List function from object stores return the full path of the files
// instead of the path relative to the current client prefix (which comes in the
// URL path after the bucket).
//
// This function will remove the base path from the file path returned by the
// OS, or in other words it transforms the absolute path of the file into a
// relative path based on the provided OS path.
func trimBaseDir(osPath, filePath string) string {
	filePath = path.Clean(filePath)
	// We can't just TrimPrefix in this case because there can be other stuff in
	// the OS path before the actual base dir. So we look for the prefix of the
	// file path which is a suffix of the OS path (the "base dir")
	baseDir := filePath
	for !strings.HasSuffix(osPath, baseDir) {
		baseDir = path.Dir(baseDir)
		if baseDir == "/" || baseDir == "." || baseDir == "" {
			return filePath
		}
	}
	return strings.TrimPrefix(filePath, baseDir)
}

// Returns the directory where the files will be stored given an OS URL
func getTargetDir(url *url.URL) string {
	// remove the file name
	dir := path.Dir(url.Path)
	if url.Scheme == "s3" || strings.HasPrefix(url.Scheme, "s3+") {
		dir = strings.TrimLeft(dir, "/")
		split := strings.SplitN(dir, "/", 2)
		if len(split) == 2 {
			dir = split[1]
		} else {
			// only bucket name and file in URL
			dir = ""
		}
	}
	return dir
}

func contains[T comparable](v T, list []T) bool {
	for _, elm := range list {
		if elm == v {
			return true
		}
	}
	return false
}
