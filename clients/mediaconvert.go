package clients

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/mediaconvert"
	"github.com/hashicorp/go-retryablehttp"
	xerrors "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
	"golang.org/x/sync/errgroup"
)

var pollDelay = 10 * time.Second
var retryableHttpClient = newRetryableHttpClient()

const rateLimitedPollDelay = 15 * time.Second
const maxMP4OutDuration = 2 * time.Minute

// https://docs.aws.amazon.com/mediaconvert/latest/ug/mediaconvert_error_codes.html
var errCodesAcceleration = []int64{
	1041, // Acceleration Settings Error
	1042, // Job Doesn't Require Enough Processing Power for Accelerated Transcoding
	1043, // Secret Undocumented Error. Returned for this error msg: "Your input files aren't compatible with accelerated transcoding for the following reasons: [You can't use accelerated transcoding with input files that have empty edit lists as in this input: [0].] Disable accelerated transcoding and resubmit your job."
	1550, // Acceleration Fault: There is an unexpected error with the accelerated transcoding of this job
}
var ErrJobAcceleration = errors.New("job should not have acceleration")

type ByteAccumulatorWriter struct {
	count int64
}

func (acc *ByteAccumulatorWriter) Write(p []byte) (int, error) {
	acc.count += int64(len(p))
	return 0, nil
}

type MediaConvertOptions struct {
	Endpoint, Region, Role       string
	AccessKeyID, AccessKeySecret string
	// Bucket that will be used for direct input/output files from MediaConvert.
	// The actual input/output files will be copied to/from this bucket.
	//
	// This should be a regular s3:// URL with only the bucket name (and/or sub
	// path) and the OS URL will be created internally from it using the same
	// region and credentials above.
	S3TransferBucket *url.URL
}

type AWSMediaConvertClient interface {
	CreateJob(*mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error)
	GetJob(*mediaconvert.GetJobInput) (*mediaconvert.GetJobOutput, error)
}

type MediaConvert struct {
	role                                  string
	s3TransferBucket, osTransferBucketURL *url.URL
	client                                AWSMediaConvertClient
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
		Path:   path.Join(opts.S3TransferBucket.Host, opts.S3TransferBucket.Path),
	}

	client := mediaconvert.New(sess)
	return &MediaConvert{opts.Role, opts.S3TransferBucket, osTransferBucket, client}, nil
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
	targetDir := getTargetDir(args)

	mcInputRelPath := path.Join("input", targetDir, "video")
	// AWS MediaConvert adds the .m3u8 to the end of the output file name
	mcOutputRelPath := path.Join("output", targetDir, "index")
	var srcInputFile *url.URL

	log.Log(args.RequestID, "Copying input file to S3", "source", args.InputFile, "dest", mc.s3TransferBucket.JoinPath(mcInputRelPath), "filename", mcInputRelPath)
	size, err := copyFile(ctx, args.InputFile.String(), mc.osTransferBucketURL.String(), mcInputRelPath, args.RequestID)
	if err != nil {
		log.Log(args.RequestID, "error copying input file to S3", "bytes", size, "err", fmt.Sprintf("%s", err))
		if args.InputFile.Scheme == "http" || args.InputFile.Scheme == "https" {
			// If copyFile fails (e.g. file server closes connection), then attempt transcoding
			// by directly passing the source URL to MC instead of using the S3 source URL.
			srcInputFile = args.InputFile
		} else {
			return err
		}
	} else {
		log.Log(args.RequestID, "Successfully copied", size, "bytes to S3")
		srcInputFile = mc.s3TransferBucket.JoinPath(mcInputRelPath)
	}

	// temporarily probe input mp4 here...
	f, err := DownloadOSURL(mc.osTransferBucketURL.JoinPath(mcInputRelPath).String())
	if err != nil {
		log.Log(args.RequestID, "error downloading MP4 input file from S3 for probing", "err", fmt.Sprintf("%s", err))
	}
	probe, err := video.ProbeFileFromOS(f)
	if err != nil {
		log.Log(args.RequestID, "error probing MP4 input file from S3", "err", fmt.Sprintf("%s", err))
	}

	bitrate, err := strconv.ParseInt(probe.FirstVideoStream().BitRate, 10, 64)
	if err != nil {
		log.Log(args.RequestID, "error parsing bitrate from probed data", "err", fmt.Sprintf("%s", err))
	}
	fsize, err := strconv.ParseInt(probe.Format.Size, 10, 64)
	if err != nil {
		log.Log(args.RequestID, "error parsing size from probed data", "err", fmt.Sprintf("%s", err))
	}
	frameRate := probe.FirstVideoStream().AvgFrameRate
	parts := strings.Split(frameRate, "/")
	var fps float64
	if len(parts) > 1 {
		x, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			log.Log(args.RequestID, "error parsing fps from probed data", "err", fmt.Sprintf("%s", err))
		}
		y, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			log.Log(args.RequestID, "error parsing fps from probed data", "err", fmt.Sprintf("%s", err))
		}
		fps = x / y
	} else {
		fps, err = strconv.ParseFloat(frameRate, 64)
		if err != nil {
			log.Log(args.RequestID, "error parsing fps from probed data", "err", fmt.Sprintf("%s", err))
		}
	}
	iv := video.InputVideo{
		Tracks: []video.InputTrack{
			{
				Type:      "video",
				Codec:     probe.FirstVideoStream().CodecName,
				Bitrate:   bitrate,
				SizeBytes: fsize,
				VideoTrack: video.VideoTrack{
					Width:  int64(probe.FirstVideoStream().Width),
					Height: int64(probe.FirstVideoStream().Height),
					FPS:    fps,
				},
			},
		},
		Duration: probe.Format.Duration().Seconds(),
	}

	args.CollectSourceSize(size)
	mcArgs := args
	mcArgs.InputFileInfo = iv
	mcArgs.InputFile = srcInputFile
	mcArgs.HLSOutputFile = mc.s3TransferBucket.JoinPath(mcOutputRelPath)

	fmt.Println("mcArgs.....", mcArgs)
	//return err

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
	if err := copyDir(mcOutputBaseDir, ourOutputBaseDir, args); err != nil {
		return fmt.Errorf("error copying output files: %w", err)
	}
	return nil
}

// This is the function that does the core AWS workflow for transcoding a file.
// It expects args to be directly compatible with AWS (i.e. S3-only files).
func (mc *MediaConvert) coreAwsTranscode(ctx context.Context, args TranscodeJobArgs, accelerated bool) (err error) {
	log.Log(args.RequestID, "Creating AWS MediaConvert job", "input", args.InputFile, "output", args.HLSOutputFile, "accelerated", accelerated)

	transcodeProfiles := args.Profiles
	if len(transcodeProfiles) == 0 {
		transcodeProfiles, err = video.GetPlaybackProfiles(args.InputFileInfo)
		if err != nil {
			return fmt.Errorf("failed to get playback profiles: %w", err)
		}
	}

	hlsOut := args.HLSOutputFile.String()
	mp4Out := ""
	if args.InputFileInfo.Duration <= maxMP4OutDuration.Seconds() {
		pathParts := strings.Split(hlsOut, string(filepath.Separator))
		pathParts[len(pathParts)-1] = "static"
		mp4Out = strings.Join(pathParts, string(filepath.Separator))
	}
	payload := createJobPayload(args.InputFile.String(), hlsOut, mp4Out, mc.role, accelerated, transcodeProfiles)
	job, err := mc.client.CreateJob(payload)
	if err != nil {
		return fmt.Errorf("error creting mediaconvert job: %w", err)
	}
	jobID := job.Job.Id
	log.AddContext(args.RequestID, "mediaconvert_job_id", aws.StringValue(jobID))
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
			// If we got rate limited then try again, but start polling on a longer interval
			if _, ok := err.(*mediaconvert.TooManyRequestsException); ok {
				log.Log(args.RequestID, "Received mediaconvert TooManyRequestsException. Switching polling to longer interval")
				ticker.Reset(rateLimitedPollDelay)
				continue
			}

			// For any other errors, fail and let the higher level task retrying logic handle it
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

func createJobPayload(inputFile, hlsOutputFile, mp4OutputFile, role string, accelerated bool, profiles []video.EncodedProfile) *mediaconvert.CreateJobInput {
	var acceleration *mediaconvert.AccelerationSettings
	if accelerated {
		acceleration = &mediaconvert.AccelerationSettings{
			Mode: aws.String(mediaconvert.AccelerationModePreferred),
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
			OutputGroups: outputGroups(hlsOutputFile, mp4OutputFile, profiles),
			TimecodeConfig: &mediaconvert.TimecodeConfig{
				Source: aws.String("ZEROBASED"),
			},
		},
		Role:                 aws.String(role),
		AccelerationSettings: acceleration,
	}
}

func outputGroups(hlsOutputFile, mp4OutputFile string, profiles []video.EncodedProfile) []*mediaconvert.OutputGroup {
	groups := []*mediaconvert.OutputGroup{
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
			Outputs:    outputs("M3U8", profiles),
			CustomName: aws.String("hls"),
		},
	}
	if mp4OutputFile != "" {
		groups = append(groups, &mediaconvert.OutputGroup{
			Name: aws.String("Static MP4 Output"),
			OutputGroupSettings: &mediaconvert.OutputGroupSettings{
				FileGroupSettings: &mediaconvert.FileGroupSettings{
					Destination: aws.String(mp4OutputFile),
					DestinationSettings: &mediaconvert.DestinationSettings{
						S3Settings: &mediaconvert.S3DestinationSettings{},
					},
				},
				Type: aws.String("FILE_GROUP_SETTINGS"),
			},
			Outputs:    outputs("MP4", profiles),
			CustomName: aws.String("mp4"),
		})
	}
	return groups
}

func outputs(container string, profiles []video.EncodedProfile) []*mediaconvert.Output {
	outs := make([]*mediaconvert.Output, 0, len(profiles))
	for _, profile := range profiles {
		outs = append(outs, output(container, profile.Name, profile.Height, profile.Bitrate))
	}
	return outs
}

func output(container, name string, height, maxBitrate int64) *mediaconvert.Output {
	return &mediaconvert.Output{
		VideoDescription: &mediaconvert.VideoDescription{
			Height: aws.Int64(height),
			CodecSettings: &mediaconvert.VideoCodecSettings{
				Codec: aws.String("H_264"),
				H264Settings: &mediaconvert.H264Settings{
					MaxBitrate:         aws.Int64(maxBitrate),
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
		ContainerSettings: &mediaconvert.ContainerSettings{
			Container: aws.String(container),
		},
		NameModifier: aws.String(name),
	}
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

func copyFile(ctx context.Context, sourceURL, destOSBaseURL, filename, requestID string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	writtenBytes := ByteAccumulatorWriter{count: 0}
	c, err := getFile(ctx, sourceURL, requestID)
	if err != nil {
		return writtenBytes.count, fmt.Errorf("download error: %w", err)
	}
	defer c.Close()

	content := io.TeeReader(c, &writtenBytes)

	err = UploadToOSURL(destOSBaseURL, filename, content, 1*time.Minute)
	if err != nil {
		return writtenBytes.count, fmt.Errorf("upload error: %w", err)
	}

	storageDriver, err := drivers.ParseOSURL(destOSBaseURL, true)
	if err != nil {
		return writtenBytes.count, err
	}
	sess := storageDriver.NewSession("")
	info, err := sess.ReadData(context.Background(), filename)
	if err != nil {
		return writtenBytes.count, err
	}

	defer info.Body.Close()
	defer sess.EndSession()

	if err != nil {
		return writtenBytes.count, err
	}

	return writtenBytes.count, nil

}

func copyDir(source, dest *url.URL, args TranscodeJobArgs) error {
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
				_, err := copyFile(ctx, source.JoinPath(file).String(), dest.String(), file, args.RequestID)
				args.CollectTranscodedSegment()
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
func getTargetDir(args TranscodeJobArgs) string {
	var (
		url       = args.HLSOutputFile
		requestID = args.RequestID
	)
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
	} else if url.Scheme == "file" {
		dir = path.Join("/", url.Host, dir)
	}
	return path.Join(dir, requestID)
}

func contains[T comparable](v T, list []T) bool {
	for _, elm := range list {
		if elm == v {
			return true
		}
	}
	return false
}

func newRetryableHttpClient() *http.Client {
	client := retryablehttp.NewClient()
	client.RetryMax = 2                          // Retry a maximum of this+1 times
	client.RetryWaitMin = 200 * time.Millisecond // Wait at least this long between retries
	client.RetryWaitMax = 1 * time.Second        // Wait at most this long between retries (exponential backoff)
	client.HTTPClient = &http.Client{
		// Give up on requests that take more than this long - the file is probably too big for us to process locally if it takes this long
		// or something else has gone wrong and the request is hanging
		Timeout: MAX_COPY_DURATION,
	}

	return client.StandardClient()
}
