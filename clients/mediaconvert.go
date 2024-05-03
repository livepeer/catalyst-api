package clients

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/mediaconvert"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/video"
	"golang.org/x/sync/errgroup"
)

const MAX_COPY_DIR_DURATION = 2 * time.Hour

var pollDelay = 10 * time.Second

const (
	rateLimitedPollDelay   = 15 * time.Second
	mp4OutFilePrefix       = "static"
	mediaConvertJobTimeout = time.Hour
	// don't run mediaconvert for inputs longer than this
	maxMediaconvertDuration = 4 * time.Hour
)

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
	CancelJob(input *mediaconvert.CancelJobInput) (*mediaconvert.CancelJobOutput, error)
}

type MediaConvert struct {
	role                                  string
	s3TransferBucket, osTransferBucketURL *url.URL
	client                                AWSMediaConvertClient
	s3                                    S3
	probe                                 video.Prober
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

	s3Config := aws.NewConfig().
		WithRegion(opts.Region).
		WithCredentials(credentials.NewStaticCredentials(opts.AccessKeyID, opts.AccessKeySecret, ""))
	s3Sess, err := session.NewSession(s3Config)
	if err != nil {
		return nil, fmt.Errorf("error creating AWS session: %w", err)
	}

	client := mediaconvert.New(sess)
	s3Client := &S3Client{s3.New(s3Sess)}
	probe := video.Probe{}
	return &MediaConvert{
		role:                opts.Role,
		s3TransferBucket:    opts.S3TransferBucket,
		osTransferBucketURL: osTransferBucket,
		client:              client,
		s3:                  s3Client,
		probe:               probe,
	}, nil
}

// This does the whole transcode job, including the moving of the input file to
// S3, creating the AWS job and polling until its completed, and finally copying
// the output from S3 back to the final destination.
//
// It calls the input.ReportProgress function to report the progress of the job
// during the polling loop.
func (mc *MediaConvert) Transcode(ctx context.Context, args TranscodeJobArgs) (outs []video.OutputVideo, err error) {
	inputDuration := args.InputFileInfo.Duration
	if inputDuration <= 0 || inputDuration > maxMediaconvertDuration.Seconds() {
		return nil, fmt.Errorf("input too long for mediaconvert: %v", inputDuration)
	}

	var (
		mcArgs    = args
		hlsTarget = args.HLSOutputLocation
		mp4Target = args.MP4OutputLocation
	)

	videoTrack, err := mcArgs.InputFileInfo.GetTrack(video.TrackTypeVideo)
	hasVideoTrack := err == nil

	if hasVideoTrack {
		if len(mcArgs.Profiles) == 0 {
			mcArgs.Profiles, err = video.GetDefaultPlaybackProfiles(videoTrack)
			if err != nil {
				return nil, fmt.Errorf("failed to get playback profiles: %w", err)
			}
		} else if len(mcArgs.Profiles) == 1 && mcArgs.Profiles[0].Bitrate != 0 && mcArgs.Profiles[0].Height == 0 && mcArgs.Profiles[0].Width == 0 {
			mcArgs.Profiles[0].Height = videoTrack.Height
		}
	}

	// If we don't have a video track then ignore any profiles that have been passed in
	// and do a default audio transcode
	if !hasVideoTrack {
		mcArgs.Profiles = []video.EncodedProfile{}
	}

	var mcHlsOutputRelPath string
	if hlsTarget != nil {
		// AWS MediaConvert adds the .m3u8 to the end of the output file name
		mcHlsOutputRelPath = path.Join("output", getTargetDir(hlsTarget, args.RequestID, "hls"), "index")
		mcArgs.HLSOutputLocation = mc.s3TransferBucket.JoinPath(mcHlsOutputRelPath)
	}

	var mcMp4OutputRelPath string
	if mp4Target != nil {
		// sets the mp4 path to be the same as HLS except for the suffix being "static"
		// resulting files look something like https://storage.googleapis.com/bucket/25afy0urw3zu2476/static360p0.mp4
		mcMp4OutputRelPath = path.Join("output", getTargetDir(mp4Target, args.RequestID, "mp4"), mp4OutFilePrefix)
		mcArgs.MP4OutputLocation = mc.s3TransferBucket.JoinPath(mcMp4OutputRelPath)
	}

	// Do the actual transcode
	err = mc.coreAwsTranscode(ctx, mcArgs, true)
	if err == ErrJobAcceleration {
		err = mc.coreAwsTranscode(ctx, mcArgs, false)
	}
	if err != nil {
		return nil, err
	}

	if hlsTarget != nil {
		mcHlsOutputBaseDir := mc.osTransferBucketURL.JoinPath(mcHlsOutputRelPath, "..")
		log.Log(args.RequestID, "Copying HLS output files from S3", "source", mcHlsOutputBaseDir, "dest", hlsTarget)
		if err := copyDir(mcHlsOutputBaseDir, hlsTarget, args); err != nil {
			return nil, fmt.Errorf("error copying output files: %w", err)
		}
	}

	if args.GenerateMP4 {
		mcMp4OutputBaseDir := mc.osTransferBucketURL.JoinPath(mcMp4OutputRelPath, "..")
		log.Log(args.RequestID, "Copying MP4 output files from S3", "source", mcMp4OutputBaseDir, "dest", mp4Target)
		if err := copyDir(mcMp4OutputBaseDir, mp4Target, args); err != nil {
			return nil, fmt.Errorf("error copying output files: %w", err)
		}
	}

	hlsPlaybackBaseURL, mp4PlaybackBaseURL, err := Publish(toStr(hlsTarget), toStr(mp4Target))
	if err != nil {
		return nil, err
	}

	outputVideo := video.OutputVideo{
		Type: "object_store",
	}
	if hlsTarget != nil {
		hlsPlaybackDirURL, err := url.Parse(hlsPlaybackBaseURL)
		if err != nil {
			return nil, err
		}
		outputHLSFiles, err := mc.outputVideoFiles(mcArgs, hlsPlaybackDirURL, "index", "m3u8")
		if err != nil {
			return nil, err
		}
		outputVideo.Manifest = hlsPlaybackDirURL.JoinPath("index.m3u8").String()
		outputVideo.Videos = outputHLSFiles
	}
	if args.GenerateMP4 {
		mp4PlaybackDirURL, err := url.Parse(mp4PlaybackBaseURL)
		if err != nil {
			return nil, err
		}
		outputMP4Files, err := mc.outputVideoFiles(mcArgs, mp4PlaybackDirURL, mp4OutFilePrefix, "mp4")
		if err != nil {
			return nil, err
		}
		outputVideo.MP4Outputs = outputMP4Files
		if outputVideo.Manifest == "" {
			outputVideo.Manifest = mp4PlaybackBaseURL
		}
	}
	return []video.OutputVideo{outputVideo}, nil
}

func (mc *MediaConvert) outputVideoFiles(mcArgs TranscodeJobArgs, ourOutputBaseDir *url.URL, filePrefix, fileSuffix string) (files []video.OutputVideoFile, err error) {
	for _, profile := range mcArgs.Profiles {
		suffix := profile.Name + "." + fileSuffix
		// get object from s3 to check that it exists and to find out the file size
		videoFile := video.OutputVideoFile{
			Type:     fileSuffix,
			Location: ourOutputBaseDir.JoinPath(filePrefix + suffix).String(),
		}
		// probe output mp4 files
		if fileSuffix == "mp4" {
			key := mcArgs.MP4OutputLocation.JoinPath("..", filePrefix+suffix).Path
			presignedOutputFileURL, err := mc.s3.PresignS3(mc.s3TransferBucket.Host, key)
			if err != nil {
				return nil, fmt.Errorf("error creating s3 url: %w", err)
			}
			videoFile, err = video.PopulateOutput(mcArgs.RequestID, mc.probe, presignedOutputFileURL, videoFile)
			if err != nil {
				return nil, err
			}
		}
		files = append(files, videoFile)
	}
	return
}

// This is the function that does the core AWS workflow for transcoding a file.
// It expects args to be directly compatible with AWS (i.e. S3-only files).
func (mc *MediaConvert) coreAwsTranscode(ctx context.Context, args TranscodeJobArgs, accelerated bool) (err error) {
	log.Log(args.RequestID, "Creating AWS MediaConvert job", "input", args.InputFile, "output", args.HLSOutputLocation, "accelerated", accelerated)

	var mp4OutputLocation string
	if args.GenerateMP4 {
		mp4OutputLocation = toStr(args.MP4OutputLocation)
	}

	payload := createJobPayload(args.InputFile.String(), toStr(args.HLSOutputLocation), mp4OutputLocation, mc.role, accelerated, args.Profiles, args.SegmentSizeSecs)
	job, err := mc.client.CreateJob(payload)
	if err != nil {
		return fmt.Errorf("error creating mediaconvert job: %w", err)
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

			// cancel job if taking too long
			if job.Job.CreatedAt != nil && time.Since(*job.Job.CreatedAt) > mediaConvertJobTimeout {
				_, err := mc.client.CancelJob(&mediaconvert.CancelJobInput{Id: jobID})
				if err == nil {
					// cancel succeeded so we can return here
					return fmt.Errorf("job took too long so was cancelled")
				}
				log.LogError(args.RequestID, "failed to cancel mediaconvert job", err)
			}
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

func createJobPayload(inputFile, hlsOutputFile, mp4OutputFile, role string, accelerated bool, profiles []video.EncodedProfile, segmentSizeSecs int64) *mediaconvert.CreateJobInput {
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
							Offset:           aws.Int64(0),
							DefaultSelection: aws.String("DEFAULT"),
							SelectorType:     aws.String("TRACK"),
							ProgramSelection: aws.Int64(1),
						},
					},
					FileInput:      aws.String(inputFile),
					TimecodeSource: aws.String("ZEROBASED"),
					VideoSelector: &mediaconvert.VideoSelector{
						Rotate: aws.String(mediaconvert.InputRotateAuto),
					},
				},
			},
			OutputGroups: outputGroups(hlsOutputFile, mp4OutputFile, profiles, segmentSizeSecs),
			TimecodeConfig: &mediaconvert.TimecodeConfig{
				Source: aws.String("ZEROBASED"),
			},
		},
		Role:                 aws.String(role),
		AccelerationSettings: acceleration,
	}
}

func outputGroups(hlsOutputFile, mp4OutputFile string, profiles []video.EncodedProfile, segmentSizeSecs int64) []*mediaconvert.OutputGroup {
	var groups []*mediaconvert.OutputGroup
	if hlsOutputFile != "" {
		groups = append(groups, &mediaconvert.OutputGroup{
			Name: aws.String("Apple HLS"),
			OutputGroupSettings: &mediaconvert.OutputGroupSettings{
				HlsGroupSettings: &mediaconvert.HlsGroupSettings{
					Destination:      aws.String(hlsOutputFile),
					MinSegmentLength: aws.Int64(0),
					SegmentLength:    aws.Int64(segmentSizeSecs),
				},
				Type: aws.String("HLS_GROUP_SETTINGS"),
			},
			Outputs:    outputs("M3U8", profiles),
			CustomName: aws.String("hls"),
		})
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
	// If we don't have any video profiles, it means we're in audio-only mode
	if len(profiles) == 0 {
		return audioOnlyOutputs(container, "audioonly")
	} else {
		outs := make([]*mediaconvert.Output, 0, len(profiles))
		for _, profile := range profiles {
			outs = append(outs, output(container, profile.Name, profile.Height, profile.Bitrate))
		}
		return outs
	}
}

func audioOnlyOutputs(container, name string) []*mediaconvert.Output {
	return []*mediaconvert.Output{
		{
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
		},
	}
}

func output(container, name string, height, maxBitrate int64) *mediaconvert.Output {
	return &mediaconvert.Output{
		VideoDescription: &mediaconvert.VideoDescription{
			Height: aws.Int64(height),
			CodecSettings: &mediaconvert.VideoCodecSettings{
				Codec: aws.String("H_264"),
				H264Settings: &mediaconvert.H264Settings{
					GopSizeUnits:       aws.String(mediaconvert.H264GopSizeUnitsAuto),
					MaxBitrate:         aws.Int64(maxBitrate),
					RateControlMode:    aws.String("QVBR"),
					SceneChangeDetect:  aws.String("TRANSITION_DETECTION"),
					QualityTuningLevel: aws.String("SINGLE_PASS"),
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

func copyDir(source, dest *url.URL, args TranscodeJobArgs) error {
	ctx, cancel := context.WithTimeout(context.Background(), MAX_COPY_DIR_DURATION)
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
				_, err := CopyFile(ctx, source.JoinPath(file).String(), dest.String(), file, args.RequestID)
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
	return strings.TrimLeft(strings.TrimPrefix(filePath, baseDir), "/")
}

// Returns the directory where the files will be stored given an OS URL
func getTargetDir(url *url.URL, requestID, suffix string) string {
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
	return path.Join(dir, requestID, suffix)
}

func contains[T comparable](v T, list []T) bool {
	for _, elm := range list {
		if elm == v {
			return true
		}
	}
	return false
}

func toStr(URL *url.URL) string {
	if URL != nil {
		return URL.String()
	}
	return ""
}
