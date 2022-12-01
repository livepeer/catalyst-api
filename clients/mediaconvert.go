package clients

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/mediaconvert"
	"github.com/livepeer/catalyst-api/log"
)

const pollDelay = 10 * time.Second

type MediaConvertOptions struct {
	Endpoint, Region, Role       string
	AccessKeyID, AccessKeySecret string
	// TODO: move the aux bucket usage to this client. This is unused for now.
	S3AuxBucket *url.URL
}

type MediaConvert struct {
	MediaConvertOptions
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

// This should do the whole transcode job, including the polling loop for the
// job status until it is completed.
//
// It should call the input.ReportProgress function to report the progress of
// the job during the polling loop.
func (mc *MediaConvert) Transcode(ctx context.Context, input TranscodeJobInput) error {
	job, err := mc.client.CreateJob(createJobPayload(input.InputFile, input.HLSOutputFile, mc.Role))
	if err != nil {
		return fmt.Errorf("error creting mediaconvert job: %w", err)
	}
	jobID := job.Job.Id
	log.AddContext("mediaconvert_job_id", aws.StringValue(jobID))
	log.Log(input.RequestID, "Created MediaConvert job")

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
			log.Log(input.RequestID, "Got mediaconvert job progress", "progress", progress, "status")
			if input.ReportProgress != nil {
				input.ReportProgress(progress)
			}
		case mediaconvert.JobStatusComplete:
			log.Log(input.RequestID, "Mediaconvert job completed successfully")
			return nil
		case mediaconvert.JobStatusError:
			errMsg := aws.StringValue(job.Job.ErrorMessage)
			log.Log(input.RequestID, "Mediaconvert job failed", "error", errMsg)
			return fmt.Errorf("job failed: %s", errMsg)
		case mediaconvert.JobStatusCanceled:
			log.Log(input.RequestID, "Mediaconvert job unexpectedly canceled")
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
