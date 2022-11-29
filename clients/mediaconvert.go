package clients

import (
 "context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/mediaconvert"
)

type MediaConvert struct {
	// TODO
}

func NewMediaConvertClient( /*add any static config you need like endpoint, role, queue etc*/ ) TranscodeProvider {
	return &MediaConvert{
		// TODO
	}
}

// This should do the whole transcode job, including the polling loop for the
// job status until it is completed.
//
// It should call the input.ReportProgress function to report the progress of
// the job during the polling loop.
func (mc *MediaConvert) Transcode(ctx context.Context, input TranscodeJobInput) error {
	config := aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", "default"),
	}
	sess, err := session.NewSession(&config)

	svc := mediaconvert.New(sess)

	// Add endpoint to the service client
	svc.Endpoint = "https://vasjpylpa.mediaconvert.us-east-1.amazonaws.com"


	job, err := svc.CreateJob(&mediaconvert.CreateJobInput{
		Settings: &mediaconvert.JobSettings{
			Inputs: []*mediaconvert.Input{
				{
					AudioSelectors: map[string]*mediaconvert.AudioSelector{
						"Audio Selector 1": {
							DefaultSelection: aws.String("DEFAULT"),
						},
					}, 
					FileInput:      aws.String(input.InputFile), 
					TimecodeSource: aws.String("ZEROBASED"),
					VideoSelector:  &mediaconvert.VideoSelector{},
				},
			},
			OutputGroups: []*mediaconvert.OutputGroup{
				{
					Name: aws.String("Apple HLS"),
					OutputGroupSettings: &mediaconvert.OutputGroupSettings{
						HlsGroupSettings: &mediaconvert.HlsGroupSettings{
							Destination:      aws.String(input.HLSOutputFile),
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
		Role:  aws.String("arn:aws:iam::055703596054:role/service-role/MediaConvert_Default_Role"),
		Queue: aws.String("arn:aws:mediaconvert:us-east-1:055703596054:queues/Default"),
		AccelerationSettings: &mediaconvert.AccelerationSettings{
			Mode: aws.String("ENABLED"),
		},
	})



	if err != nil {
		fmt.Println(err)
	}

	// print job id
	fmt.Println(*job.Job.Id)

	// checking if the job is completed (every 3 seconds)
	for {
		time.Sleep(3 * time.Second)
		job, err := svc.GetJob(&mediaconvert.GetJobInput{
			Id: job.Job.Id,
		})
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(*job.Job.Status)
		if *job.Job.Status == "COMPLETE" {
			fmt.Println("Job is complete")
			break
		}
	}
	return nil
}




