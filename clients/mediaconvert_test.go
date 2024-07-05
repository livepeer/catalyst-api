package clients

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/mediaconvert"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
)

const dummyHlsPlaylist = `
#EXTM3U

#EXTINF:10,
0.ts

#EXT-X-ENDLIST`

var inputVideo = video.InputVideo{
	Tracks: []video.InputTrack{{
		Type:    "video",
		Codec:   "",
		Bitrate: 3000,
		VideoTrack: video.VideoTrack{
			Width:  1080,
			Height: 7200,
		},
		AudioTrack: video.AudioTrack{},
	}},
	Duration: 60,
}

func TestReportsMediaConvertProgress(t *testing.T) {
	require := require.New(t)

	createJobCalls, getJobCalls := 0, 0
	awsStub := &stubMediaConvertClient{
		createJob: func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
			createJobCalls++
			return &mediaconvert.CreateJobOutput{Job: &mediaconvert.Job{Id: aws.String("10")}}, nil
		},
		getJob: func(input *mediaconvert.GetJobInput) (*mediaconvert.GetJobOutput, error) {
			getJobCalls++
			switch getJobCalls {
			case 1:
				return &mediaconvert.GetJobOutput{Job: &mediaconvert.Job{
					Status:             aws.String(mediaconvert.JobStatusProgressing),
					JobPercentComplete: aws.Int64(50),
				}}, nil
			case 2:
				return nil, errors.New("done with this test")
			default:
				require.Fail("unexpected call")
				return nil, errors.New("unreachable")
			}
		},
	}
	mc, f, _, cleanup := setupTestMediaConvert(t, awsStub)
	defer cleanup()

	reportProgressCalls := 0
	_, err := mc.Transcode(context.Background(), TranscodeJobArgs{
		InputFile:         mustParseURL(t, "file://"+f.Name()),
		HLSOutputLocation: mustParseURL(t, "s3+https://endpoint.com/bucket/1234"),
		ReportProgress: func(progress float64) {
			reportProgressCalls++
			require.InEpsilon(0.5, progress, 1e-9)
		},
		InputFileInfo: inputVideo,
	})
	require.ErrorContains(err, "done with this test")
	require.Equal(1, createJobCalls)
	require.Equal(2, getJobCalls)
	require.Equal(1, reportProgressCalls)
}

func TestInputDurationCheck(t *testing.T) {
	require := require.New(t)

	awsStub := &stubMediaConvertClient{}
	mc, f, _, cleanup := setupTestMediaConvert(t, awsStub)
	defer cleanup()

	_, err := mc.Transcode(context.Background(), TranscodeJobArgs{
		InputFile:         mustParseURL(t, "file://"+f.Name()),
		HLSOutputLocation: mustParseURL(t, "s3+https://endpoint.com/bucket/1234"),
		InputFileInfo: video.InputVideo{
			Duration: 60_000,
		},
	})
	require.EqualError(err, "input too long for mediaconvert: 60000")
}

func TestRetriesOnAccelerationError(t *testing.T) {
	require := require.New(t)

	createdJobs := 0
	awsStub := &stubMediaConvertClient{
		createJob: func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
			createdJobs++
			switch createdJobs {
			case 1:
				require.Equal(*input.AccelerationSettings.Mode, mediaconvert.AccelerationModePreferred)
				return &mediaconvert.CreateJobOutput{Job: &mediaconvert.Job{Id: aws.String("420")}}, nil
			case 2:
				require.Nil(input.AccelerationSettings)
				return nil, errors.New("we are done with this test")
			default:
				require.Fail("should not have been called")
				return nil, errors.New("unreachable")
			}
		},
		getJob: func(input *mediaconvert.GetJobInput) (*mediaconvert.GetJobOutput, error) {
			switch *input.Id {
			case "420":
				return &mediaconvert.GetJobOutput{Job: &mediaconvert.Job{
					Status:       aws.String(mediaconvert.JobStatusError),
					ErrorMessage: aws.String("enhance your calm"),
					ErrorCode:    aws.Int64(1550),
				}}, nil
			default:
				require.Fail("unknown job id " + *input.Id)
				return nil, errors.New("unreachable")
			}
		},
	}
	mc, inputFile, _, cleanup := setupTestMediaConvert(t, awsStub)
	defer cleanup()

	_, err := mc.Transcode(context.Background(), TranscodeJobArgs{
		// use a non existing HTTP endpoint for the file
		InputFile:         mustParseURL(t, "file://"+inputFile.Name()),
		HLSOutputLocation: mustParseURL(t, "s3+https://endpoint.com/bucket/1234"),
		InputFileInfo:     inputVideo,
	})
	require.ErrorContains(err, "done with this test")
	require.Equal(2, createdJobs)
}

func TestCopiesMediaConvertOutputToFinalLocation(t *testing.T) {
	require := require.New(t)

	var transfOutputFile string
	createJobCalls, getJobCalls := 0, 0
	awsStub := &stubMediaConvertClient{
		createJob: func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
			createJobCalls++
			return &mediaconvert.CreateJobOutput{Job: &mediaconvert.Job{Id: aws.String("10")}}, nil
		},
		getJob: func(input *mediaconvert.GetJobInput) (*mediaconvert.GetJobOutput, error) {
			getJobCalls++
			switch getJobCalls {
			case 1:
				return &mediaconvert.GetJobOutput{Job: &mediaconvert.Job{
					Status:             aws.String(mediaconvert.JobStatusProgressing),
					JobPercentComplete: aws.Int64(50),
				}}, nil
			case 2:
				require.NoError(os.WriteFile(transfOutputFile, []byte(dummyHlsPlaylist), 0777))
				require.NoError(os.WriteFile(path.Join(transfOutputFile, "../1.ts"), []byte(exampleFileContents), 0777))

				return &mediaconvert.GetJobOutput{Job: &mediaconvert.Job{
					Status: aws.String(mediaconvert.JobStatusComplete),
				}}, nil
			default:
				require.Fail("unexpected call")
				return nil, errors.New("unreachable")
			}
		},
	}
	mc, inputFile, transferDir, cleanup := setupTestMediaConvert(t, awsStub)
	defer cleanup()

	outLocation := path.Join(transferDir, "../hls")
	defer os.RemoveAll(path.Dir(outLocation))
	transfOutputFile = path.Join(transferDir, "output", outLocation, "index.m3u8")
	require.NoError(os.MkdirAll(path.Dir(transfOutputFile), 0777))

	_, err := mc.Transcode(context.Background(), TranscodeJobArgs{
		InputFile:                mustParseURL(t, "file://"+inputFile.Name()),
		HLSOutputLocation:        mustParseURL(t, "file:/"+outLocation),
		ReportProgress:           func(progress float64) {},
		CollectTranscodedSegment: func() {},
		InputFileInfo:            inputVideo,
	})
	require.NoError(err)
	require.Equal(1, createJobCalls)
	require.Equal(2, getJobCalls)

	// Check that the output files were copied to the osTransferBucketURL folder
	content, err := os.ReadFile(path.Join(outLocation, "index.m3u8"))
	require.NoError(err)
	require.Equal(dummyHlsPlaylist, string(content))

	content, err = os.ReadFile(path.Join(outLocation, "1.ts"))
	require.NoError(err)
	require.Equal(exampleFileContents, string(content))
}

func Test_createJobPayload(t *testing.T) {
	var (
		inputFile     = "input"
		hlsOutputFile = "output"
		role          = "role"
	)
	type args struct {
		mp4OutputFile string
		accelerated   bool
		profiles      []video.EncodedProfile
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "happy",
			args: args{
				mp4OutputFile: "mp4out",
				accelerated:   false,
				profiles:      video.DefaultTranscodeProfiles,
			},
			want: "fixtures/mediaconvert_payloads/happy.txt",
		},
		{
			name: "accelerated",
			args: args{
				mp4OutputFile: "mp4out",
				accelerated:   true,
				profiles:      video.DefaultTranscodeProfiles,
			},
			want: "fixtures/mediaconvert_payloads/accelerated.txt",
		},
		{
			name: "no MP4",
			args: args{
				accelerated: false,
				profiles:    video.DefaultTranscodeProfiles,
			},
			want: "fixtures/mediaconvert_payloads/no-mp4.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := createJobPayload(inputFile, hlsOutputFile, tt.args.mp4OutputFile, "thumbs", role, tt.args.accelerated, tt.args.profiles, config.DefaultSegmentSizeSecs)
			require.NotNil(t, actual)
			require.Equal(t, loadFixture(t, tt.want, actual.String()), actual.String())
		})
	}
}

func Test_MP4OutDurationCheck(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name        string
		duration    float64
		outputs     []string
		generatemp4 bool
	}{
		{
			name:        "hls and mp4",
			duration:    120,
			outputs:     []string{"thumbs", "hls", "mp4"},
			generatemp4: true,
		},
		{
			name:        "hls only",
			duration:    121,
			outputs:     []string{"thumbs", "hls"},
			generatemp4: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			awsStub := &stubMediaConvertClient{
				createJob: func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
					require.Len(input.Settings.OutputGroups, len(tt.outputs))
					for i, outputName := range tt.outputs {
						require.Equal(outputName, *input.Settings.OutputGroups[i].CustomName)
					}
					// throw an error to end exit early as we only want to test the MC job input
					return nil, errors.New("secret error")
				},
			}
			mc, f, _, cleanup := setupTestMediaConvert(t, awsStub)
			defer cleanup()
			iv := inputVideo
			iv.Duration = tt.duration
			_, err := mc.Transcode(context.Background(), TranscodeJobArgs{
				InputFile:         mustParseURL(t, "file://"+f.Name()),
				HLSOutputLocation: mustParseURL(t, "s3+https://endpoint.com/bucket/1234"),
				MP4OutputLocation: mustParseURL(t, "s3+https://endpoint.com/bucket/1234"),
				GenerateMP4:       tt.generatemp4,
				InputFileInfo:     iv,
			})
			require.Error(err)
		})
	}
}

func loadFixture(t *testing.T, expectedPath, actual string) string {
	if os.Getenv("REGEN_FIXTURES") != "" {
		err := os.WriteFile(expectedPath, []byte(actual), 0644)
		require.NoError(t, err)
	}

	file, err := os.ReadFile(expectedPath)
	require.NoError(t, err)
	return string(file)
}

func setupTestMediaConvert(t *testing.T, awsStub AWSMediaConvertClient) (mc *MediaConvert, inputFile *os.File, transferDir string, cleanup func()) {
	oldMaxRetryInterval, oldPollDelay := maxRetryInterval, pollDelay
	maxRetryInterval, pollDelay = 1*time.Millisecond, 1*time.Millisecond

	var err error
	inputFile, err = os.CreateTemp(os.TempDir(), "user-input-*")
	require.NoError(t, err)
	movieFile, err := os.Open("./fixtures/mediaconvert_payloads/sample.mp4")
	require.NoError(t, err)
	_, err = io.Copy(inputFile, movieFile)
	require.NoError(t, err)
	_, err = inputFile.WriteString(exampleFileContents)
	require.NoError(t, err)
	require.NoError(t, movieFile.Close())

	// use the random file name as the dir name for the transfer file
	transferDir = path.Join(inputFile.Name()+"-dir", "transfer")
	require.NoError(t, os.MkdirAll(transferDir, 0777))

	cleanup = func() {
		maxRetryInterval, pollDelay = oldMaxRetryInterval, oldPollDelay
		inErr := os.Remove(inputFile.Name())
		dirErr := os.RemoveAll(transferDir)
		require.NoError(t, inErr)
		require.NoError(t, dirErr)
		require.NoError(t, inputFile.Close())
	}

	s3Client := &stubS3Client{transferDir}
	probe := video.Probe{}
	mc = &MediaConvert{
		s3TransferBucket:    mustParseURL(t, "s3://thebucket"),
		osTransferBucketURL: mustParseURL(t, "file://"+transferDir),
		client:              awsStub,
		s3:                  s3Client,
		probe:               probe,
	}
	return
}

func mustParseURL(t *testing.T, str string) *url.URL {
	u, err := url.Parse(str)
	require.NoError(t, err)
	return u
}

type stubMediaConvertClient struct {
	createJob func(*mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error)
	getJob    func(*mediaconvert.GetJobInput) (*mediaconvert.GetJobOutput, error)
}

func (s *stubMediaConvertClient) CreateJob(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
	if s.createJob == nil {
		return nil, errors.New("not implemented")
	}
	return s.createJob(input)
}

func (s *stubMediaConvertClient) GetJob(input *mediaconvert.GetJobInput) (*mediaconvert.GetJobOutput, error) {
	if s.getJob == nil {
		return nil, errors.New("not implemented")
	}
	return s.getJob(input)
}

func (s *stubMediaConvertClient) CancelJob(input *mediaconvert.CancelJobInput) (*mediaconvert.CancelJobOutput, error) {
	// noop
	return nil, nil
}

type stubS3Client struct {
	transferDir string
}

func (s *stubS3Client) PresignS3(_, key string) (string, error) {
	return s.transferDir + "/" + key, nil
}

func (s *stubS3Client) GetObject(bucket, key string) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{
		ContentLength: aws.Int64(123),
	}, nil
}
