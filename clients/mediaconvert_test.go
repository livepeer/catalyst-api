package clients

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/mediaconvert"
	"github.com/stretchr/testify/require"
)

func TestOnlyS3URLsToAWSClient(t *testing.T) {
	require := require.New(t)
	f, err := os.CreateTemp(os.TempDir(), "user-input-*")
	require.NoError(err)
	defer os.Remove(f.Name())
	_, err = f.WriteString(exampleFileContents)
	require.NoError(err)

	// use the random file name as the dir name for the transfer file
	mcInputDir := path.Join(os.TempDir(), "out", f.Name())
	transferredFile := path.Join(mcInputDir, "input/1234/video")
	defer os.Remove(transferredFile)

	mc := &MediaConvert{
		s3TransferBucket:    mustParseURL("s3://thebucket"),
		osTransferBucketURL: mustParseURL("file://" + mcInputDir),
		client: stubMediaConvertClient{
			createJob: func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
				// check that only an s3:// URL is sent to AWS client
				require.Equal("s3://thebucket/input/1234/video", *input.Settings.Inputs[0].FileInput)
				require.Equal("s3://thebucket/output/1234/index", *input.Settings.OutputGroups[0].OutputGroupSettings.HlsGroupSettings.Destination)
				return nil, errors.New("secret error")
			},
		},
	}
	err = mc.Transcode(context.Background(), TranscodeJobArgs{
		InputFile:     mustParseURL("file://" + f.Name()),
		HLSOutputFile: mustParseURL("s3+https://endpoint.com/bucket/1234/index.m3u8"),
		CollectSourceSize: func(size int64) {
			require.Equal(len(exampleFileContents), int(size))
		},
	})
	require.ErrorContains(err, "secret error")

	// Check that the file was copied to the osTransferBucketURL folder
	_, err = os.Stat(transferredFile)
	require.NoError(err)
}

func TestSendsOriginalURLToS3OnCopyError(t *testing.T) {
	require := require.New(t)
	mcInputDir := path.Join(os.TempDir(), "out")
	transferredFile := path.Join(mcInputDir, "input/1234/video")
	defer os.Remove(transferredFile)

	awsStub := &stubMediaConvertClient{
		createJob: func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
			// check that the https? URL is sent to AWS client if the copy fails
			require.Equal("http://localhost:3000/not-here.mp4", *input.Settings.Inputs[0].FileInput)
			require.Equal("s3://thebucket/output/1234/index", *input.Settings.OutputGroups[0].OutputGroupSettings.HlsGroupSettings.Destination)
			return nil, errors.New("secret error")
		},
	}
	mc := &MediaConvert{
		s3TransferBucket:    mustParseURL("s3://thebucket"),
		osTransferBucketURL: mustParseURL("file://" + mcInputDir),
		client:              awsStub,
	}
	err := mc.Transcode(context.Background(), TranscodeJobArgs{
		// use a non existing HTTP endpoint for the file
		InputFile:         mustParseURL("http://localhost:3000/not-here.mp4"),
		HLSOutputFile:     mustParseURL("s3+https://endpoint.com/bucket/1234/index.m3u8"),
		CollectSourceSize: func(size int64) {},
	})
	require.ErrorContains(err, "secret error")

	// Now check that it does NOT send the original URL to S3 if it's an OS URL
	awsStub.createJob = func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
		require.Fail("should not have been called")
		return nil, errors.New("unreachable")
	}
	err = mc.Transcode(context.Background(), TranscodeJobArgs{
		// use a non existing an OS URL
		InputFile:         mustParseURL("s3+https://user:pwd@localhost:4321/bucket/no-minio-here.mp4"),
		HLSOutputFile:     mustParseURL("s3+https://endpoint.com/bucket/1234/index.m3u8"),
		CollectSourceSize: func(size int64) {},
	})
	require.ErrorContains(err, "download error")

	// Check that no file was created to the osTransferBucketURL folder
	_, err = os.Stat(transferredFile)
	require.ErrorContains(err, "no such file")
}

func TestRetriesOnAccelerationError(t *testing.T) {
	require := require.New(t)
	mcInputDir := path.Join(os.TempDir(), "out")
	transferredFile := path.Join(mcInputDir, "input/1234/video")
	defer os.Remove(transferredFile)

	createdJobs := 0
	mc := &MediaConvert{
		s3TransferBucket:    mustParseURL("s3://thebucket"),
		osTransferBucketURL: mustParseURL("file://" + mcInputDir),
		client: stubMediaConvertClient{
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
		},
	}
	err := mc.Transcode(context.Background(), TranscodeJobArgs{
		// use a non existing HTTP endpoint for the file
		InputFile:         mustParseURL("http://localhost:3000/not-here.mp4"),
		HLSOutputFile:     mustParseURL("s3+https://endpoint.com/bucket/1234/index.m3u8"),
		CollectSourceSize: func(size int64) {},
	})
	require.ErrorContains(err, "done with this test")
	require.Equal(2, createdJobs)

	// Check that no file was created to the osTransferBucketURL folder
	_, err = os.Stat(transferredFile)
	require.ErrorContains(err, "no such file")
}

func mustParseURL(str string) *url.URL {
	u, err := url.Parse(str)
	if err != nil {
		panic(err)
	}
	return u
}

type stubMediaConvertClient struct {
	createJob func(*mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error)
	getJob    func(*mediaconvert.GetJobInput) (*mediaconvert.GetJobOutput, error)
}

func (s stubMediaConvertClient) CreateJob(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
	if s.createJob == nil {
		return nil, errors.New("not implemented")
	}
	return s.createJob(input)
}

func (s stubMediaConvertClient) GetJob(input *mediaconvert.GetJobInput) (*mediaconvert.GetJobOutput, error) {
	if s.getJob == nil {
		return nil, errors.New("not implemented")
	}
	return s.getJob(input)
}
