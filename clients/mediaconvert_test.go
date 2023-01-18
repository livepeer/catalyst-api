package clients

import (
	"context"
	"errors"
	"net/url"
	"os"
	"io"
	"path"
	"testing"
	"time"
	//"crypto/md5"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/mediaconvert"
	"github.com/livepeer/catalyst-api/config"
	"github.com/stretchr/testify/require"
)

const dummyHlsPlaylist = `
#EXTM3U

#EXTINF:10,
0.ts

#EXT-X-ENDLIST`

func TestOnlyS3URLsToAWSClient(t *testing.T) {
	require := require.New(t)
	awsStub := &stubMediaConvertClient{
		createJob: func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
			// check that only an s3:// URL is sent to AWS client
			require.Equal("s3://thebucket/input/1234/video", *input.Settings.Inputs[0].FileInput)
			require.Equal("s3://thebucket/output/1234/index", *input.Settings.OutputGroups[0].OutputGroupSettings.HlsGroupSettings.Destination)
			return nil, errors.New("secret error")
		},
	}
	mc, f, _, cleanup := setupTestMediaConvert(t, awsStub)
	defer cleanup()
	sz, err := f.Stat()
	require.NoError(err)

	err = mc.Transcode(context.Background(), TranscodeJobArgs{
		InputFile:     mustParseURL(t, "file://"+f.Name()),
		HLSOutputFile: mustParseURL(t, "s3+https://endpoint.com/bucket/1234/index.m3u8"),
		CollectSourceSize: func(size int64) {
			require.Equal(sz.Size(), int64(size))
		},
	})
	require.ErrorContains(err, "secret error")
/*
	// Check that the file was copied to the osTransferBucketURL folder
	content, err := os.Open(path.Join(transferDir, "input/1234/video"))
	require.NoError(err)

	hashContent := md5.New()
	require.NoError(err)
	_, err = io.Copy(hashContent, content)
	require.NoError(err)

	hashInputFile := md5.New()
	require.NoError(err)
	_, err = io.Copy(hashInputFile, f)
	require.NoError(err)

	require.Equal(hashInputFile, hashContent)
*/
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
	err := mc.Transcode(context.Background(), TranscodeJobArgs{
		InputFile:         mustParseURL(t, "file://"+f.Name()),
		HLSOutputFile:     mustParseURL(t, "s3+https://endpoint.com/bucket/1234/index.m3u8"),
		CollectSourceSize: func(size int64) {},
		ReportProgress: func(progress float64) {
			reportProgressCalls++
			require.InEpsilon(0.5, progress, 1e-9)
		},
	})
	require.ErrorContains(err, "done with this test")
	require.Equal(1, createJobCalls)
	require.Equal(2, getJobCalls)
	require.Equal(1, reportProgressCalls)
}

func TestSendsOriginalURLToS3OnCopyError(t *testing.T) {
	require := require.New(t)

	awsStub := &stubMediaConvertClient{
		createJob: func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
			// check that the https? URL is sent to AWS client if the copy fails
			require.Equal("http://localhost:3000/not-here.mp4", *input.Settings.Inputs[0].FileInput)
			require.Equal("s3://thebucket/output/1234/index", *input.Settings.OutputGroups[0].OutputGroupSettings.HlsGroupSettings.Destination)
			return nil, errors.New("secret error")
		},
	}
	mc, _, transferDir, cleanup := setupTestMediaConvert(t, awsStub)
	defer cleanup()

	err := mc.Transcode(context.Background(), TranscodeJobArgs{
		// use a non existing HTTP endpoint for the file
		InputFile:         mustParseURL(t, "http://localhost:3000/not-here.mp4"),
		HLSOutputFile:     mustParseURL(t, "s3+https://endpoint.com/bucket/1234/index.m3u8"),
		CollectSourceSize: func(size int64) {},
	})
	require.ErrorContains(err, "error")

	// Now check that it does NOT send the original URL to S3 if it's an OS URL
	awsStub.createJob = func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
		require.Fail("should not have been called")
		return nil, errors.New("unreachable")
	}
	err = mc.Transcode(context.Background(), TranscodeJobArgs{
		// use a non existing OS URL
		InputFile:         mustParseURL(t, "s3+https://user:pwd@localhost:4321/bucket/no-minio-here.mp4"),
		HLSOutputFile:     mustParseURL(t, "s3+https://endpoint.com/bucket/1234/index.m3u8"),
		CollectSourceSize: func(size int64) {},
	})
	require.ErrorContains(err, "download error")

	// Check that no file was created to the osTransferBucketURL folder
	_, err = os.Stat(path.Join(transferDir, "input/1234/video"))
	require.ErrorContains(err, "no such file")
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

	err := mc.Transcode(context.Background(), TranscodeJobArgs{
		// use a non existing HTTP endpoint for the file
		InputFile:         mustParseURL(t, "file://"+inputFile.Name()),
		HLSOutputFile:     mustParseURL(t, "s3+https://endpoint.com/bucket/1234/index.m3u8"),
		CollectSourceSize: func(size int64) {},
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

	outFile := path.Join(transferDir, "../out/index.m3u8")
	defer os.RemoveAll(path.Dir(outFile))
	transfOutputFile = path.Join(transferDir, "output", outFile)
	require.NoError(os.MkdirAll(path.Dir(transfOutputFile), 0777))

	err := mc.Transcode(context.Background(), TranscodeJobArgs{
		InputFile:                mustParseURL(t, "file://"+inputFile.Name()),
		HLSOutputFile:            mustParseURL(t, "file:/"+outFile),
		CollectSourceSize:        func(size int64) {},
		ReportProgress:           func(progress float64) {},
		CollectTranscodedSegment: func() {},
	})
	require.NoError(err)
	require.Equal(1, createJobCalls)
	require.Equal(2, getJobCalls)

	// Check that the output files were copied to the osTransferBucketURL folder
	content, err := os.ReadFile(outFile)
	require.NoError(err)
	require.Equal(dummyHlsPlaylist, string(content))

	content, err = os.ReadFile(path.Join(outFile, "../1.ts"))
	require.NoError(err)
	require.Equal(exampleFileContents, string(content))
}

func Test_createJobPayload(t *testing.T) {
	type args struct {
		inputFile     string
		hlsOutputFile string
		mp4OutputFile string
		role          string
		accelerated   bool
		profiles      []EncodedProfile
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "happy",
			args: args{
				inputFile:     "input",
				hlsOutputFile: "output",
				mp4OutputFile: "mp4out",
				role:          "role",
				accelerated:   false,
				profiles:      DefaultTranscodeProfiles,
			},
			want: "fixtures/mediaconvert_payloads/happy.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := createJobPayload(tt.args.inputFile, tt.args.hlsOutputFile, tt.args.mp4OutputFile, tt.args.role, tt.args.accelerated, tt.args.profiles)
			require.NotNil(t, actual)
			require.Equal(t, loadFixture(t, tt.want, actual.String()), actual.String())
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
	oldMaxRetryInterval, oldRetries, oldPollDelay := maxRetryInterval, config.DownloadOSURLRetries, pollDelay
	maxRetryInterval, config.DownloadOSURLRetries, pollDelay = 1*time.Millisecond, 1, 1*time.Millisecond

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
		maxRetryInterval, config.DownloadOSURLRetries, pollDelay = oldMaxRetryInterval, oldRetries, oldPollDelay
		inErr := os.Remove(inputFile.Name())
		dirErr := os.RemoveAll(transferDir)
		require.NoError(t, inErr)
		require.NoError(t, dirErr)
		require.NoError(t, inputFile.Close())
	}

	mc = &MediaConvert{
		s3TransferBucket:    mustParseURL(t, "s3://thebucket"),
		osTransferBucketURL: mustParseURL(t, "file://"+transferDir),
		client:              awsStub,
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
