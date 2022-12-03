package clients

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path"
	"testing"

	"github.com/aws/aws-sdk-go/service/mediaconvert"
	"github.com/stretchr/testify/require"
)

func TestOnlyS3URLsToAWSClient(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "user-input-*")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = f.WriteString(exampleFileContents)
	require.NoError(t, err)

	// use the random file name as the dir name for the transfer file
	mcInputDir := path.Join(os.TempDir(), path.Ext(f.Name()))
	transferredFile := path.Join(mcInputDir, "input/1234/video")
	defer os.Remove(transferredFile)

	mc := &MediaConvert{
		s3TransferBucket:    mustParseURL("s3://thebucket"),
		osTransferBucketURL: mustParseURL("file://" + mcInputDir),
		client: stubMediaConvertClient{
			createJob: func(input *mediaconvert.CreateJobInput) (*mediaconvert.CreateJobOutput, error) {
				// check that only an s3:// URL is sent to AWS client
				require.Equal(t, "s3://thebucket/input/1234/video", *input.Settings.Inputs[0].FileInput)
				require.Equal(t, "s3://thebucket/output/1234/index", *input.Settings.OutputGroups[0].OutputGroupSettings.HlsGroupSettings.Destination)
				return nil, errors.New("secret error")
			},
		},
	}
	err = mc.Transcode(context.Background(), TranscodeJobArgs{
		InputFile:     mustParseURL("file://" + f.Name()),
		HLSOutputFile: mustParseURL("s3+https://endpoint.com/bucket/1234/index.m3u8"),
	})
	require.ErrorContains(t, err, "secret error")

	// Check that the file was copied to the osTransferBucketURL folder
	_, err = os.Stat(transferredFile)
	require.NoError(t, err)
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
