package clients

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/video"
)

type InputCopy struct {
	S3    S3
	Probe video.Prober
}

// CopyInputToS3 copies the input video to our S3 transfer bucket and probes the file.
func (s *InputCopy) CopyInputToS3(args TranscodeJobArgs, s3HTTPTransferURL *url.URL) (TranscodeJobArgs, error) {
	if s3HTTPTransferURL == nil {
		return TranscodeJobArgs{}, errors.New("s3HTTPTransferURL was nil")
	}
	s3URL, err := url.Parse("s3://" + s3HTTPTransferURL.Path)
	if err != nil {
		return TranscodeJobArgs{}, fmt.Errorf("failed to parse s3 url: %w", err)
	}

	log.Log(args.RequestID, "Copying input file to S3", "source", args.InputFile, "dest", s3URL)
	size, err := CopyFile(context.Background(), args.InputFile.String(), s3HTTPTransferURL.String(), "", args.RequestID)
	if err != nil {
		return TranscodeJobArgs{}, fmt.Errorf("error copying input file to S3: %w", err)
	}
	if size <= 0 {
		return TranscodeJobArgs{}, fmt.Errorf("zero bytes found for source: %s", args.InputFile)
	}
	log.Log(args.RequestID, "Copied", "bytes", size, "source", args.InputFile, "dest", s3URL)
	args.CollectSourceSize(size)

	presignedInputFileURL, err := s.S3.PresignS3(s3URL.Host, s3URL.Path)
	if err != nil {
		return TranscodeJobArgs{}, fmt.Errorf("error creating s3 url: %w", err)
	}

	log.Log(args.RequestID, "starting probe", "s3url", s3URL)
	inputVideoProbe, err := s.Probe.ProbeFile(presignedInputFileURL)
	if err != nil {
		log.Log(args.RequestID, "probe failed", "s3url", s3URL, "err", err)
		return TranscodeJobArgs{}, fmt.Errorf("error probing MP4 input file from S3: %w", err)
	}
	log.Log(args.RequestID, "probe succeeded", "s3url", s3URL)
	videoTrack, err := inputVideoProbe.GetVideoTrack()
	if err != nil {
		return TranscodeJobArgs{}, fmt.Errorf("no video track found in input video: %w", err)
	}
	if videoTrack.FPS <= 0 {
		// unsupported, includes things like motion jpegs
		return TranscodeJobArgs{}, fmt.Errorf("invalid framerate: %f", videoTrack.FPS)
	}

	if inputVideoProbe.SizeBytes > maxInputFileSizeBytes {
		return TranscodeJobArgs{}, fmt.Errorf("input file %d bytes was greater than %d bytes", inputVideoProbe.SizeBytes, maxInputFileSizeBytes)
	}
	args.InputFileInfo = inputVideoProbe
	args.InputFile = s3URL
	return args, nil
}
