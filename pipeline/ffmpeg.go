package pipeline

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/transcode"
	"github.com/livepeer/catalyst-api/video"
)

const LocalSourceFilePattern = "sourcevideo*"

type ffmpeg struct {
	// The base of where to output source segments to
	SourceOutputUrl string
}

func init() {
	// Clean up any temp source files that might be lying around from jobs that were interrupted
	// during a deploy
	if err := cleanUpLocalTmpFiles(os.TempDir(), LocalSourceFilePattern, 6*time.Hour); err != nil {
		log.LogNoRequestID("cleanUpLocalTmpFiles error: %w", err)
	}
}

func (f *ffmpeg) Name() string {
	return "catalyst_ffmpeg"
}

func (f *ffmpeg) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	log.Log(job.RequestID, "Handling job via FFMPEG/Livepeer pipeline")

	sourceOutputBaseURL, err := url.Parse(f.SourceOutputUrl)
	if err != nil {
		return nil, fmt.Errorf("cannot create sourceOutputUrl: %w", err)
	}
	sourceOutputURL := sourceOutputBaseURL.JoinPath(job.RequestID)
	segmentingTargetURL := sourceOutputURL.JoinPath(config.SEGMENTING_SUBDIR, config.SEGMENTING_TARGET_MANIFEST)

	job.SourceOutputURL = sourceOutputURL.String()
	job.SegmentingTargetURL = segmentingTargetURL.String()
	log.AddContext(job.RequestID, "segmented_url", job.SegmentingTargetURL)
	job.ReportProgress(clients.TranscodeStatusPreparing, 0.3)

	// Segmenting
	if err := copyFileToLocalTmpAndSegment(job); err != nil {
		return nil, err
	}
	job.ReportProgress(clients.TranscodeStatusPreparingCompleted, 1)

	// Transcode Beginning
	log.Log(job.RequestID, "Beginning transcoding via FFMPEG/Livepeer pipeline")

	transcodeRequest := transcode.TranscodeSegmentRequest{
		SourceFile:        job.SourceFile,
		CallbackURL:       job.CallbackURL,
		AccessToken:       job.AccessToken,
		TranscodeAPIUrl:   job.TranscodeAPIUrl,
		Profiles:          job.Profiles,
		SourceManifestURL: job.SegmentingTargetURL,
		SourceOutputURL:   job.SourceOutputURL,
		HlsTargetURL:      toStr(job.HlsTargetURL),
		Mp4TargetUrl:      toStr(job.Mp4TargetURL),
		RequestID:         job.RequestID,
		ReportProgress:    job.ReportProgress,
		GenerateMP4:       job.GenerateMP4,
	}

	inputInfo := video.InputVideo{
		Format:    "mp4",
		Duration:  job.InputFileInfo.Duration,
		SizeBytes: int64(job.sourceBytes),
		Tracks: []video.InputTrack{
			// Video Track
			{
				Type:         "video",
				Codec:        job.sourceCodecVideo,
				Bitrate:      job.sourceBitrateVideo,
				DurationSec:  job.InputFileInfo.Duration,
				StartTimeSec: 0,
				VideoTrack: video.VideoTrack{
					Width:  job.sourceWidth,
					Height: job.sourceHeight,
					FPS:    job.sourceFPS,
				},
			},
			// Audio Track
			{
				Type:         "audio",
				Codec:        job.sourceCodecAudio,
				Bitrate:      job.sourceBitrateAudio,
				DurationSec:  job.InputFileInfo.Duration,
				StartTimeSec: 0,
				AudioTrack: video.AudioTrack{
					Channels:   job.sourceChannels,
					SampleRate: job.sourceSampleRate,
					SampleBits: job.sourceSampleBits,
				},
			},
		},
	}

	job.state = "transcoding"

	sourceManifest, err := transcode.DownloadRenditionManifest(transcodeRequest.SourceManifestURL)
	if err != nil {
		return nil, fmt.Errorf("error downloading source manifest: %s", err)
	}

	job.sourceSegments = len(sourceManifest.GetAllSegments())

	outputs, transcodedSegments, err := transcode.RunTranscodeProcess(transcodeRequest, job.StreamName, inputInfo)
	if err != nil {
		log.LogError(job.RequestID, "RunTranscodeProcess returned an error", err)
		return nil, fmt.Errorf("transcoding failed: %w", err)
	}

	job.transcodedSegments = transcodedSegments

	return &HandlerOutput{
		Result: &UploadJobResult{
			InputVideo: inputInfo,
			Outputs:    outputs,
		}}, nil
}

func copyFileToLocalTmpAndSegment(job *JobInfo) error {
	// Create a temporary local file to write to
	localSourceFile, err := os.CreateTemp(os.TempDir(), LocalSourceFilePattern)
	if err != nil {
		return fmt.Errorf("failed to create local file (%s) for segmenting: %s", localSourceFile.Name(), err)
	}
	defer localSourceFile.Close()
	defer os.Remove(localSourceFile.Name()) // Clean up the file as soon as we're done segmenting

	// Copy the file locally because of issues with ffmpeg segmenting and remote files
	// We can be aggressive with the timeout because we're copying from cloud storage
	timeout, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	_, err = clients.CopyFile(timeout, job.SignedSourceURL, localSourceFile.Name(), "", job.RequestID)
	if err != nil {
		return fmt.Errorf("failed to copy file (%s) locally for segmenting: %s", job.SignedSourceURL, err)
	}

	// Begin Segmenting
	log.Log(job.RequestID, "Beginning segmenting via FFMPEG/Livepeer pipeline")
	job.ReportProgress(clients.TranscodeStatusPreparing, 0.5)

	// FFMPEG fails when presented with a raw IP + Path type URL, so we prepend "http://" to it
	internalAddress := config.HTTPInternalAddress
	if !strings.HasPrefix(internalAddress, "http") {
		internalAddress = "http://" + internalAddress
	}

	destinationURL := fmt.Sprintf("%s/api/ffmpeg/%s/index.m3u8", internalAddress, job.StreamName)
	if err := video.Segment(localSourceFile.Name(), destinationURL, job.TargetSegmentSizeSecs); err != nil {
		return err
	}

	return nil
}

func cleanUpLocalTmpFiles(dir string, filenamePattern string, maxAge time.Duration) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Mode().IsRegular() {
			if match, _ := filepath.Match(filenamePattern, info.Name()); match {
				if time.Since(info.ModTime()) > maxAge {
					err = os.Remove(path)
					if err != nil {
						return fmt.Errorf("error removing file %s: %w", path, err)
					}
					log.LogNoRequestID("Cleaned up file", "path", path, "filename", info.Name(), "age", info.ModTime())
				}
			}
		}
		return nil
	})
}

func toStr(URL *url.URL) string {
	if URL != nil {
		return URL.String()
	}
	return ""
}
