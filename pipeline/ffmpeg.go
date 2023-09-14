package pipeline

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/transcode"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
)

const LocalSourceFilePattern = "sourcevideo*"

type ffmpeg struct {
	// The base of where to output source segments to
	SourceOutputURL *url.URL
	// Broadcaster for local transcoding
	Broadcaster         clients.BroadcasterClient
	probe               video.Prober
	sourcePlaybackHosts map[string]string
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

	sourceOutputURL := f.SourceOutputURL.JoinPath(job.RequestID)
	segmentingTargetURL := sourceOutputURL.JoinPath(config.SEGMENTING_SUBDIR, config.SEGMENTING_TARGET_MANIFEST)

	job.SegmentingTargetURL = segmentingTargetURL.String()
	log.AddContext(job.RequestID, "segmented_url", job.SegmentingTargetURL)
	job.ReportProgress(clients.TranscodeStatusPreparing, 0.3)

	// Segment only for non-HLS inputs
	if job.InputFileInfo.Format != "hls" {
		if err := copyFileToLocalTmpAndSegment(job); err != nil {
			return nil, err
		}
	} else {
		job.SegmentingTargetURL = job.SourceFile
	}
	job.SegmentingDone = time.Now()
	f.sendSourcePlayback(job)
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
		SourceOutputURL:   sourceOutputURL.String(),
		HlsTargetURL:      toStr(job.HlsTargetURL),
		Mp4TargetUrl:      toStr(job.Mp4TargetURL),
		FragMp4TargetUrl:  toStr(job.FragMp4TargetURL),
		RequestID:         job.RequestID,
		ReportProgress:    job.ReportProgress,
		GenerateMP4:       job.GenerateMP4,
	}

	inputInfo := video.InputVideo{
		Format:    job.InputFileInfo.Format,
		Duration:  job.InputFileInfo.Duration,
		SizeBytes: job.sourceBytes,
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

	sourceManifest, err := clients.DownloadRenditionManifest(transcodeRequest.RequestID, transcodeRequest.SourceManifestURL)
	if err != nil {
		return nil, fmt.Errorf("error downloading source manifest: %s", err)
	}

	sourceSegments := sourceManifest.GetAllSegments()
	job.sourceSegments = len(sourceSegments)
	err = f.probeSourceSegments(job, sourceSegments)
	if err != nil {
		return nil, err
	}

	outputs, transcodedSegments, err := transcode.RunTranscodeProcess(transcodeRequest, job.StreamName, inputInfo, f.Broadcaster)
	if err != nil {
		log.LogError(job.RequestID, "RunTranscodeProcess returned an error", err)
		return nil, fmt.Errorf("transcoding failed: %w", err)
	}

	job.TranscodingDone = time.Now()
	job.transcodedSegments = transcodedSegments

	return &HandlerOutput{
		Result: &UploadJobResult{
			InputVideo: inputInfo,
			Outputs:    outputs,
		}}, nil
}

var sourcePlaybackBucketBlocklist = []string{"lp-us-catalyst-vod-pvt-monster", "lp-us-catalyst-vod-pvt-com"}

func (f *ffmpeg) sendSourcePlayback(job *JobInfo) {
	segmentingTargetURL, err := url.Parse(job.SegmentingTargetURL)
	if err != nil {
		log.LogError(job.RequestID, "unable to parse url for source playback", err)
		return
	}

	sourceURL, err := url.Parse(job.SourceFile)
	if err != nil {
		log.LogError(job.RequestID, "unable to parse source url for source playback", err)
		return
	}

	prefix := ""
	if clients.IsHLSInput(sourceURL) {
		for k, v := range f.sourcePlaybackHosts {
			if strings.HasPrefix(job.SourceFile, k) {
				prefix = strings.Replace(job.SourceFile, k, v, 1)
				break
			}
		}
		if prefix == "" {
			log.Log(job.RequestID, "no source playback prefix found", "host", sourceURL.Host)
			return
		}
	}

	segmentingPath := strings.Split(segmentingTargetURL.Path, "/")
	if len(segmentingPath) < 3 || segmentingPath[1] == "" {
		log.Log(job.RequestID, "unable to find bucket for source playback", "segmentingTargetURL", segmentingTargetURL)
		return
	}
	// assume bucket is second element in slice (first element should be an empty string as the path has a leading slash)
	segmentingBucket := segmentingPath[1]
	if (job.HlsTargetURL == nil || !strings.Contains(job.HlsTargetURL.String(), "/"+segmentingBucket+"/")) && prefix == "" {
		log.Log(job.RequestID, "source playback not available, not a studio job", "segmentingTargetURL", segmentingTargetURL)
		return
	}

	// source playback won't currently work for token gating so we're excluding the private buckets here
	for _, blocked := range sourcePlaybackBucketBlocklist {
		if segmentingBucket == blocked {
			log.Log(job.RequestID, "source playback not available, not main bucket")
			return
		}
	}

	sourceMaster := m3u8.NewMasterPlaylist()
	videoTrack, err := job.InputFileInfo.GetTrack(video.TrackTypeVideo)
	if err != nil {
		log.LogError(job.RequestID, "unable to find a video track for source playback", err)
		return
	}

	if prefix == "" {
		prefix = "/" + path.Join(segmentingPath[2:]...)
	}
	sourceMaster.Append(prefix, &m3u8.MediaPlaylist{}, m3u8.VariantParams{
		Bandwidth:  uint32(videoTrack.Bitrate),
		Resolution: fmt.Sprintf("%dx%d", videoTrack.Width, videoTrack.Height),
		Name:       fmt.Sprintf("%dp", videoTrack.Height),
	})
	err = clients.UploadToOSURLFields(job.HlsTargetURL.String(), "index.m3u8", sourceMaster.Encode(), 10*time.Minute, &drivers.FileProperties{CacheControl: "max-age=60"})
	if err != nil {
		log.LogError(job.RequestID, "failed to write source playback playlist", err)
		return
	}

	sourcePlaylist := job.HlsTargetURL.JoinPath("index.m3u8").String()
	sourceOutput := video.OutputVideo{
		Manifest: sourcePlaylist,
	}
	tsm := clients.NewTranscodeStatusSourcePlayback(job.CallbackURL, job.RequestID, clients.TranscodeStatusPreparingCompleted, 1, &sourceOutput)
	err = job.statusClient.SendTranscodeStatus(tsm)
	if err != nil {
		log.LogError(job.RequestID, "failed to send status message for source playback", err)
		return
	}
	job.SourcePlaybackDone = time.Now()
}

func (f *ffmpeg) probeSourceSegments(job *JobInfo, sourceSegments []*m3u8.MediaSegment) error {
	if job.InputFileInfo.Format == "hls" {
		return nil
	}
	segCount := len(sourceSegments)
	if segCount < 4 {
		for _, segment := range sourceSegments {
			if err := f.probeSourceSegment(job.RequestID, segment, job.SegmentingTargetURL); err != nil {
				return err
			}
		}
		return nil
	}
	segmentsToCheck := []int{0, 1, segCount - 2, segCount - 1}
	for _, i := range segmentsToCheck {
		if err := f.probeSourceSegment(job.RequestID, sourceSegments[i], job.SegmentingTargetURL); err != nil {
			return err
		}
	}
	return nil
}

func (f *ffmpeg) probeSourceSegment(requestID string, seg *m3u8.MediaSegment, sourceManifestURL string) error {
	u, err := clients.ManifestURLToSegmentURL(sourceManifestURL, seg.URI)
	if err != nil {
		return fmt.Errorf("error checking source segments: %w", err)
	}
	probeURL, err := clients.SignURL(u)
	if err != nil {
		return fmt.Errorf("failed to create signed url for %s: %w", u, err)
	}
	_, err = f.probe.ProbeFile(requestID, probeURL)
	if err != nil {
		return fmt.Errorf("probe failed for segment %s: %w", u, err)
	}

	// check for audio issues https://linear.app/livepeer/issue/VID-287/audio-missing-after-segmenting
	_, err = f.probe.ProbeFile(requestID, probeURL, "-loglevel", "warning")
	if err != nil && strings.Contains(err.Error(), "no TS found at start of file, duration not set") {
		return fmt.Errorf("probe failed for segment %s: %w", u, err)
	}
	return nil
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
