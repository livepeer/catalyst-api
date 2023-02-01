package transcode

import (
	"bytes"
	"context"
	"fmt"
	"github.com/livepeer/go-tools/drivers"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/video"
)

const UPLOAD_TIMEOUT = 5 * time.Minute

type TranscodeSegmentRequest struct {
	SourceFile        string                 `json:"source_location"`
	CallbackURL       string                 `json:"callback_url"`
	SourceManifestURL string                 `json:"source_manifest_url"`
	TargetURL         string                 `json:"target_url,omitempty"`
	StreamKey         string                 `json:"streamKey"`
	AccessToken       string                 `json:"accessToken"`
	TranscodeAPIUrl   string                 `json:"transcodeAPIUrl"`
	Profiles          []video.EncodedProfile `json:"profiles"`
	Detection         struct {
		Freq                uint `json:"freq"`
		SampleRate          uint `json:"sampleRate"`
		SceneClassification []struct {
			Name string `json:"name"`
		} `json:"sceneClassification"`
	} `json:"detection"`

	SourceStreamInfo clients.MistStreamInfo                 `json:"-"`
	RequestID        string                                 `json:"-"`
	ReportProgress   func(clients.TranscodeStatus, float64) `json:"-"`
}

var LocalBroadcasterClient clients.BroadcasterClient

func init() {
	b, err := clients.NewLocalBroadcasterClient(config.DefaultBroadcasterURL)
	if err != nil {
		panic(fmt.Sprintf("Error initialising Local Broadcaster Client with URL %q: %s", config.DefaultBroadcasterURL, err))
	}
	LocalBroadcasterClient = b
}

func RunTranscodeProcess(transcodeRequest TranscodeSegmentRequest, streamName string, inputInfo video.InputVideo) ([]clients.OutputVideo, int, error) {
	log.AddContext(transcodeRequest.RequestID, "source", transcodeRequest.SourceFile, "source_manifest", transcodeRequest.SourceManifestURL, "stream_name", streamName)
	log.Log(transcodeRequest.RequestID, "RunTranscodeProcess (v2) Beginning")

	var segmentsCount = 0

	outputs := []clients.OutputVideo{}

	targetURL, err := url.Parse(transcodeRequest.TargetURL)
	if err != nil {
		return outputs, segmentsCount, fmt.Errorf("failed to parse transcodeRequest.TargetURL: %s", err)
	}
	tout, err := url.Parse(path.Dir(targetURL.Path))
	if err != nil {
		return outputs, segmentsCount, fmt.Errorf("failed to parse targetTranscodedPath: %s", err)
	}
	targetTranscodedRenditionOutputURL := targetURL.ResolveReference(tout)

	// Grab some useful parameters to be used later from the TranscodeSegmentRequest
	sourceManifestOSURL := transcodeRequest.SourceManifestURL
	// transcodeProfiles are desired constraints for transcoding process
	transcodeProfiles := transcodeRequest.Profiles

	// If Profiles haven't been overridden, use the default set
	if len(transcodeProfiles) == 0 {
		transcodeProfiles, err = video.GetPlaybackProfiles(inputInfo)
		if err != nil {
			return outputs, segmentsCount, fmt.Errorf("failed to get playback profiles: %w", err)
		}
	}

	// Download the "source" manifest that contains all the segments we'll be transcoding
	sourceManifest, err := DownloadRenditionManifest(sourceManifestOSURL)
	if err != nil {
		return outputs, segmentsCount, fmt.Errorf("error downloading source manifest: %s", err)
	}

	// Generate the full segment URLs from the manifest
	sourceSegmentURLs, err := GetSourceSegmentURLs(sourceManifestOSURL, sourceManifest)
	if err != nil {
		return outputs, segmentsCount, fmt.Errorf("error generating source segment URLs: %s", err)
	}

	// Use RequestID as part of manifestID when talking to the Broadcaster
	manifestID := "manifest-" + transcodeRequest.RequestID
	// transcodedStats hold actual info from transcoded results within requested constraints (this usually differs from requested profiles)
	transcodedStats := statsFromProfiles(transcodeProfiles)

	var jobs *ParallelTranscoding
	jobs = NewParallelTranscoding(sourceSegmentURLs, func(segment segmentInfo) error {
		err := transcodeSegment(segment, streamName, manifestID, transcodeRequest, transcodeProfiles, targetTranscodedRenditionOutputURL, transcodedStats)
		segmentsCount++
		if err != nil {
			return err
		}
		if jobs.IsRunning() && transcodeRequest.ReportProgress != nil {
			// Sending callback only if we are still running
			var completedRatio = calculateCompletedRatio(jobs.GetTotalCount(), jobs.GetCompletedCount()+1)
			transcodeRequest.ReportProgress(clients.TranscodeStatusTranscoding, completedRatio)
		}
		return nil
	})
	jobs.Start()
	if err = jobs.Wait(); err != nil {
		// return first error to caller
		return outputs, segmentsCount, err
	}

	// Build the manifests and push them to storage
	manifestManifestURL, err := GenerateAndUploadManifests(sourceManifest, targetTranscodedRenditionOutputURL.String(), transcodedStats)
	if err != nil {
		return outputs, segmentsCount, err
	}

	playbackBaseURL := PublishDriverSession(targetTranscodedRenditionOutputURL.String(), tout.Path)

	output := clients.OutputVideo{Type: "object_store", Manifest: strings.ReplaceAll(manifestManifestURL, targetTranscodedRenditionOutputURL.String(), playbackBaseURL)}
	for _, rendition := range transcodedStats {
		output.Videos = append(output.Videos, clients.OutputVideoFile{Location: strings.ReplaceAll(rendition.ManifestLocation, targetTranscodedRenditionOutputURL.String(), playbackBaseURL), SizeBytes: int(rendition.Bytes)})
	}
	outputs = []clients.OutputVideo{output}
	// Return outputs for .dtsh file creation
	return outputs, segmentsCount, nil
}

func PublishDriverSession(osUrl string, relPath string) string {
	osDriver, err := drivers.ParseOSURL(osUrl, true)
	if err != nil {
		return ""
	}
	videoUrl, err := osDriver.Publish(context.Background())
	if err == drivers.ErrNotSupported {
		return osUrl
	} else if err == nil {
		res, _ := url.JoinPath(videoUrl, relPath)
		return res
	}
	return ""
}

func transcodeSegment(
	segment segmentInfo, streamName, manifestID string,
	transcodeRequest TranscodeSegmentRequest,
	transcodeProfiles []video.EncodedProfile,
	targetOSURL *url.URL,
	transcodedStats []*RenditionStats,
) error {
	rc, err := clients.DownloadOSURL(segment.Input.URL)
	if err != nil {
		return fmt.Errorf("failed to download source segment %q: %s", segment.Input, err)
	}

	start := time.Now()

	var tr clients.TranscodeResult
	// If an AccessToken is provided via the request for transcode, then use remote Broadcasters.
	// Otherwise, use the local harcoded Broadcaster.
	if transcodeRequest.AccessToken != "" {
		creds := clients.Credentials{
			AccessToken:  transcodeRequest.AccessToken,
			CustomAPIURL: transcodeRequest.TranscodeAPIUrl,
		}
		broadcasterClient, _ := clients.NewRemoteBroadcasterClient(creds)
		// TODO: failed to run TranscodeSegmentWithRemoteBroadcaster: CreateStream(): http POST(https://origin.livepeer.com/api/stream) returned 422 422 Unprocessable Entity
		tr, err = broadcasterClient.TranscodeSegmentWithRemoteBroadcaster(rc, int64(segment.Index), transcodeProfiles, streamName, segment.Input.DurationMillis)
		if err != nil {
			return fmt.Errorf("failed to run TranscodeSegmentWithRemoteBroadcaster: %s", err)
		}
	} else {
		tr, err = LocalBroadcasterClient.TranscodeSegment(rc, int64(segment.Index), transcodeProfiles, segment.Input.DurationMillis, manifestID)
		if err != nil {
			return fmt.Errorf("failed to run TranscodeSegment: %s", err)
		}
	}

	duration := time.Since(start)
	metrics.Metrics.TranscodeSegmentDurationSec.Observe(duration.Seconds())

	for _, transcodedSegment := range tr.Renditions {
		renditionIndex := getProfileIndex(transcodeProfiles, transcodedSegment.Name)
		if renditionIndex == -1 {
			return fmt.Errorf("failed to find profile with name %q while parsing rendition segment", transcodedSegment.Name)
		}

		targetRenditionURL, err := url.JoinPath(targetOSURL.String(), transcodedSegment.Name)
		if err != nil {
			return fmt.Errorf("error building rendition segment URL %q: %s", targetRenditionURL, err)
		}

		err = clients.UploadToOSURL(targetRenditionURL, fmt.Sprintf("%d.ts", segment.Index), bytes.NewReader(transcodedSegment.MediaData), UPLOAD_TIMEOUT)
		if err != nil {
			return fmt.Errorf("failed to upload master playlist: %s", err)
		}
		// bitrate calculation
		transcodedStats[renditionIndex].Bytes += int64(len(transcodedSegment.MediaData))
		transcodedStats[renditionIndex].DurationMs += float64(segment.Input.DurationMillis)
	}

	for _, stats := range transcodedStats {
		stats.BitsPerSecond = uint32(float64(stats.Bytes) * 8.0 / float64(stats.DurationMs/1000))
	}

	return nil
}

func getProfileIndex(transcodeProfiles []video.EncodedProfile, profile string) int {
	for i, p := range transcodeProfiles {
		if p.Name == profile {
			return i
		}
	}
	return -1
}

func calculateCompletedRatio(totalSegments, completedSegments int) float64 {
	return (1 / float64(totalSegments)) * float64(completedSegments)
}

func channelFromWaitgroup(wg *sync.WaitGroup) chan bool {
	completed := make(chan bool)
	go func() {
		wg.Wait()
		close(completed)
	}()
	return completed
}

type segmentInfo struct {
	Input SourceSegment
	Index int
}

func statsFromProfiles(profiles []video.EncodedProfile) []*RenditionStats {
	stats := []*RenditionStats{}
	for _, profile := range profiles {
		stats = append(stats, &RenditionStats{
			Name:   profile.Name,
			Width:  profile.Width,  // TODO: extract this from actual media retrieved from B
			Height: profile.Height, // TODO: extract this from actual media retrieved from B
			FPS:    profile.FPS,    // TODO: extract this from actual media retrieved from B
		})
	}
	return stats
}

type RenditionStats struct {
	Name             string
	Width            int64
	Height           int64
	FPS              int64
	Bytes            int64
	DurationMs       float64
	ManifestLocation string
	BitsPerSecond    uint32
}
