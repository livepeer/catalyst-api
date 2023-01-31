package transcode

import (
	"bytes"
	"context"
	"fmt"
	"github.com/livepeer/go-tools/drivers"
	"net/url"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
)

type TranscodeSegmentRequest struct {
	SourceFile        string                   `json:"source_location"`
	CallbackURL       string                   `json:"callback_url"`
	SourceManifestURL string                   `json:"source_manifest_url"`
	TargetURL         string                   `json:"target_url,omitempty"`
	StreamKey         string                   `json:"streamKey"`
	AccessToken       string                   `json:"accessToken"`
	TranscodeAPIUrl   string                   `json:"transcodeAPIUrl"`
	Profiles          []clients.EncodedProfile `json:"profiles"`
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

const (
	MIN_VIDEO_BITRATE            = 100_000
	ABSOLUTE_MIN_VIDEO_BITRATE   = 5_000
	MAX_DEFAULT_RENDITION_WIDTH  = 1280
	MAX_DEFAULT_RENDITION_HEIGHT = 720
)

// The default set of encoding profiles to use when none are specified
var defaultTranscodeProfiles = []clients.EncodedProfile{
	// {
	// 	Name:    "240p0",
	// 	FPS:     0,
	// 	Bitrate: 250_000,
	// 	Width:   426,
	// 	Height:  240,
	// },
	{
		Name:    "360p0",
		FPS:     0,
		Bitrate: 800_000,
		Width:   640,
		Height:  360,
	},
	// {
	// 	Name:    "480p0",
	// 	FPS:     0,
	// 	Bitrate: 1_600_000,
	// 	Width:   854,
	// 	Height:  480,
	// },
	{
		Name:    "720p0",
		FPS:     0,
		Bitrate: 3_000_000,
		Width:   1280,
		Height:  720,
	},
}

var LocalBroadcasterClient clients.BroadcasterClient

func init() {
	b, err := clients.NewLocalBroadcasterClient(config.DefaultBroadcasterURL)
	if err != nil {
		panic(fmt.Sprintf("Error initialising Local Broadcaster Client with URL %q: %s", config.DefaultBroadcasterURL, err))
	}
	LocalBroadcasterClient = b
}

func RunTranscodeProcess(transcodeRequest TranscodeSegmentRequest, streamName string, inputInfo clients.InputVideo) ([]clients.OutputVideo, int, error) {
	log.AddContext(transcodeRequest.RequestID, "source", transcodeRequest.SourceFile, "source_manifest", transcodeRequest.SourceManifestURL, "stream_name", streamName)
	log.Log(transcodeRequest.RequestID, "RunTranscodeProcess (v2) Beginning")

	var segmentsCount = 0

	outputs := []clients.OutputVideo{}

	// TODO: Add private key
	// Generate the rendition output URL (e.g. s3+https://USER:PASS@storage.googleapis.com/user/hls/)
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
		transcodeProfiles, err = getPlaybackProfiles(inputInfo)
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

	// TODO: Generate manifest for W3S
	// Build the manifests and push them to storage
	manifestManifestURL, err := GenerateAndUploadManifests(sourceManifest, targetTranscodedRenditionOutputURL.String(), transcodedStats)
	if err != nil {
		return outputs, segmentsCount, err
	}

	// TODO
	osDriver, _ := drivers.ParseOSURL(targetTranscodedRenditionOutputURL.String(), true)
	videoUrl, err := osDriver.Publish(context.TODO())
	if err == nil && videoUrl != "" {
		manifestManifestURL, _ = url.JoinPath(videoUrl, tout.Path, "index.m3u8")
	}

	// TODO: Generate all manifests
	// TODO: Make sure credentials from w3 are not returned
	output := clients.OutputVideo{Type: "object_store", Manifest: manifestManifestURL}
	//for _, rendition := range transcodedStats {
	//	output.Videos = append(output.Videos, clients.OutputVideoFile{Location: rendition.ManifestLocation, SizeBytes: int(rendition.Bytes)})
	//}
	outputs = []clients.OutputVideo{output}
	// Return outputs for .dtsh file creation
	return outputs, segmentsCount, nil
}

func transcodeSegment(
	segment segmentInfo, streamName, manifestID string,
	transcodeRequest TranscodeSegmentRequest,
	transcodeProfiles []clients.EncodedProfile,
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

		err = clients.UploadToOSURL(targetRenditionURL, fmt.Sprintf("%d.ts", segment.Index), bytes.NewReader(transcodedSegment.MediaData), time.Minute)
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

func getProfileIndex(transcodeProfiles []clients.EncodedProfile, profile string) int {
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

func getPlaybackProfiles(iv clients.InputVideo) ([]clients.EncodedProfile, error) {
	video, err := iv.GetVideoTrack()
	if err != nil {
		return nil, fmt.Errorf("no video track found in input video: %w", err)
	}
	profiles := make([]clients.EncodedProfile, 0, len(defaultTranscodeProfiles)+1)
	for _, profile := range defaultTranscodeProfiles {
		// transcoding job will adjust the width to match aspect ratio. no need to
		// check it here.
		lowerQualityThanSrc := profile.Height <= video.Height && profile.Bitrate < video.Bitrate
		if lowerQualityThanSrc {
			profiles = append(profiles, profile)
		}
	}
	if len(profiles) == 0 {
		profiles = []clients.EncodedProfile{lowBitrateProfile(video)}
	}
	profiles = append(profiles, clients.EncodedProfile{
		Name:    strconv.FormatInt(int64(video.Height), 10) + "p0",
		Bitrate: video.Bitrate,
		FPS:     0,
		Width:   video.Width,
		Height:  video.Height,
	})
	return profiles, nil
}

func lowBitrateProfile(video clients.InputTrack) clients.EncodedProfile {
	bitrate := int64(float64(video.Bitrate) * (1.0 / 2.0))
	if bitrate < MIN_VIDEO_BITRATE && video.Bitrate > MIN_VIDEO_BITRATE {
		bitrate = MIN_VIDEO_BITRATE
	} else if bitrate < ABSOLUTE_MIN_VIDEO_BITRATE {
		bitrate = ABSOLUTE_MIN_VIDEO_BITRATE
	}
	return clients.EncodedProfile{
		Name:    "low-bitrate",
		FPS:     0,
		Bitrate: bitrate,
		Width:   video.Width,
		Height:  video.Height,
	}
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

func statsFromProfiles(profiles []clients.EncodedProfile) []*RenditionStats {
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
