package transcode

import (
	"bytes"
	"fmt"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
)

type TranscodeSegmentRequest struct {
	SourceFile      string                   `json:"source_location"`
	CallbackURL     string                   `json:"callback_url"`
	UploadURL       string                   `json:"upload_url"`
	StreamKey       string                   `json:"streamKey"`
	AccessToken     string                   `json:"accessToken"`
	TranscodeAPIUrl string                   `json:"transcodeAPIUrl"`
	Profiles        []clients.EncodedProfile `json:"profiles"`
	Detection       struct {
		Freq                uint `json:"freq"`
		SampleRate          uint `json:"sampleRate"`
		SceneClassification []struct {
			Name string `json:"name"`
		} `json:"sceneClassification"`
	} `json:"detection"`
	SourceStreamInfo clients.MistStreamInfo
	RequestID        string
}

var MAX_DEFAULT_RENDITION_WIDTH = int64(1280)
var MAX_DEFAULT_RENDITION_HEIGHT = int64(720)

// The default set of encoding profiles to use when none are specified
var defaultTranscodeProfiles = []clients.EncodedProfile{
	{
		Name:    "720p",
		Bitrate: 2000000,
		FPS:     30,
		Width:   MAX_DEFAULT_RENDITION_WIDTH,
		Height:  MAX_DEFAULT_RENDITION_HEIGHT,
	},
	{
		Name:    "360p",
		Bitrate: 500000,
		FPS:     30,
		Width:   640,
		Height:  360,
	},
}

var localBroadcasterClient clients.BroadcasterClient

func init() {
	b, err := clients.NewLocalBroadcasterClient(config.DefaultBroadcasterURL)
	if err != nil {
		panic(fmt.Sprintf("Error initialising Local Broadcaster Client with URL %q: %s", config.DefaultBroadcasterURL, err))
	}
	localBroadcasterClient = b
}

func RunTranscodeProcess(transcodeRequest TranscodeSegmentRequest, streamName string, inputInfo clients.InputVideo) ([]clients.OutputVideo, error) {
	log.AddContext(transcodeRequest.RequestID, "source", transcodeRequest.SourceFile, "target", transcodeRequest.UploadURL, "stream_name", streamName)
	log.Log(transcodeRequest.RequestID, "RunTranscodeProcess (v2) Beginning")

	outputs := []clients.OutputVideo{}

	// Parse the manifest destination of the segmented output specified in the request
	segmentedOutputManifestURL, err := url.Parse(transcodeRequest.UploadURL)
	if err != nil {
		return outputs, fmt.Errorf("failed to parse transcodeRequest.UploadURL: %s", err)
	}
	// Go back to the root directory to set as the output for transcode renditions
	targetTranscodedPath := path.Dir(path.Dir(segmentedOutputManifestURL.Path))
	// Use the same manifest filename that was used for the segmented manifest
	targetTranscodedManifestFilename := path.Base(segmentedOutputManifestURL.String())
	// Generate the new output path of the transcoded manifest
	targetTranscodedOutputPath := path.Join(targetTranscodedPath, targetTranscodedManifestFilename)
	// Generate the manifest output URL from the manifest output path (e.g. s3+https://USER:PASS@storage.googleapis.com/user/hls/index.m3u8)
	tpath, err := url.Parse(targetTranscodedOutputPath)
	if err != nil {
		return outputs, fmt.Errorf("failed to parse targetTranscodedOutputPath: %s", err)
	}
	targetTranscodedOutputURL := segmentedOutputManifestURL.ResolveReference(tpath)
	fmt.Println(targetTranscodedOutputURL)
	// Generate the rendition output URL (e.g. s3+https://USER:PASS@storage.googleapis.com/user/hls/)
	tout, err := url.Parse(targetTranscodedPath)
	if err != nil {
		return outputs, fmt.Errorf("failed to parse targetTranscodedPath: %s", err)
	}
	targetTranscodedRenditionOutputURL := segmentedOutputManifestURL.ResolveReference(tout)

	// Grab some useful parameters to be used later from the TranscodeSegmentRequest
	sourceManifestOSURL := transcodeRequest.UploadURL
	// transcodeProfiles are desired constraints for transcoding process
	transcodeProfiles := transcodeRequest.Profiles

	// If Profiles haven't been overridden, use the default set
	if len(transcodeProfiles) == 0 {
		transcodeProfiles = defaultTranscodeProfiles
		if isInputVideoBiggerThanDefaults(inputInfo) {
			videoTrack, err := inputInfo.GetVideoTrack()
			if err != nil {
				return outputs, fmt.Errorf("error finding a video track: %s", err)
			}
			transcodeProfiles = append(defaultTranscodeProfiles, clients.EncodedProfile{
				Name:    "source",
				Bitrate: videoTrack.Bitrate,
				FPS:     int64(videoTrack.FPS),
				Width:   videoTrack.Width,
				Height:  videoTrack.Height,
			})
		}
	}

	// Download the "source" manifest that contains all the segments we'll be transcoding
	sourceManifest, err := DownloadRenditionManifest(sourceManifestOSURL)
	if err != nil {
		return outputs, fmt.Errorf("error downloading source manifest: %s", err)
	}

	// Generate the full segment URLs from the manifest
	sourceSegmentURLs, err := GetSourceSegmentURLs(sourceManifestOSURL, sourceManifest)
	if err != nil {
		return outputs, fmt.Errorf("error generating source segment URLs: %s", err)
	}

	// Generate a unique ID to use when talking to the Broadcaster
	manifestID := "manifest-" + config.RandomTrailer(8)
	// transcodedStats hold actual info from transcoded results within requested constraints (this usually differs from requested profiles)
	transcodedStats := statsFromProfiles(transcodeProfiles)

	// Iterate through the segment URLs and transcode them
	// Use channel to queue segments
	queue := make(chan segmentInfo, len(sourceSegmentURLs))
	for segmentIndex, u := range sourceSegmentURLs {
		queue <- segmentInfo{Input: u, Index: segmentIndex}
	}
	close(queue)
	// Use channel for recieving the errors
	errors := make(chan error, 100)
	// Start number of workers in parallel
	var completed sync.WaitGroup
	completed.Add(config.TranscodingParallelJobs)
	for index := 0; index < config.TranscodingParallelJobs; index++ {
		go func() {
			defer completed.Done()
			for segment := range queue {
				err := transcodeSegment(segment, streamName, manifestID, transcodeRequest, transcodeProfiles, targetTranscodedRenditionOutputURL, transcodedStats)
				if err != nil {
					errors <- err
					return
				}
				var completedRatio = calculateCompletedRatio(len(sourceSegmentURLs), segment.Index+1)
				if err = clients.DefaultCallbackClient.SendTranscodeStatus(transcodeRequest.CallbackURL, clients.TranscodeStatusTranscoding, completedRatio); err != nil {
					log.LogError(transcodeRequest.RequestID, "failed to send transcode status callback", err, "url", transcodeRequest.CallbackURL)
				}
			}
		}()
		// Add some desync interval to avoid load spikes on segment-encode-end
		time.Sleep(713 * time.Millisecond)
	}
	// Wait for all segments to transcode or first error
	select {
	case <-channelFromWaitgroup(&completed):
	case err = <-errors:
		return outputs, err
	}

	// Build the manifests and push them to storage
	manifestManifestURL, err := GenerateAndUploadManifests(sourceManifest, targetTranscodedRenditionOutputURL.String(), transcodedStats)
	if err != nil {
		return outputs, err
	}

	output := clients.OutputVideo{Type: "object_store", Manifest: manifestManifestURL}
	for _, rendition := range transcodedStats {
		output.Videos = append(output.Videos, clients.OutputVideoFile{Location: rendition.ManifestLocation})
	}
	outputs = []clients.OutputVideo{output}
	// Send the success callback
	err = clients.DefaultCallbackClient.SendTranscodeStatusCompleted(transcodeRequest.CallbackURL, inputInfo, outputs)
	if err != nil {
		log.LogError(transcodeRequest.RequestID, "Failed to send TranscodeStatusCompleted callback", err, "url", transcodeRequest.CallbackURL)
	}
	// Return outputs for .dtsh file creation
	return outputs, nil
}

func transcodeSegment(segment segmentInfo, streamName, manifestID string, transcodeRequest TranscodeSegmentRequest, transcodeProfiles []clients.EncodedProfile, targetOSURL *url.URL, transcodedStats []RenditionStats) error {
	rc, err := clients.DownloadOSURL(segment.Input.URL)
	if err != nil {
		return fmt.Errorf("failed to download source segment %q: %s", segment.Input, err)
	}
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
		tr, err = localBroadcasterClient.TranscodeSegment(rc, int64(segment.Index), transcodeProfiles, segment.Input.DurationMillis, manifestID)
		if err != nil {
			return fmt.Errorf("failed to run TranscodeSegment: %s", err)
		}
	}
	for _, transcodedSegment := range tr.Renditions {
		renditionIndex := getProfileIndex(transcodeProfiles, transcodedSegment.Name)
		if renditionIndex == -1 {
			return fmt.Errorf("failed to find profile with name %q while parsing rendition segment", transcodedSegment.Name)
		}

		targetRenditionURL, err := url.JoinPath(targetOSURL.String(), fmt.Sprintf("rendition-%d/", renditionIndex))
		if err != nil {
			return fmt.Errorf("error building rendition segment URL %q: %s", targetRenditionURL, err)
		}

		err = clients.UploadToOSURL(targetRenditionURL, fmt.Sprintf("%d.ts", segment.Index), bytes.NewReader(transcodedSegment.MediaData))
		if err != nil {
			return fmt.Errorf("failed to upload master playlist: %s", err)
		}
		// bitrate calculation
		transcodedStats[renditionIndex].Bytes += int64(len(transcodedSegment.MediaData))
		transcodedStats[renditionIndex].DurationMs += float64(segment.Input.DurationMillis)
	}

	for _, stats := range transcodedStats {
		stats.BitsPerSecond = uint32(float64(stats.Bytes) * 8000.0 / float64(stats.DurationMs))
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

func isInputVideoBiggerThanDefaults(iv clients.InputVideo) bool {
	for _, t := range iv.Tracks {
		if t.Type == "video" {
			if t.Width > MAX_DEFAULT_RENDITION_WIDTH {
				return true
			}
			if t.Height > MAX_DEFAULT_RENDITION_HEIGHT {
				return true
			}
		}
	}
	return false
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

func statsFromProfiles(profiles []clients.EncodedProfile) []RenditionStats {
	stats := []RenditionStats{}
	for _, profile := range profiles {
		stats = append(stats, RenditionStats{
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
