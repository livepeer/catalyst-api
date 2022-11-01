package transcode

import (
	"bytes"
	"fmt"
	"net/url"

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

// The default set of encoding profiles to use when none are specified
var defaultTranscodeProfiles = []clients.EncodedProfile{
	{
		Name:    "720p",
		Bitrate: 2000000,
		FPS:     30,
		Width:   1280,
		Height:  720,
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

	// Create a separate subdirectory for the transcoded renditions
	segmentedUploadURL, err := url.Parse(transcodeRequest.UploadURL)
	if err != nil {
		return outputs, fmt.Errorf("failed to parse transcodeRequest.UploadURL: %s", err)
	}
	relativeTranscodeURL, err := url.Parse("transcoded/")
	if err != nil {
		return outputs, fmt.Errorf("failed to parse relativeTranscodeURL: %s", err)
	}

	targetOSURL := segmentedUploadURL.ResolveReference(relativeTranscodeURL)
	// Grab some useful parameters to be used later from the TranscodeSegmentRequest
	sourceManifestOSURL := transcodeRequest.UploadURL
	transcodeProfiles := transcodeRequest.Profiles
	callbackURL := transcodeRequest.CallbackURL

	// If Profiles haven't been overridden, use the default set
	if len(transcodeProfiles) == 0 {
		transcodeProfiles = defaultTranscodeProfiles
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

	// Iterate through the segment URLs and transcode them
	for segmentIndex, u := range sourceSegmentURLs {
		rc, err := clients.DownloadOSURL(u.URL)
		if err != nil {
			return outputs, fmt.Errorf("failed to download source segment %q: %s", u, err)
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
			// Get renditions from remote broadcaster
			tr, err = broadcasterClient.TranscodeSegmentWithRemoteBroadcaster(rc, int64(segmentIndex), transcodeProfiles, streamName, u.DurationMillis)
			if err != nil {
				return outputs, fmt.Errorf("failed to run TranscodeSegmentWithRemoteBroadcaster: %s", err)
			}
		} else {
			// Get renditions from local broadcaster
			tr, err = localBroadcasterClient.TranscodeSegment(rc, int64(segmentIndex), transcodeProfiles, u.DurationMillis, manifestID)
			if err != nil {
				return outputs, fmt.Errorf("failed to run TranscodeSegment: %s", err)
			}
		}
		// Store renditions
		for _, transcodedSegment := range tr.Renditions {
			renditionIndex := getProfileIndex(transcodeProfiles, transcodedSegment.Name)
			if renditionIndex == -1 {
				return outputs, fmt.Errorf("failed to find profile with name %q while parsing rendition segment", transcodedSegment.Name)
			}

			relativeRenditionPath := fmt.Sprintf("rendition-%d/", renditionIndex)
			relativeRenditionURL, err := url.Parse(relativeRenditionPath)
			if err != nil {
				return outputs, fmt.Errorf("error building rendition segment URL %q: %s", relativeRenditionPath, err)
			}
			renditionURL := targetOSURL.ResolveReference(relativeRenditionURL)

			err = clients.UploadToOSURL(renditionURL.String(), fmt.Sprintf("%d.ts", segmentIndex), bytes.NewReader(transcodedSegment.MediaData))
			if err != nil {
				return outputs, fmt.Errorf("failed to upload master playlist: %s", err)
			}
		}

		var completedRatio = calculateCompletedRatio(len(sourceSegmentURLs), segmentIndex+1)
		if err = clients.DefaultCallbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, completedRatio); err != nil {
			log.LogError(transcodeRequest.RequestID, "failed to send transcode status callback", err, "url", callbackURL)
		}
	}

	// Build the manifests and push them to storage
	manifestManifestURL, err := GenerateAndUploadManifests(sourceManifest, targetOSURL.String(), transcodeProfiles)
	if err != nil {
		return outputs, err
	}

	outputs = []clients.OutputVideo{
		{
			Type:     "google-s3",
			Manifest: manifestManifestURL,
		},
	}
	// Send the success callback
	err = clients.DefaultCallbackClient.SendTranscodeStatusCompleted(callbackURL, inputInfo, outputs)
	if err != nil {
		log.LogError(transcodeRequest.RequestID, "Failed to send TranscodeStatusCompleted callback", err, "url", callbackURL)
	}
	// Return outputs for .dtsh file creation
	return outputs, nil
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
