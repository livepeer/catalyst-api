package transcode

import (
	"bytes"
	"fmt"
	"io"
	"net/url"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
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

func RunTranscodeProcess(transcodeRequest TranscodeSegmentRequest, streamName string, durationMillis int64) error {

	// Create a separate subdirectory for the transcoded renditions
	segmentedUploadURL, err := url.Parse(transcodeRequest.UploadURL)
	if err != nil {
		return fmt.Errorf("failed to parse transcodeRequest.UploadURL: %s", err)
	}
	relativeTranscodeURL, err := url.Parse("transcoded/index.m3u8")
	if err != nil {
		return fmt.Errorf("failed to parse relativeTranscodeURL: %s", err)
	}
	targetManifestOSURL := segmentedUploadURL.ResolveReference(relativeTranscodeURL)
	// Grab some useful parameters to be used later from the TranscodeSegmentRequest
	sourceManifestOSURL := transcodeRequest.UploadURL
	transcodeProfiles := transcodeRequest.Profiles
	callbackURL := transcodeRequest.CallbackURL

	_ = config.Logger.Log("msg", "RunTranscodeProcess (v2) Beginning", "source", sourceManifestOSURL, "target", targetManifestOSURL)

	// If Profiles haven't been overridden, use the default set
	if len(transcodeProfiles) == 0 {
		transcodeProfiles = defaultTranscodeProfiles
	}

	// Download the "source" manifest that contains all the segments we'll be transcoding
	sourceManifest, err := DownloadRenditionManifest(sourceManifestOSURL)
	if err != nil {
		return fmt.Errorf("error downloading source manifest: %s", err)
	}

	// Generate the full segment URLs from the manifest
	sourceSegmentURLs, err := GetSourceSegmentURLs(sourceManifestOSURL, sourceManifest)
	if err != nil {
		return fmt.Errorf("error generating source segment URLs: %s", err)
	}

	// Iterate through the segment URLs and transcode them
	for i, u := range sourceSegmentURLs {
		rc, err := clients.DownloadOSURL(u)
		if err != nil {
			return fmt.Errorf("failed to download source segment %q: %s", u, err)
		}

		// Download and read the segment, log the size in bytes and discard for now
		buf := &bytes.Buffer{}
		nRead, err := io.Copy(buf, rc)
		if err != nil {
			return fmt.Errorf("failed to read source segment data %q: %s", u, err)
		}
		_ = config.Logger.Log("msg", "downloaded source segment", "url", u, "size_bytes", nRead, "error", err)

		// If an AccessToken is provided via the request for transcode, then use remote Broadcasters.
		// Otherwise, use the local harcoded Broadcaster.
		if transcodeRequest.AccessToken != "" {
			creds := clients.Credentials{
				AccessToken:  transcodeRequest.AccessToken,
				CustomAPIURL: transcodeRequest.TranscodeAPIUrl,
			}
			broadcasterClient, _ := clients.NewRemoteBroadcasterClient(creds)

			tr, err := broadcasterClient.TranscodeSegmentWithRemoteBroadcaster(buf, int64(i), transcodeProfiles, streamName, durationMillis)
			if err != nil {
				return fmt.Errorf("failed to run TranscodeSegmentWithRemoteBroadcaster: %s", err)
			}
			fmt.Println("transcodeResult", tr) //remove this
			// TODO: Upload the output segments
		} else {
			broadcasterClient, _ := clients.NewLocalBroadcasterClient(config.DefaultBroadcasterURL)
			tr, err := broadcasterClient.TranscodeSegment(buf, int64(i), transcodeProfiles, durationMillis)
			if err != nil {
				return fmt.Errorf("failed to run TranscodeSegment: %s", err)
			}
			fmt.Println("transcodeResult", tr) //remove this
			// TODO: Upload the output segments
		}

		var completedRatio = calculateCompletedRatio(len(sourceSegmentURLs), i+1)
		if err = clients.DefaultCallbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, completedRatio); err != nil {
			_ = config.Logger.Log("msg", "failed to send transcode status callback", "url", callbackURL, "error", err)
		}
	}

	// Build the manifests and push them to storage
	err = GenerateAndUploadManifests(sourceManifest, targetManifestOSURL.String(), transcodeProfiles)
	if err != nil {
		return err
	}

	return nil
}

func calculateCompletedRatio(totalSegments, completedSegments int) float64 {
	return (1 / float64(totalSegments)) * float64(completedSegments)
}
