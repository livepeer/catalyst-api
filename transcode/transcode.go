package transcode

import (
	"bufio"
	"bytes"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/video"
)

const UPLOAD_TIMEOUT = 5 * time.Minute
const TRANSMUX_STORAGE_DIR = "/tmp/transmux_stage"

type TranscodeSegmentRequest struct {
	SourceFile        string                 `json:"source_location"`
	CallbackURL       string                 `json:"callback_url"`
	SourceManifestURL string                 `json:"source_manifest_url"`
	HlsTargetURL      string                 `json:"target_url"`
	Mp4TargetUrl      string                 `json:mp4_target_url`
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
	GenerateMP4      bool
}

var LocalBroadcasterClient clients.BroadcasterClient

func init() {
	b, err := clients.NewLocalBroadcasterClient(config.DefaultBroadcasterURL)
	if err != nil {
		panic(fmt.Sprintf("Error initialising Local Broadcaster Client with URL %q: %s", config.DefaultBroadcasterURL, err))
	}
	LocalBroadcasterClient = b
}

func RunTranscodeProcess(transcodeRequest TranscodeSegmentRequest, streamName string, inputInfo video.InputVideo) ([]video.OutputVideo, int, error) {
	log.AddContext(transcodeRequest.RequestID, "source", transcodeRequest.SourceFile, "source_manifest", transcodeRequest.SourceManifestURL, "stream_name", streamName)
	log.Log(transcodeRequest.RequestID, "RunTranscodeProcess (v2) Beginning")

	var segmentsCount = 0

	outputs := []video.OutputVideo{}

	var hlsTargetURL *url.URL
	var err error
	if transcodeRequest.HlsTargetURL != "" {
		hlsTargetURL, err = url.Parse(transcodeRequest.HlsTargetURL)
	} else {
		// TargetURL not defined in the /api/transcode/file request; for the backwards-compatibility, use SourceManifestURL.
		// For the /api/vod endpoint, TargetURL is always defined.
		hlsTargetURL, err = url.Parse(path.Dir(transcodeRequest.SourceManifestURL))
	}

	if err != nil {
		return outputs, segmentsCount, fmt.Errorf("failed to parse transcodeRequest.TargetURL: %s", err)
	}
	tout, err := url.Parse(path.Dir(hlsTargetURL.Path))
	if err != nil {
		return outputs, segmentsCount, fmt.Errorf("failed to parse targetTranscodedPath: %s", err)
	}
	targetTranscodedRenditionOutputURL := hlsTargetURL.ResolveReference(tout)

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

	renditionList := video.TRenditionList{RenditionSegmentTable: make(map[string]*video.TSegmentList)}
	// only populate video.TRenditionList map if MP4 is enabled via override or short-form video detection
	if transcodeRequest.GenerateMP4 {
		for _, profile := range transcodeProfiles {
			renditionList.AddRenditionSegment(profile.Name,
				&video.TSegmentList{
					SegmentDataTable: make(map[int][]byte),
				})
		}
	}

	var jobs *ParallelTranscoding
	jobs = NewParallelTranscoding(sourceSegmentURLs, func(segment segmentInfo) error {
		err := transcodeSegment(segment, streamName, manifestID, transcodeRequest, transcodeProfiles, targetTranscodedRenditionOutputURL, transcodedStats, &renditionList)
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
	manifestURL, err := GenerateAndUploadManifests(sourceManifest, targetTranscodedRenditionOutputURL.String(), transcodedStats)
	if err != nil {
		return outputs, segmentsCount, err
	}

	playbackBaseURL, err := clients.PublishDriverSession(targetTranscodedRenditionOutputURL.String(), tout.Path)
	if err != nil {
		return outputs, segmentsCount, err
	}

	var mp4Outputs []video.OutputVideoFile
	// Transmux received segments from T into a single mp4
	if transcodeRequest.GenerateMP4 {
		mp4TargetUrlBase, err := url.Parse(transcodeRequest.Mp4TargetUrl)
		if err != nil {
			return outputs, segmentsCount, err
		}
		for rendition, segments := range renditionList.RenditionSegmentTable {
			// a. create folder to hold transmux-ed files in local storage temporarily
			err := os.MkdirAll(TRANSMUX_STORAGE_DIR, 0700)
			if err != nil && !os.IsExist(err) {
				log.Log(transcodeRequest.RequestID, "failed to create temp dir for transmuxing", "dir", TRANSMUX_STORAGE_DIR, "err", err)
				return outputs, segmentsCount, err
			}

			// b. create a single .ts file for a given rendition by concatenating all segments in order
			if rendition == "low-bitrate" {
				// skip mp4 generation for low-bitrate profile
				continue
			}
			concatTsFileName := filepath.Join(TRANSMUX_STORAGE_DIR, transcodeRequest.RequestID+"_"+rendition+".ts")
			defer os.Remove(concatTsFileName)
			totalBytes, err := video.ConcatTS(concatTsFileName, segments)
			if err != nil {
				log.Log(transcodeRequest.RequestID, "error concatenating .ts", "file", concatTsFileName, "err", err)
				continue
			}

			// c. Verify the total bytes written for the single .ts file for a given rendition matches the total # of bytes we received from T
			renditionIndex := getProfileIndex(transcodeProfiles, rendition)
			var rendBytesWritten int64 = -1
			for _, v := range transcodedStats {
				if v.Name == rendition {
					rendBytesWritten = v.Bytes
				}
			}
			if rendBytesWritten != totalBytes {
				log.Log(transcodeRequest.RequestID, "bytes written does not match", "file", concatTsFileName, "bytes expected", transcodedStats[renditionIndex].Bytes, "bytes written", totalBytes)
				break
			}

			// d. Transmux the single .ts file into an .mp4 file
			mp4OutputFileName := concatTsFileName[:len(concatTsFileName)-len(filepath.Ext(concatTsFileName))] + ".mp4"
			err = video.MuxTStoMP4(concatTsFileName, mp4OutputFileName)
			if err != nil {
				log.Log(transcodeRequest.RequestID, "error transmuxing", "err", err)
				continue
			}

			// e. Upload mp4 output file
			mp4OutputFile, err := os.Open(mp4OutputFileName)
			defer os.Remove(mp4OutputFileName)
			if err != nil {
				log.Log(transcodeRequest.RequestID, "error opening mp4", "file", mp4OutputFileName, "err", err)
				break
			}

			mp4TargetUrl := mp4TargetUrlBase.JoinPath(rendition, filepath.Base(mp4OutputFile.Name()))
			err = backoff.Retry(func() error {
				return clients.UploadToOSURL(mp4TargetUrl.String(), "", bufio.NewReader(mp4OutputFile), UPLOAD_TIMEOUT)
			}, clients.UploadRetryBackoff())
			if err != nil {
				log.Log(transcodeRequest.RequestID, "failed to upload mp4", "file", mp4OutputFile.Name())
				break
			}

			mp4PlaybackURL := strings.ReplaceAll(mp4TargetUrl.String(), targetTranscodedRenditionOutputURL.String(), playbackBaseURL)
			mp4Out := video.OutputVideoFile{
				Type:     "mp4",
				Location: mp4PlaybackURL,
			}
			signedURL, err := clients.SignURL(mp4TargetUrl)
			if err != nil {
				return outputs, segmentsCount, fmt.Errorf("failed to create signed url for %s: %w", mp4TargetUrl, err)
			}
			mp4Out, err = video.PopulateOutput(video.Probe{}, signedURL, mp4Out)
			if err != nil {
				return outputs, segmentsCount, err
			}

			mp4Outputs = append(mp4Outputs, mp4Out)
		}
	}

	manifestURL = strings.ReplaceAll(manifestURL, targetTranscodedRenditionOutputURL.String(), playbackBaseURL)
	output := video.OutputVideo{Type: "object_store", Manifest: manifestURL}
	for _, rendition := range transcodedStats {
		videoManifestURL := strings.ReplaceAll(rendition.ManifestLocation, targetTranscodedRenditionOutputURL.String(), playbackBaseURL)
		output.Videos = append(output.Videos, video.OutputVideoFile{Location: videoManifestURL, SizeBytes: rendition.Bytes})
	}
	output.MP4Outputs = mp4Outputs
	outputs = []video.OutputVideo{output}
	// Return outputs for .dtsh file creation
	return outputs, segmentsCount, nil
}

func transcodeSegment(
	segment segmentInfo, streamName, manifestID string,
	transcodeRequest TranscodeSegmentRequest,
	transcodeProfiles []video.EncodedProfile,
	targetOSURL *url.URL,
	transcodedStats []*RenditionStats,
	renditionList *video.TRenditionList,
) error {
	start := time.Now()

	var tr clients.TranscodeResult
	err := backoff.Retry(func() error {
		rc, err := clients.DownloadOSURL(segment.Input.URL)
		if err != nil {
			return fmt.Errorf("failed to download source segment %q: %s", segment.Input, err)
		}

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
		return nil
	}, TranscodeRetryBackoff())

	if err != nil {
		return err
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

		if transcodeRequest.GenerateMP4 {
			// get inner segments table from outer rendition table
			segmentsList := renditionList.GetSegmentList(transcodedSegment.Name)
			// add new entry for segment # and corresponding byte stream
			segmentsList.AddSegmentData(segment.Index, transcodedSegment.MediaData)
		}

		err = backoff.Retry(func() error {
			return clients.UploadToOSURL(targetRenditionURL, fmt.Sprintf("%d.ts", segment.Index), bytes.NewReader(transcodedSegment.MediaData), UPLOAD_TIMEOUT)
		}, clients.UploadRetryBackoff())
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

func TranscodeRetryBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(5*time.Second), 10)
}
