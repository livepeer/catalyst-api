package transcode

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/clients"
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
	SourceOutputURL   string                 `json:"source_output_url"`
	HlsTargetURL      string                 `json:"target_url"`
	Mp4TargetUrl      string                 `json:"mp4_target_url"`
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

	RequestID      string                                 `json:"-"`
	ReportProgress func(clients.TranscodeStatus, float64) `json:"-"`
	GenerateMP4    bool
}

func RunTranscodeProcess(transcodeRequest TranscodeSegmentRequest, streamName string, inputInfo video.InputVideo, broadcaster clients.BroadcasterClient) ([]video.OutputVideo, int, error) {
	log.AddContext(transcodeRequest.RequestID, "source_manifest", transcodeRequest.SourceManifestURL, "stream_name", streamName)
	log.Log(transcodeRequest.RequestID, "RunTranscodeProcess (v2) Beginning")

	var segmentsCount = 0

	var outputs []video.OutputVideo

	hlsTargetURL, err := getHlsTargetURL(transcodeRequest)
	if err != nil {
		return outputs, segmentsCount, err
	}

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
	sourceManifest, err := clients.DownloadRenditionManifest(transcodeRequest.RequestID, sourceManifestOSURL)
	if err != nil {
		return outputs, segmentsCount, fmt.Errorf("error downloading source manifest: %s", err)
	}

	// Generate the full segment URLs from the manifest
	sourceSegmentURLs, err := clients.GetSourceSegmentURLs(sourceManifestOSURL, sourceManifest)
	if err != nil {
		return outputs, segmentsCount, fmt.Errorf("error generating source segment URLs: %s", err)
	}
	log.Log(transcodeRequest.RequestID, "Fetched Source Segments URLs", "num_urls", len(sourceSegmentURLs))

	// The last segment in an HLS manifest may contain an audio-only track - this is common
	// during a livestream recording where the video stream can end sooner with a trailing audio stream
	// which results in a segment at the end that just contains audio. This segment should *not* be
	// submitted to the T.
	lastSegment := sourceSegmentURLs[len(sourceSegmentURLs)-1]
	lastSegmentURL, err := clients.SignURL(lastSegment.URL)
	if err != nil {
		return outputs, segmentsCount, fmt.Errorf("failed to create signed url for last segment %s: %w", lastSegment.URL, err)
	}
	p := video.Probe{}
	// ProbeFile will return err for various reasons so we use the subsequent GetTrack method to check for video tracks
	lastSegmentProbe, _ := p.ProbeFile(transcodeRequest.RequestID, lastSegmentURL)
	// GetTrack will return an err if TrackTypeVideo was not found
	_, err = lastSegmentProbe.GetTrack(video.TrackTypeVideo)
	if err != nil {
		var lastSegmentIdx int
		for i, entry := range sourceManifest.Segments {
			if entry == nil {
				lastSegmentIdx = i - 1
				break
			}
		}
		log.Log(transcodeRequest.RequestID, "last segment in manifest contains an audio-only track", "skipped-segment", lastSegmentIdx)
		// remove the last segment from both the manifest and list of segment URLs
		sourceManifest.Segments[lastSegmentIdx] = nil
		sourceSegmentURLs = sourceSegmentURLs[:len(sourceSegmentURLs)-1]
	}

	// Use RequestID as part of manifestID when talking to the Broadcaster
	manifestID := "manifest-" + transcodeRequest.RequestID
	// transcodedStats hold actual info from transcoded results within requested constraints (this usually differs from requested profiles)
	transcodedStats := statsFromProfiles(transcodeProfiles)

	renditionList := video.TRenditionList{RenditionSegmentTable: make(map[string]*video.TSegmentList)}
	// Only populate video.TRenditionList map if MP4 is enabled via override or short-form video detection.
	// And if the original input file was an HLS video, then only generate an MP4 for the highest bitrate profile.
	var maxBitrate int64
	var maxProfile video.EncodedProfile
	if transcodeRequest.GenerateMP4 {
		if inputInfo.Format == "hls" {
			for _, profile := range transcodeProfiles {
				if profile.Bitrate > maxBitrate {
					maxBitrate = profile.Bitrate
					maxProfile = profile
				}
			}
			renditionList.AddRenditionSegment(maxProfile.Name,
				&video.TSegmentList{
					SegmentDataTable: make(map[int][]byte),
				})
		} else {
			for _, profile := range transcodeProfiles {
				renditionList.AddRenditionSegment(profile.Name,
					&video.TSegmentList{
						SegmentDataTable: make(map[int][]byte),
					})
			}
		}
	}

	var jobs *ParallelTranscoding
	jobs = NewParallelTranscoding(sourceSegmentURLs, func(segment segmentInfo) error {
		err := transcodeSegment(segment, streamName, manifestID, transcodeRequest, transcodeProfiles, hlsTargetURL, transcodedStats, &renditionList, broadcaster)
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
	manifestURL, err := clients.GenerateAndUploadManifests(sourceManifest, hlsTargetURL.String(), transcodedStats)
	if err != nil {
		return outputs, segmentsCount, err
	}

	var mp4OutputsPre []video.OutputVideoFile
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

			filename := fmt.Sprintf("%s.mp4", rendition)
			err = backoff.Retry(func() error {
				return clients.UploadToOSURL(mp4TargetUrlBase.String(), filename, bufio.NewReader(mp4OutputFile), UPLOAD_TIMEOUT)
			}, clients.UploadRetryBackoff())
			if err != nil {
				log.Log(transcodeRequest.RequestID, "failed to upload mp4", "file", mp4OutputFile.Name())
				break
			}

			mp4Out := video.OutputVideoFile{
				Type:     "mp4",
				Location: mp4TargetUrlBase.JoinPath(filename).String(),
			}
			mp4OutputsPre = append(mp4OutputsPre, mp4Out)
		}
	}

	hlsPlaybackBaseURL, mp4PlaybackBaseURL, err := clients.Publish(hlsTargetURL.String(), transcodeRequest.Mp4TargetUrl)
	if err != nil {
		return outputs, segmentsCount, err
	}

	var mp4Outputs []video.OutputVideoFile
	if transcodeRequest.GenerateMP4 {
		for _, mp4Out := range mp4OutputsPre {
			mp4Out.Location = strings.ReplaceAll(mp4Out.Location, transcodeRequest.Mp4TargetUrl, mp4PlaybackBaseURL)

			mp4TargetUrl, err := url.Parse(mp4Out.Location)
			if err != nil {
				return outputs, segmentsCount, fmt.Errorf("failed to parse mp4Out.Location %s: %w", mp4Out.Location, err)
			}

			var probeURL string
			if mp4TargetUrl.Scheme == "ipfs" {
				// probe IPFS with web3.storage URL, since ffprobe does not support "ipfs://"
				probeURL = fmt.Sprintf("https://%s.ipfs.w3s.link/%s", mp4TargetUrl.Host, mp4TargetUrl.Path)
			} else {
				var err error
				probeURL, err = clients.SignURL(mp4TargetUrl)
				if err != nil {
					return outputs, segmentsCount, fmt.Errorf("failed to create signed url for %s: %w", mp4TargetUrl, err)
				}
			}

			mp4Out, err = video.PopulateOutput(transcodeRequest.RequestID, video.Probe{}, probeURL, mp4Out)
			if err != nil {
				return outputs, segmentsCount, err
			}

			mp4Outputs = append(mp4Outputs, mp4Out)
		}
	}

	var manifest string
	if transcodeRequest.HlsTargetURL != "" {
		manifest = strings.ReplaceAll(manifestURL, hlsTargetURL.String(), hlsPlaybackBaseURL)
	} else {
		manifest = strings.ReplaceAll(manifestURL, hlsTargetURL.String(), mp4PlaybackBaseURL)
	}
	output := video.OutputVideo{Type: "object_store", Manifest: manifest}
	if transcodeRequest.HlsTargetURL != "" {
		for _, rendition := range transcodedStats {
			videoManifestURL := strings.ReplaceAll(rendition.ManifestLocation, hlsTargetURL.String(), hlsPlaybackBaseURL)
			output.Videos = append(output.Videos, video.OutputVideoFile{Location: videoManifestURL, SizeBytes: rendition.Bytes})
		}
	}
	output.MP4Outputs = mp4Outputs
	outputs = []video.OutputVideo{output}
	// Return outputs for .dtsh file creation
	return outputs, segmentsCount, nil
}

// getHlsTargetURL extracts URL for storing rendition HLS segments.
// If HLS output is requested, then the URL from the VOD request is used
// If HLS output is not requested, then the URL from source_output flag is used
func getHlsTargetURL(tsr TranscodeSegmentRequest) (*url.URL, error) {
	if tsr.HlsTargetURL != "" {
		hlsTargetURL, err := url.Parse(tsr.HlsTargetURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse transcodeRequest.TargetURL: %s", err)
		}
		return hlsTargetURL, nil
	} else {
		sourceOutputURL, err := url.Parse(tsr.SourceOutputURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse transcodeRequest.SourceOutputURL: %s", err)
		}
		return sourceOutputURL.JoinPath("rendition"), nil
	}
}

func transcodeSegment(
	segment segmentInfo, streamName, manifestID string,
	transcodeRequest TranscodeSegmentRequest,
	transcodeProfiles []video.EncodedProfile,
	targetOSURL *url.URL,
	transcodedStats []*video.RenditionStats,
	renditionList *video.TRenditionList,
	broadcaster clients.BroadcasterClient,
) error {
	start := time.Now()

	var tr clients.TranscodeResult
	err := backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), clients.MaxCopyFileDuration)
		defer cancel()
		rc, err := clients.GetFile(ctx, transcodeRequest.RequestID, segment.Input.URL.String(), nil)
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
			tr, err = broadcaster.TranscodeSegment(rc, int64(segment.Index), transcodeProfiles, segment.Input.DurationMillis, manifestID)
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
			if segmentsList != nil {
				// add new entry for segment # and corresponding byte stream if the profile
				// exists in the renditionList which contains only profiles for which mp4s will
				// be generated i.e. all profiles for mp4 inputs and only highest quality
				// rendition for hls inputs like recordings.
				segmentsList.AddSegmentData(segment.Index, transcodedSegment.MediaData)
			}
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
	Input clients.SourceSegment
	Index int
}

func statsFromProfiles(profiles []video.EncodedProfile) []*video.RenditionStats {
	stats := []*video.RenditionStats{}
	for _, profile := range profiles {
		stats = append(stats, &video.RenditionStats{
			Name:   profile.Name,
			Width:  profile.Width,  // TODO: extract this from actual media retrieved from B
			Height: profile.Height, // TODO: extract this from actual media retrieved from B
			FPS:    profile.FPS,    // TODO: extract this from actual media retrieved from B
		})
	}
	return stats
}

func TranscodeRetryBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(5*time.Second), 10)
}
