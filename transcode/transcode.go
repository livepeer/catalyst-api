package transcode

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	c2pa2 "github.com/livepeer/catalyst-api/c2pa"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/video"
)

const (
	UploadTimeout      = 5 * time.Minute
	TransmuxStorageDir = "/tmp/transmux_stage"
)

type TranscodeSegmentRequest struct {
	SourceFile        string                 `json:"source_location"`
	CallbackURL       string                 `json:"callback_url"`
	SourceManifestURL string                 `json:"source_manifest_url"`
	SourceOutputURL   string                 `json:"source_output_url"`
	HlsTargetURL      string                 `json:"target_url"`
	Mp4TargetUrl      string                 `json:"mp4_target_url"`
	FragMp4TargetUrl  string                 `json:"fragmented_mp4_target_url"`
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
	C2PA           *c2pa2.C2PA                            `json:"-"`
	LocalSourceTmp string                                 `json:"-"`
	GenerateMP4    bool
	IsClip         bool
}

type TranscodedSegmentInfo struct {
	RequestID     string
	RenditionName string
	SegmentIndex  int
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
	transcodeProfiles, err := video.SetTranscodeProfiles(inputInfo, transcodeRequest.Profiles)
	if err != nil || len(transcodeProfiles) == 0 {
		return outputs, segmentsCount, fmt.Errorf("failed to set playback profiles: %w", err)
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
	// Only populate video.TRenditionList map if MP4/FragmentedMP4 is enabled or short-form video detection.
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

	// Create a buffered channel for segment information
	segmentChannel := make(chan TranscodedSegmentInfo, 10) // Buffer size of 10

	var wg sync.WaitGroup

	var jobs *ParallelTranscoding
	jobs = NewParallelTranscoding(sourceSegmentURLs, func(segment segmentInfo) error {
		err := transcodeSegment(segment, streamName, manifestID, transcodeRequest, transcodeProfiles, hlsTargetURL, transcodedStats, &renditionList, broadcaster, segmentChannel)

		wg.Add(1)       // Increment the WaitGroup counter
		defer wg.Done() // Decrement the counter when the function exits

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

	if transcodeRequest.GenerateMP4 {
		// Create folder to hold transmux-ed files in local storage temporarily
		TransmuxStorageDir, err := os.MkdirTemp(os.TempDir(), "transmux_stage_")
		if err != nil && !os.IsExist(err) {
			log.Log(transcodeRequest.RequestID, "failed to create temp dir for transmuxing", "dir", TransmuxStorageDir, "err", err)
			return outputs, segmentsCount, err
		}
		defer os.RemoveAll(TransmuxStorageDir)

		// Start the disk-writing goroutine
		go func(transmuxTopLevelDir string, renditionList *video.TRenditionList) {
			var segmentBatch []TranscodedSegmentInfo

			for segInfo := range segmentChannel {
				segmentBatch = append(segmentBatch, segInfo)
				if len(segmentBatch) >= 1 {
					writeSegmentsToDisk(transmuxTopLevelDir, renditionList, segmentBatch)
					fmt.Println("XXX: writing to disk here because > 10", segInfo.RenditionName, segInfo.SegmentIndex)
					//access and delete via mutexes
					segmentBatch = nil
				}
			}
			// Handle any remaining segments after the channel is closed
			if len(segmentBatch) > 0 {
				writeSegmentsToDisk(transmuxTopLevelDir, renditionList, segmentBatch)
				fmt.Println("XXX: writing to disk here after channel close")
			}
		}(TransmuxStorageDir, &renditionList)
	}

	jobs.Start()
	if err = jobs.Wait(); err != nil {
		// return first error to caller
		return outputs, segmentsCount, err
	}

	// Wait for all transcoding jobs to complete
	wg.Wait()
	// Close the channel to signal that no more segments will be sent
	close(segmentChannel)

	fmt.Println("XXX: DONE")
return outputs, segmentsCount, err

	// Build the manifests and push them to storage
	manifestURL, err := clients.GenerateAndUploadManifests(sourceManifest, hlsTargetURL.String(), transcodedStats, transcodeRequest.IsClip)
	if err != nil {
		return outputs, segmentsCount, err
	}

	var mp4OutputsPre []video.OutputVideoFile
	var fmp4ManifestUrls []string
	// Transmux received segments from T into a single mp4
	if transcodeRequest.GenerateMP4 {
		// Check if we should generate a standard MP4, fragmented MP4, or both.
		mp4TargetUrlBase, enableStandardMp4, err := getMp4OutputType(transcodeRequest.Mp4TargetUrl)
		if err != nil {
			return outputs, segmentsCount, err
		}
		fragMp4TargetUrlBase, enableFragMp4, err := getMp4OutputType(transcodeRequest.FragMp4TargetUrl)
		if err != nil {
			return outputs, segmentsCount, err
		}
		if !(enableStandardMp4 || enableFragMp4) {
			return outputs, segmentsCount, fmt.Errorf("a valid mp4 or fragmented-mp4 URL must be provided since MP4 output was requested")
		}

		var concatFiles []string
		for rendition, segments := range renditionList.RenditionSegmentTable {
			// Create folder to hold transmux-ed files in local storage temporarily
			TransmuxStorageDir, err := os.MkdirTemp(os.TempDir(), "transmux_stage_")
			if err != nil && !os.IsExist(err) {
				log.Log(transcodeRequest.RequestID, "failed to create temp dir for transmuxing", "dir", TransmuxStorageDir, "err", err)
				return outputs, segmentsCount, err
			}
			defer os.RemoveAll(TransmuxStorageDir)

			// Create a single .ts file for a given rendition by concatenating all segments in order
			if rendition == "low-bitrate" {
				// skip mp4 generation for low-bitrate profile
				continue
			}
			concatTsFileName := filepath.Join(TransmuxStorageDir, transcodeRequest.RequestID+"_"+rendition+".ts")
			concatFiles = append(concatFiles, concatTsFileName)
			defer os.Remove(concatTsFileName)

			// For now, use the stream based concat for clipping only and file based concat for everything else.
			// Eventually, all mp4 generation can be moved to stream based concat once proven effective.
			var totalBytes int64
			if transcodeRequest.IsClip {
				totalBytes, err = video.ConcatTS(concatTsFileName, segments, true)
			} else {
				totalBytes, err = video.ConcatTS(concatTsFileName, segments, false)
			}
			if err != nil {
				log.Log(transcodeRequest.RequestID, "error concatenating .ts", "file", concatTsFileName, "err", err)
				continue
			}

			// Verify the total bytes written for the single .ts file for a given rendition matches the total # of bytes we received from T
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

			// Mux the .ts file to generate either a regular MP4 (w/ faststart) or fMP4 packaged with HLS/DASH
			if enableStandardMp4 {
				// Transmux the single .ts file into an mp4 file
				mp4OutputFileName := concatTsFileName[:len(concatTsFileName)-len(filepath.Ext(concatTsFileName))] + ".mp4"
				defer os.Remove(mp4OutputFileName)
				standardMp4OutputFiles, err := video.MuxTStoMP4(concatTsFileName, mp4OutputFileName)
				if err != nil {
					log.Log(transcodeRequest.RequestID, "error transmuxing to regular mp4", "file", mp4OutputFileName, "err", err)
					continue
				}

				// Add C2PA Signature
				if transcodeRequest.C2PA != nil {
					for _, f := range standardMp4OutputFiles {
						if err := transcodeRequest.C2PA.SignFile(f, f, rendition, transcodeRequest.LocalSourceTmp); err != nil {
							log.LogError(transcodeRequest.RequestID, "error signing C2PA manifest", err, "file", f)
						}
					}
				}

				// Upload the mp4 file
				mp4Out, err := uploadMp4Files(mp4TargetUrlBase, standardMp4OutputFiles, rendition)
				if err != nil {
					return outputs, segmentsCount, fmt.Errorf("error uploading transmuxed standard mp4 file: %s", err)
				}
				mp4OutputsPre = append(mp4OutputsPre, mp4Out...)
			}
		}

		if enableFragMp4 {
			fmp4OutputDir := filepath.Join(TransmuxStorageDir, transcodeRequest.RequestID+"_fmp4")
			fmp4ManifestOutputFile := filepath.Join(fmp4OutputDir, clients.DashManifestFilename)
			err := video.MuxTStoFMP4(fmp4ManifestOutputFile, concatFiles...)
			if err != nil {
				return outputs, segmentsCount, fmt.Errorf("error transmuxing to fmp4: %w", err)
			}
			// Upload the fragmented-mp4 file(s) and related manifests
			fragMp4TargetBaseOutput := fragMp4TargetUrlBase.JoinPath(clients.Fmp4PostfixDir)
			entries, err := os.ReadDir(fmp4OutputDir)
			if err != nil {
				return outputs, segmentsCount, fmt.Errorf("error listing dir for fragmented mp4 file(s): %w", err)
			}
			var files []string
			for _, entry := range entries {
				files = append(files, filepath.Join(fmp4OutputDir, entry.Name()))
			}
			_, err = uploadMp4Files(fragMp4TargetBaseOutput, files, "")
			if err != nil {
				return outputs, segmentsCount, fmt.Errorf("error uploading transmuxed fragmented mp4 file(s): %w", err)
			}

			fmp4ManifestUrls = append(fmp4ManifestUrls,
				fragMp4TargetBaseOutput.JoinPath(clients.DashManifestFilename).String(),
				fragMp4TargetBaseOutput.JoinPath("master.m3u8").String(),
			)
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
			// Ignore fmp4 manifest files (also ends in .m3u8) since probing these files doesn't reveal much
			// and ffprobe can either fail or take a long time instead.
			fileExt := filepath.Ext(mp4Out.Location)
			if fileExt != ".mp4" && fileExt != ".m4s" {
				continue
			}
			// Generate signed URLs of all mp4 and fmp4 files to probe
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
			// Populate OutputVideo structs with results from probing step to send back in final response to Studio
			mp4Out, err = video.PopulateOutput(transcodeRequest.RequestID, video.Probe{}, probeURL, mp4Out)
			if err != nil {
				return outputs, segmentsCount, fmt.Errorf("failed to populate output for %s: %w", probeURL, err)
			}
			mp4Outputs = append(mp4Outputs, mp4Out)
		}
		// If fmp4 manifest urls were generated in the fmp4 generation stage, then append the master playlist as an additional output
		if len(fmp4ManifestUrls) > 0 {
			for _, u := range fmp4ManifestUrls {
				mp4Outputs = append(mp4Outputs, video.OutputVideoFile{Type: "fmp4-master-playlist", Location: u})
			}
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

func writeSegmentsToDisk(transmuxTopLevelDir string, renditionList *video.TRenditionList, segmentBatch []TranscodedSegmentInfo) (int64, error) {
	for _, segInfo := range segmentBatch {

		segmentList := renditionList.GetSegmentList(segInfo.RenditionName)
		segmentData := segmentList.GetSegment(segInfo.SegmentIndex)
		segmentFilename := filepath.Join(transmuxTopLevelDir, segInfo.RequestID+"_"+segInfo.RenditionName+"_"+strconv.Itoa(segInfo.SegmentIndex)+".ts")
		segmentFile, err := os.Create(segmentFilename)
		if err != nil {
			return 0, fmt.Errorf("error creating .ts file to write transcoded segment data err: %w", err)
		}
		defer segmentFile.Close()
		_, err = segmentFile.Write(segmentData)
		if err != nil {
			return 0, fmt.Errorf("error writing segment err: %w", err)
		}

	}
	return 0, nil
}

func uploadMp4Files(basePath *url.URL, mp4OutputFiles []string, prefix string) ([]video.OutputVideoFile, error) {
	var mp4OutputsPre []video.OutputVideoFile
	// e. Upload all mp4 related output files
	for _, o := range mp4OutputFiles {
		var filename string
		mp4OutputFile, err := os.Open(o)
		defer os.Remove(o)
		if err != nil {
			return []video.OutputVideoFile{}, fmt.Errorf("failed to open %s to upload: %s", o, err)
		}
		if prefix != "" {
			filename = fmt.Sprintf("%s.mp4", prefix)
		} else {
			filename = filepath.Base(mp4OutputFile.Name())
		}
		err = backoff.Retry(func() error {
			return clients.UploadToOSURL(basePath.String(), filename, bufio.NewReader(mp4OutputFile), UploadTimeout)
		}, clients.UploadRetryBackoff())
		if err != nil {
			return []video.OutputVideoFile{}, fmt.Errorf("failed to upload %s: %s", mp4OutputFile.Name(), err)
		}

		mp4Out := video.OutputVideoFile{
			Type:     "mp4",
			Location: basePath.JoinPath(filename).String(),
		}
		mp4OutputsPre = append(mp4OutputsPre, mp4Out)
	}
	return mp4OutputsPre, nil
}

// getMp4OutputType checks the target url of the MP4 or Fragmented-MP4
// output location and returns an *url.URL along with a boolean to
// indicate that specific output type (mp4 or f-mp4) has been enabled.
func getMp4OutputType(targetUrl string) (*url.URL, bool, error) {
	if len(targetUrl) != 0 {
		targetUrlBase, err := url.Parse(targetUrl)
		if err != nil {
			return nil, false, err
		} else {
			return targetUrlBase, true, nil
		}
	}
	return nil, false, nil
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
	segmentChannel chan<- TranscodedSegmentInfo,
) error {
	start := time.Now()

	transcodeConf := clients.LivepeerTranscodeConfiguration{
		TimeoutMultiplier: 10,
	}
	// If this is a request to transcode a Clip source input, then
	// force T to do a re-init of transcoder after segment at idx=0.
	// This is required because the segment at idx=0 is a locally
	// re-encoded segment and the following segment at idx=1 is a
	// source recorded segment. Without a re-init of the transcoder,
	// the different encoding between the two segments causes the
	// transcode operation to incorrectly tag the output segment as
	// having two video tracks.
	if transcodeRequest.IsClip && (int64(segment.Index) == 0 || segment.IsLastSegment) {
		transcodeConf.ForceSessionReinit = true
	} else {
		transcodeConf.ForceSessionReinit = false
	}
	// This is a temporary workaround that implements the same logic
	// as the previous if block -- a new manifestID will force a
	// T session re-init between segment at index=0 and index=1.
	if transcodeRequest.IsClip && (int64(segment.Index) == 0 || segment.IsLastSegment) {
		manifestID = manifestID + "_clip"
	}

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
			tr, err = broadcaster.TranscodeSegment(rc, int64(segment.Index), transcodeProfiles, segment.Input.DurationMillis, manifestID, transcodeConf)
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

				segmentChannel <- TranscodedSegmentInfo{
					RequestID:     transcodeRequest.RequestID,
					RenditionName: transcodedSegment.Name, // Use actual rendition name
					SegmentIndex:  segment.Index,          // Use actual segment index
				}

			}
		}

		err = backoff.Retry(func() error {
			return clients.UploadToOSURL(targetRenditionURL, fmt.Sprintf("%d.ts", segment.Index), bytes.NewReader(transcodedSegment.MediaData), UploadTimeout)
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
	Input         clients.SourceSegment
	Index         int
	IsLastSegment bool
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
