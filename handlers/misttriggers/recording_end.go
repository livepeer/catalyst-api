package misttriggers

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/subprocess"
	"github.com/livepeer/catalyst-api/transcode"
)

// This trigger is run whenever an output to file finishes writing, either through the pushing system (with a file target) or when ran manually.
// It’s purpose is for handling re-encodes or logging of stored files, etcetera.
// This trigger is stream-specific and non-blocking.
//
// The payload for this trigger is multiple lines, each separated by a single newline character (without an ending newline), containing data as such:
// stream name
// path to file that just finished writing
// output protocol name
// number of bytes written to file
// amount of seconds that writing took (NOT duration of stream media data!)
// time of connection start (unix-time)
// time of connection end (unix-time)
// duration of stream media data (milliseconds)
// first media timestamp (milliseconds)
// last media timestamp (milliseconds)
func (d *MistCallbackHandlersCollection) TriggerRecordingEnd(w http.ResponseWriter, req *http.Request, payload []byte) {
	p, err := ParseRecordingEndPayload(string(payload))
	if err != nil {
		errors.WriteHTTPBadRequest(w, "Error parsing RECORDING_END payload", err)
		return
	}

	switch streamNameToPipeline(p.StreamName) {
	case Transcoding:
		// TODO
	case Segmenting:
		d.triggerRecordingEndSegmenting(w, p)
	default:
		// Not related to API logic
	}
}

func (d *MistCallbackHandlersCollection) triggerRecordingEndSegmenting(w http.ResponseWriter, p RecordingEndPayload) {
	// when uploading is done, remove trigger and stream from Mist
	defer cache.DefaultStreamCache.Segmenting.Remove(p.StreamName)

	// Grab the Request ID to enable us to log properly
	requestID := cache.DefaultStreamCache.Segmenting.GetRequestID(p.StreamName)

	callbackUrl := cache.DefaultStreamCache.Segmenting.GetCallbackUrl(p.StreamName)
	if callbackUrl == "" {
		log.Log(requestID, "RECORDING_END trigger invoked for unknown stream")
		return
	}

	// Try to clean up the trigger and stream from Mist. If these fail then we only log, since we still want to do any
	// further cleanup stages and callbacks
	defer func() {
		if err := d.MistClient.DeleteStream(p.StreamName); err != nil {
			log.LogError(requestID, "Failed to delete stream in triggerRecordingEndSegmenting", err)
		}
	}()

	// Let Studio know that we've finished the Segmenting phase
	if err := clients.DefaultCallbackClient.SendTranscodeStatus(callbackUrl, clients.TranscodeStatusPreparingCompleted, 1); err != nil {
		log.LogError(requestID, "Failed to send transcode status callback", err)
	}

	// Get the source stream's detailed track info before kicking off transcode
	// Mist currently returns the "booting" error even after successfully segmenting MOV files
	streamInfo, err := d.MistClient.GetStreamInfo(p.StreamName)
	if err != nil {
		log.LogError(requestID, "Failed to get stream info", err)
		return
	}

	// Compare duration of source stream to the segmented stream to ensure the input file was completely segmented before attempting to transcode
	var inputVideoLengthMillis int64
	for track, trackInfo := range streamInfo.Meta.Tracks {
		if strings.Contains(track, "video") {
			inputVideoLengthMillis = trackInfo.Lastms
		}
	}
	if math.Abs(float64(inputVideoLengthMillis-p.StreamMediaDurationMillis)) > 500 {
		log.Log(requestID, "Input video duration does not match segmented video duration", "input_duration_ms", inputVideoLengthMillis, "segmented_duration_ms", p.StreamMediaDurationMillis)
		return
	}

	si := cache.DefaultStreamCache.Segmenting.Get(p.StreamName)
	transcodeRequest := transcode.TranscodeSegmentRequest{
		SourceFile:       si.SourceFile,
		CallbackURL:      si.CallbackURL,
		AccessToken:      si.AccessToken,
		TranscodeAPIUrl:  si.TranscodeAPIUrl,
		SourceStreamInfo: streamInfo,
		UploadURL:        si.UploadURL,
		RequestID:        requestID,
	}

	go func() {
		inputInfo := clients.InputVideo{
			Format:    "mp4", // hardcoded as mist stream is in dtsc format.
			Duration:  float64(p.StreamMediaDurationMillis) / 1000.0,
			SizeBytes: p.WrittenBytes,
		}
		for _, track := range streamInfo.Meta.Tracks {
			inputInfo.Tracks = append(inputInfo.Tracks, clients.InputTrack{
				Type:         track.Type,
				Codec:        track.Codec,
				Bitrate:      int64(track.Bps * 8),
				DurationSec:  float64(track.Lastms-track.Firstms) / 1000.0,
				StartTimeSec: float64(track.Firstms) / 1000.0,
				VideoTrack: clients.VideoTrack{
					Width:  int64(track.Width),
					Height: int64(track.Height),
					FPS:    float64(track.Fpks) / 1000.0,
				},
				AudioTrack: clients.AudioTrack{
					Channels:   track.Channels,
					SampleRate: track.Rate,
					SampleBits: track.Size,
				},
			})
		}

		outputs, err := transcode.RunTranscodeProcess(transcodeRequest, p.StreamName, inputInfo)
		if err != nil {
			log.LogError(requestID, "RunTranscodeProcess returned an error", err)

			if err := clients.DefaultCallbackClient.SendTranscodeStatusError(callbackUrl, "Transcoding Failed: "+err.Error()); err != nil {
				log.LogError(requestID, "Failed to send Error callback", err)
			}
			return
		}

		// prepare .dtsh headers for all rendition playlists
		for _, output := range outputs {
			// output is multivariant playlist
			err := createDtsh(requestID, output.Manifest)
			if err != nil {
				// should not block the ingestion flow or make it fail on error.
				log.LogError(requestID, "master createDtsh() failed", err, "destination", output.Manifest)
			}
			for _, rendition := range output.Videos {
				// we create dtsh for all rendition playlists
				err := createDtsh(requestID, rendition.Location)
				if err != nil {
					// should not block the ingestion flow or make it fail on error.
					log.LogError(requestID, "createDtsh() failed", err, "destination", rendition.Location)
				}
			}
		}

	}()
}

type RecordingEndPayload struct {
	StreamName                string
	WrittenFilepath           string
	OutputProtocol            string
	WrittenBytes              int
	WritingDurationSecs       int
	ConnectionStartTimeUnix   int
	ConnectionEndTimeUnix     int
	StreamMediaDurationMillis int64
	FirstMediaTimestampMillis int64
	LastMediaTimestampMillis  int64
}

func ParseRecordingEndPayload(payload string) (RecordingEndPayload, error) {
	lines := strings.Split(strings.TrimSuffix(payload, "\n"), "\n")
	if len(lines) != 10 {
		return RecordingEndPayload{}, fmt.Errorf("expected 10 lines in RECORDING_END payload but got %d. Payload: %s", len(lines), payload)
	}

	WrittenBytes, err := strconv.Atoi(lines[3])
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 3, lines[3], err)
	}

	WritingDurationSecs, err := strconv.Atoi(lines[4])
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 4, lines[4], err)
	}

	ConnectionStartTimeUnix, err := strconv.Atoi(lines[5])
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 5, lines[5], err)
	}

	ConnectionEndTimeUnix, err := strconv.Atoi(lines[6])
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 6, lines[6], err)
	}

	StreamMediaDurationMillis, err := strconv.ParseInt(lines[7], 10, 64)
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 7, lines[7], err)
	}

	FirstMediaTimestampMillis, err := strconv.ParseInt(lines[8], 10, 64)
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 8, lines[8], err)
	}

	LastMediaTimestampMillis, err := strconv.ParseInt(lines[9], 10, 64)
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 9, lines[9], err)
	}

	return RecordingEndPayload{
		StreamName:                lines[0],
		WrittenFilepath:           lines[1],
		OutputProtocol:            lines[2],
		WrittenBytes:              WrittenBytes,
		WritingDurationSecs:       WritingDurationSecs,
		ConnectionStartTimeUnix:   ConnectionStartTimeUnix,
		ConnectionEndTimeUnix:     ConnectionEndTimeUnix,
		StreamMediaDurationMillis: StreamMediaDurationMillis,
		FirstMediaTimestampMillis: FirstMediaTimestampMillis,
		LastMediaTimestampMillis:  LastMediaTimestampMillis,
	}, nil
}

func createDtsh(requestID, destination string) error {
	url, err := url.Parse(destination)
	if err != nil {
		return err
	}
	url.RawQuery = ""
	url.Fragment = ""
	headerPrepare := exec.Command(path.Join(config.PathMistDir, "MistInHLS"), "-H", url.String(), "-g", "5")
	if err = subprocess.LogOutputs(headerPrepare); err != nil {
		return err
	}
	if err = headerPrepare.Start(); err != nil {
		return err
	}
	go func() {
		if err := headerPrepare.Wait(); err != nil {
			log.LogError(requestID, "createDtsh return code", err, "destination", destination)
		}
	}()
	return nil
}
