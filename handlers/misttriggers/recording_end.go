package misttriggers

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/handlers"
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

	callbackUrl := cache.DefaultStreamCache.Segmenting.GetCallbackUrl(p.StreamName)
	if callbackUrl == "" {
		_ = config.Logger.Log("msg", "RECORDING_END trigger invoked for unknown stream", "stream_name", p.StreamName)
		return
	}

	// Try to clean up the trigger and stream from Mist. If these fail then we only log, since we still want to do any
	// further cleanup stages and callbacks
	if err := d.MistClient.DeleteStream(p.StreamName); err != nil {
		_ = config.Logger.Log("msg", "Failed to delete stream in triggerRecordingEndSegmenting", "err", err.Error(), "stream_name", p.StreamName)
	}

	// Let Studio know that we've finished the Segmenting phase
	if err := clients.DefaultCallbackClient.SendTranscodeStatus(callbackUrl, clients.TranscodeStatusPreparingCompleted, 1); err != nil {
		_ = config.Logger.Log("msg", "Failed to send transcode status callback", "err", err.Error(), "stream_name", p.StreamName)
	}

	// Get the source stream's detailed track info before kicking off transcode
	streamInfo, err := d.MistClient.GetStreamInfo(p.StreamName)
	if err != nil {
		_ = config.Logger.Log("msg", "Failed to get stream info", "err", err.Error(), "stream_name", p.StreamName)
	}

	// Compare duration of source stream to the segmented stream to ensure the input file was completely segmented before attempting to transcode
	var inputVideoLengthMillis int
	for track, trackInfo := range streamInfo.Meta.Tracks {
		if strings.Contains(track, "video") {
			inputVideoLengthMillis = trackInfo.Lastms
		}
	}
	if math.Abs(float64(inputVideoLengthMillis-p.StreamMediaDurationMillis)) > 500 {
		_ = config.Logger.Log("msg", "Input video duration does not match segmented video duration",
			"input video duration (ms):", inputVideoLengthMillis, "segmented video duration (ms):", p.StreamMediaDurationMillis)
		return
	}

	si := cache.DefaultStreamCache.Segmenting.Get(p.StreamName)
	transcodeRequest := handlers.TranscodeSegmentRequest{
		SourceFile:       si.SourceFile,
		CallbackURL:      si.CallbackURL,
		AccessToken:      si.AccessToken,
		TranscodeAPIUrl:  si.TranscodeAPIUrl,
		SourceStreamInfo: streamInfo,
		UploadURL:        si.UploadURL,
	}

	// Create a separate subdirectory for the transcoded renditions
	segmentedUploadURL, err := url.Parse(transcodeRequest.UploadURL)
	if err != nil {
		_ = config.Logger.Log("msg", "failed to parse transcodeRequest.UploadURL", "error", err)
		return
	}
	relativeTranscodeURL, err := url.Parse("transcoded/index.m3u8")
	if err != nil {
		_ = config.Logger.Log("msg", "failed to parse relativeTranscodeURL", "error", err)
		return
	}
	transcodedManifestURL := segmentedUploadURL.ResolveReference(relativeTranscodeURL)

	go func() {
		err := transcode.RunTranscodeProcess(transcodeRequest.UploadURL, transcodedManifestURL.String(), transcodeRequest.Profiles)
		if err != nil {
			_ = config.Logger.Log(
				"msg", "RunTranscodeProcess returned an error",
				"err", err.Error(),
				"stream_name", p.StreamName,
				"source", transcodeRequest.SourceFile,
				"target", transcodeRequest.UploadURL,
			)
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
	StreamMediaDurationMillis int
	FirstMediaTimestampMillis int
	LastMediaTimestampMillis  int
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

	StreamMediaDurationMillis, err := strconv.Atoi(lines[7])
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 7, lines[7], err)
	}

	FirstMediaTimestampMillis, err := strconv.Atoi(lines[8])
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 8, lines[8], err)
	}

	LastMediaTimestampMillis, err := strconv.Atoi(lines[9])
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
