package misttriggers

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
)

// TriggerPushEnd responds to PUSH_END trigger
// This trigger is run whenever an outgoing push stops, for any reason.
// This trigger is stream-specific and non-blocking. The payload for this trigger is multiple lines,
// each separated by a single newline character (without an ending newline), containing data:
//
//	push ID (integer)
//	stream name (string)
//	target URI, before variables/triggers affected it (string)
//	target URI, afterwards, as actually used (string)
//	last 10 log messages (JSON array string)
//	most recent push status (JSON object string)
func (d *MistCallbackHandlersCollection) TriggerPushEnd(w http.ResponseWriter, req *http.Request, payload []byte) {
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	if len(lines) != 6 {
		errors.WriteHTTPBadRequest(w, "Bad request payload", fmt.Errorf("unknown payload '%s'", string(payload)))
		return
	}
	// stream name is the second line in the Mist Trigger payload
	streamName := lines[1]
	// TODO: Left commented as these will all be used by the next piece we'll pull out of https://github.com/livepeer/catalyst-api/pull/30
	// destination := lines[2]
	// actualDestination := lines[3]
	// pushStatus := lines[5]

	switch streamNameToPipeline(streamName) {
	case Transcoding:
		// TODO: Left commented for illustration of the alternate code path here as this is the next piece we'll pull out of https://github.com/livepeer/catalyst-api/pull/30
		// d.TranscodingPushEnd(w, req, streamName, destination, actualDestination, pushStatus)
	case Segmenting:
		d.SegmentingPushEnd(w, req, streamName)
	default:
		// Not related to API logic
	}
}

func (d *MistCallbackHandlersCollection) SegmentingPushEnd(w http.ResponseWriter, req *http.Request, streamName string) {
	// when uploading is done, remove trigger and stream from Mist
	defer cache.DefaultStreamCache.Segmenting.Remove(streamName)

	callbackUrl := cache.DefaultStreamCache.Segmenting.GetCallbackUrl(streamName)
	if callbackUrl == "" {
		errors.WriteHTTPBadRequest(w, "PUSH_END trigger invoked for unknown stream: "+streamName, nil)
	}

	// Try to clean up the trigger and stream from Mist. If these fail then we only log, since we still want to do any
	// further cleanup stages and callbacks
	if err := d.MistClient.DeleteTrigger(streamName, TRIGGER_PUSH_END); err != nil {
		_ = config.Logger.Log("msg", "Failed to delete PUSH_END trigger", "err", err.Error(), "stream_name", streamName)
	}
	if err := d.MistClient.DeleteStream(streamName); err != nil {
		_ = config.Logger.Log("msg", "Failed to delete stream", "err", err.Error(), "stream_name", streamName)
	}

	// Let Studio know that we've finished the Segmenting phase
	if err := clients.DefaultCallbackClient.SendTranscodeStatus(callbackUrl, clients.TranscodeStatusPreparingCompleted, 1); err != nil {
		_ = config.Logger.Log("msg", "Failed to send transcode status callback", "err", err.Error(), "stream_name", streamName)
	}

	// TODO: Start Transcoding (stubbed for now with below method)
	stubTranscodingCallbacksForStudio(callbackUrl)
}
