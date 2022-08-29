package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/errors"
)

type MistCallbackHandlersCollection struct {
	MistClient  MistAPIClient
	StreamCache *StreamCache
}

// Trigger dispatches request to mapped method according to trigger name
// Only single trigger callback is allowed on Mist.
// All created streams and our handlers (segmenting, transcoding, et.) must share this endpoint.
// If handler logic grows more complicated we may consider adding dispatch mechanism here.
func (d *MistCallbackHandlersCollection) Trigger() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			Err("", "Cannot read payload", err, w)
			return
		}
		whichOne := req.Header.Get("X-Trigger")
		switch whichOne {
		case "PUSH_END":
			d.TriggerPushEnd(w, req, payload)
		case "LIVE_TRACK_LIST":
			d.TriggerLiveTrackList(w, req, payload)
		default:
			errors.WriteHTTPBadRequest(w, "Unsupported X-Trigger", fmt.Errorf("unknown trigger '%s'", whichOne))
			return
		}
	}
}

// TriggerLiveTrackList responds to LIVE_TRACK_LIST trigger.
// It is stream-specific and must be blocking. The payload for this trigger is multiple lines,
// each separated by a single newline character (without an ending newline), containing data:
//   stream name
//   push target URI
// TriggerLiveTrackList is used only by transcoding.
func (d *MistCallbackHandlersCollection) TriggerLiveTrackList(w http.ResponseWriter, req *http.Request, payload []byte) {
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	streamName := lines[0]
	encodedTracks := lines[1]

	yes, suffix := isTranscodeStream(streamName)
	if !yes {
		Err(streamName, "unknown streamName format", nil, w)
		return
	}

	if streamEnded := encodedTracks == "null"; streamEnded {
		// SOURCE_PREFIX stream is no longer needed
		inputStream := fmt.Sprintf("%s%s", SOURCE_PREFIX, suffix)
		if err := d.MistClient.DeleteStream(inputStream); err != nil {
			log.Printf("ERROR LIVE_TRACK_LIST DeleteStream(%s) %v", inputStream, err)
		}
		// Multiple pushes from RENDITION_PREFIX are in progress.
		return
	}
	tracks := make(LiveTrackListTriggerJson)
	if err := json.Unmarshal([]byte(encodedTracks), &tracks); err != nil {
		Err(streamName, "LiveTrackListTriggerJson json decode error", err, w)
		return
	}
	// Start push per each video track
	info, err := d.StreamCache.Transcoding.Get(streamName)
	if err != nil {
		Err(streamName, "LIVE_TRACK_LIST unknown push source", err, w)
		return
	}
	for i := range tracks { // i is generated name, not important, all info is contained in element
		if tracks[i].Type != "video" {
			// Only produce an rendition per each video track, selecting best audio track
			continue
		}
		destination := fmt.Sprintf("%s/%s__%dx%d.ts?video=%d&audio=maxbps", info.UploadDir, streamName, tracks[i].Width, tracks[i].Height, tracks[i].Index) //.Id)
		if err := d.MistClient.PushStart(streamName, destination); err != nil {
			log.Printf("> ERROR push to %s %v", destination, err)
		} else {
			d.StreamCache.Transcoding.AddDestination(streamName, destination)
		}
	}
}

// TriggerPushEnd responds to PUSH_END trigger
// This trigger is run whenever an outgoing push stops, for any reason.
// This trigger is stream-specific and non-blocking. The payload for this trigger is multiple lines,
// each separated by a single newline character (without an ending newline), containing data:
//   push ID (integer)
//   stream name (string)
//   target URI, before variables/triggers affected it (string)
//   target URI, afterwards, as actually used (string)
//   last 10 log messages (JSON array string)
//   most recent push status (JSON object string)
func (d *MistCallbackHandlersCollection) TriggerPushEnd(w http.ResponseWriter, req *http.Request, payload []byte) {
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	if len(lines) != 6 {
		errors.WriteHTTPBadRequest(w, "Bad request payload", fmt.Errorf("unknown payload '%s'", string(payload)))
		return
	}
	// stream name is the second line in the Mist Trigger payload
	streamName := lines[1]
	destination := lines[2]
	actualDestination := lines[3]
	pushStatus := lines[5]
	if yes, _ := isTranscodeStream(streamName); yes {
		d.TranscodingPushEnd(w, req, streamName, destination, actualDestination, pushStatus)
	} else {
		d.SegmentingPushEnd(w, req, streamName)
	}
}

func (d *MistCallbackHandlersCollection) TranscodingPushEnd(w http.ResponseWriter, req *http.Request, streamName, destination, actualDestination, pushStatus string) {
	info, err := d.StreamCache.Transcoding.Get(streamName)
	if err != nil {
		Err(streamName, "unknown push source", err, w)
		return
	}
	isBeingProcessed := false
	for i := 0; i < len(info.Destinations); i++ {
		if info.Destinations[i] == destination {
			isBeingProcessed = true
			break
		}
	}
	if !isBeingProcessed {
		Err(streamName, "missing from our books", nil, w)
		return
	}
	callbackClient := clients.NewCallbackClient()
	if uploadSuccess := pushStatus == "null"; uploadSuccess {
		if err := callbackClient.SendRenditionUpload(info.CallbackUrl, info.Source, actualDestination); err != nil {
			Err(streamName, "Cannot send rendition transcode status", err, w)
		}
	} else {
		// We forward pushStatus json to callback
		if err := callbackClient.SendRenditionUploadError(info.CallbackUrl, info.Source, actualDestination, pushStatus); err != nil {
			Err(streamName, "Cannot send rendition transcode error", err, w)
		}
	}
	// We do not delete triggers as source stream is wildcard stream: RENDITION_PREFIX
	if empty := d.StreamCache.Transcoding.RemovePushDestination(streamName, destination); empty {
		if err := callbackClient.SendSegmentTranscodeStatus(info.CallbackUrl, info.Source); err != nil {
			Err(streamName, "Cannot send segment transcode status", err, w)
		}
		d.StreamCache.Transcoding.Remove(streamName)
	}
	return
}

func (d *MistCallbackHandlersCollection) SegmentingPushEnd(w http.ResponseWriter, req *http.Request, streamName string) {
	// when uploading is done, remove trigger and stream from Mist
	defer d.StreamCache.Segmenting.Remove(streamName)
	callbackClient := clients.NewCallbackClient()
	callbackUrl, errC := d.StreamCache.Segmenting.GetCallbackUrl(streamName)
	if errC != nil {
		Err(streamName, "PUSH_END trigger invoked for unknown stream", errC, w)
	}
	if errC == nil {
		errC = callbackClient.SendTranscodeStatus(callbackUrl, clients.TranscodeStatusTranscoding, 0.0)
	}
	errT := d.MistClient.DeleteTrigger(streamName, "PUSH_END")
	errS := d.MistClient.DeleteStream(streamName)
	if errT != nil {
		Err(streamName, fmt.Sprintf("Cannot remove PUSH_END trigger for stream '%s'", streamName), errT, w)
		return
	}
	if errS != nil {
		Err(streamName, fmt.Sprintf("Cannot remove stream '%s'", streamName), errS, w)
		return
	}
	if errC != nil {
		Err(streamName, "Cannot send transcode status", errC, w)
	}

	// TODO: add timeout for the stream upload
	// TODO: start transcoding
}

func Err(streamName, where string, err error, w http.ResponseWriter) {
	txt := fmt.Sprintf("%s name=%s", where, streamName)
	// Mist does not respond or handle returned error codes. We do this for correctness.
	errors.WriteHTTPInternalServerError(w, txt, err)
	log.Printf("Trigger error %s %v", txt, err)
}
