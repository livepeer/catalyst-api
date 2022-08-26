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
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		t := &Trigger{w: w, req: req}
		whichOne := req.Header.Get("X-Trigger")
		switch whichOne {
		case "PUSH_END":
			d.TriggerPushEnd(t)
		case "LIVE_TRACK_LIST":
			d.TriggerLiveTrackList(t)
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
func (d *MistCallbackHandlersCollection) TriggerLiveTrackList(t *Trigger) {
	payload, err := io.ReadAll(t.req.Body)
	if err != nil {
		t.Err("Cannot read payload", err)
		return
	}
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	t.streamName = lines[0]
	encodedTracks := lines[1]

	yes, suffix := isTranscodeStream(t.streamName)
	if !yes {
		t.Err("unknown streamName format", nil)
		return
	}

	if streamEnded := encodedTracks == "null"; streamEnded {
		// SOURCE_PREFIX stream is no longer needed
		inputStream := fmt.Sprintf("%s%s", SOURCE_PREFIX, suffix)
		if err = d.MistClient.DeleteStream(inputStream); err != nil {
			log.Printf("ERROR LIVE_TRACK_LIST DeleteStream(%s) %v", inputStream, err)
		}
		// Multiple pushes from RENDITION_PREFIX are in progress.
		return
	}
	tracks := make(LiveTrackListTriggerJson)
	if err = json.Unmarshal([]byte(encodedTracks), &tracks); err != nil {
		t.Err("LiveTrackListTriggerJson json decode error", err)
		return
	}
	// Start push per each video track
	info, err := d.StreamCache.Transcoding.Get(t.streamName)
	if err != nil {
		t.Err("LIVE_TRACK_LIST unknown push source", err)
		return
	}
	for i := range tracks { // i is generated name, not important, all info is contained in element
		if tracks[i].Type != "video" {
			// Only produce an rendition per each video track, selecting best audio track
			continue
		}
		destination := fmt.Sprintf("%s/%s__%dx%d.ts?video=%d&audio=maxbps", info.UploadDir, t.streamName, tracks[i].Width, tracks[i].Height, tracks[i].Index) //.Id)
		if err := d.MistClient.PushStart(t.streamName, destination); err != nil {
			log.Printf("> ERROR push to %s %v", destination, err)
		} else {
			d.StreamCache.Transcoding.AddDestination(t.streamName, destination)
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
func (d *MistCallbackHandlersCollection) TriggerPushEnd(t *Trigger) {
	payload, err := io.ReadAll(t.req.Body)
	if err != nil {
		t.Err("Cannot read payload", err)
		return
	}
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	if len(lines) != 6 {
		errors.WriteHTTPBadRequest(t.w, "Bad request payload", fmt.Errorf("unknown payload '%s'", string(payload)))
		return
	}
	// stream name is the second line in the Mist Trigger payload
	t.streamName = lines[1]
	destination := lines[2]
	actualDestination := lines[3]
	pushStatus := lines[5]
	if yes, _ := isTranscodeStream(t.streamName); yes {
		d.TranscodingPushEnd(t, destination, actualDestination, pushStatus)
	} else {
		d.SegmentingPushEnd(t)
	}
}

func (d *MistCallbackHandlersCollection) TranscodingPushEnd(t *Trigger, destination, actualDestination, pushStatus string) {
	info, err := d.StreamCache.Transcoding.Get(t.streamName)
	if err != nil {
		t.Err("unknown push source", err)
		return
	}
	inOurBooks := false
	for i := 0; i < len(info.Destionations); i++ {
		if info.Destionations[i] == destination {
			inOurBooks = true
			break
		}
	}
	if !inOurBooks {
		t.Err("missing from our books", nil)
		return
	}
	callbackClient := clients.NewCallbackClient()
	if uploadSuccess := pushStatus == "null"; uploadSuccess {
		if err := callbackClient.SendRenditionUpload(info.CallbackUrl, info.Source, actualDestination); err != nil {
			t.Err("Cannot send rendition transcode status", err)
		}
	} else {
		// We forward pushStatus json to callback
		if err := callbackClient.SendRenditionUploadError(info.CallbackUrl, info.Source, actualDestination, pushStatus); err != nil {
			t.Err("Cannot send rendition transcode error", err)
		}
	}
	// We do not delete triggers as source stream is wildcard stream: RENDITION_PREFIX
	if empty := d.StreamCache.Transcoding.RemovePushDestination(t.streamName, destination); empty {
		if err := callbackClient.SendSegmentTranscodeStatus(info.CallbackUrl, info.Source); err != nil {
			t.Err("Cannot send segment transcode status", err)
		}
		d.StreamCache.Transcoding.Remove(t.streamName)
	}
	return
}

func (d *MistCallbackHandlersCollection) SegmentingPushEnd(t *Trigger) {
	// when uploading is done, remove trigger and stream from Mist
	errT := d.MistClient.DeleteTrigger(t.streamName, "PUSH_END")
	errS := d.MistClient.DeleteStream(t.streamName)
	if errT != nil {
		t.Err(fmt.Sprintf("Cannot remove PUSH_END trigger for stream '%s'", t.streamName), errT)
		return
	}
	if errS != nil {
		t.Err(fmt.Sprintf("Cannot remove stream '%s'", t.streamName), errS)
		return
	}

	callbackClient := clients.NewCallbackClient()
	callbackUrl, err := d.StreamCache.Segmenting.GetCallbackUrl(t.streamName)
	if err != nil {
		t.Err("PUSH_END trigger invoked for unknown stream", err)
	}
	if err := callbackClient.SendTranscodeStatus(callbackUrl, clients.TranscodeStatusTranscoding, 0.0); err != nil {
		t.Err("Cannot send transcode status", err)
	}
	d.StreamCache.Segmenting.Remove(t.streamName)

	// TODO: add timeout for the stream upload
	// TODO: start transcoding
}

type Trigger struct {
	w   http.ResponseWriter
	req *http.Request

	streamName string
}

func (t *Trigger) Err(where string, err error) {
	txt := fmt.Sprintf("%s name=%s", where, t.streamName)
	// Mist does not respond or handle returned error codes. We do this for correctness.
	errors.WriteHTTPInternalServerError(t.w, txt, err)
	log.Printf("Trigger error %s %v", txt, err)
}
