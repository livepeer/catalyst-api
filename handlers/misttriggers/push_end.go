package misttriggers

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/subprocess"
)

type PushEndPayload struct {
	StreamName        string
	Destination       string
	ActualDestination string
	Last10LogLines    string
	PushStatus        string
}

func ParsePushEndPayload(payload string) (PushEndPayload, error) {
	lines := strings.Split(strings.TrimSuffix(payload, "\n"), "\n")
	if len(lines) != 6 {
		return PushEndPayload{}, fmt.Errorf("expected 6 lines in PUSH_END payload but got %d. Payload: %s", len(lines), payload)
	}

	return PushEndPayload{
		StreamName:        lines[1],
		Destination:       lines[2],
		ActualDestination: lines[3],
		Last10LogLines:    lines[4],
		PushStatus:        lines[5],
	}, nil
}

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
	p, err := ParsePushEndPayload(string(payload))
	if err != nil {
		errors.WriteHTTPBadRequest(w, "Error parsing PUSH_END payload", err)
		return
	}

	switch streamNameToPipeline(p.StreamName) {
	case Transcoding:
		d.TranscodingPushEnd(w, req, p.StreamName, p.Destination, p.ActualDestination, p.PushStatus)
	case Segmenting:
		d.SegmentingPushEnd(w, req, p)
	case Recording:
		d.RecordingPushEnd(w, req, p.StreamName, p.ActualDestination, p.PushStatus)
	default:
		// Not related to API logic
	}
}

func (d *MistCallbackHandlersCollection) TranscodingPushEnd(w http.ResponseWriter, req *http.Request, streamName, destination, actualDestination, pushStatus string) {
	info := cache.DefaultStreamCache.Transcoding.Get(streamName)
	if info == nil {
		errors.WriteHTTPBadRequest(w, "PUSH_END unknown push source: "+streamName, nil)
		return
	}

	// Check if we have a record of this destination
	if !info.ContainsDestination(destination) {
		errors.WriteHTTPBadRequest(w, fmt.Sprintf("PUSH_END can't find destination %q for stream %q", destination, streamName), nil)
		return
	}

	uploadSuccess := pushStatus != "null"
	if uploadSuccess {
		// TODO: Do some maths so that we don't always send 0.5
		if err := clients.DefaultCallbackClient.SendTranscodeStatus(info.CallbackUrl, clients.TranscodeStatusTranscoding, 0.5); err != nil {
			_ = config.Logger.Log("msg", "Error sending transcode status in TranscodingPushEnd", "err", err)
		}
	} else {
		// We forward pushStatus json to callback
		if err := clients.DefaultCallbackClient.SendTranscodeStatusError(info.CallbackUrl, fmt.Sprintf("Error while pushing to %s: %s", actualDestination, pushStatus)); err != nil {
			_ = config.Logger.Log("msg", "Error sending transcode error status in TranscodingPushEnd", "err", err)
		}
	}

	if err := createDtsh(actualDestination); err != nil {
		_ = config.Logger.Log("msg", "createDtsh failed", "err", err, "destination", actualDestination)
	}

	// We do not delete triggers as source stream is wildcard stream: RENDITION_PREFIX
	cache.DefaultStreamCache.Transcoding.RemovePushDestination(streamName, destination)
	if cache.DefaultStreamCache.Transcoding.AreDestinationsEmpty(streamName) {
		if err := clients.DefaultCallbackClient.SendTranscodeStatusCompleted(info.CallbackUrl, clients.InputVideo{}, []clients.OutputVideo{}); err != nil {
			_ = config.Logger.Log("msg", "Error sending transcode completed status in TranscodingPushEnd", "err", err)
		}
		cache.DefaultStreamCache.Transcoding.Remove(streamName)
	}
}

func (d *MistCallbackHandlersCollection) RecordingPushEnd(w http.ResponseWriter, req *http.Request, streamName, actualDestination, pushStatus string) {
	var err error
	pushSuccess := pushStatus == "null"
	event := &clients.RecordingEvent{
		Event:      "end",
		Timestamp:  time.Now().UnixMilli(),
		StreamName: streamName,
		Hostname:   req.Host,
		Success:    &pushSuccess,
	}
	if event.RecordingId, err = uuidFromPushUrl(actualDestination); err != nil {
		log.Printf("RecordingPushEnd extract uuid failed %v", err)
		return
	}
	go clients.DefaultCallbackClient.SendRecordingEvent(event)
}

func (d *MistCallbackHandlersCollection) SegmentingPushEnd(w http.ResponseWriter, req *http.Request, p PushEndPayload) {
	// when uploading is done, remove trigger and stream from Mist
	defer cache.DefaultStreamCache.Segmenting.Remove(p.StreamName)

	callbackUrl := cache.DefaultStreamCache.Segmenting.GetCallbackUrl(p.StreamName)
	if callbackUrl == "" {
		errors.WriteHTTPBadRequest(w, "PUSH_END trigger invoked for unknown stream: "+p.StreamName, nil)
		return
	}

	// TODO: Find a better way to determine if the push status was successfull (i.e. segmenting step was successful)
	if !strings.Contains(p.Last10LogLines, "Buffer completely played out") {
		_ = clients.DefaultCallbackClient.SendTranscodeStatusError(callbackUrl, "Segmenting Failed: "+p.PushStatus)
		_ = errors.WriteHTTPBadRequest(w, "Segmenting Failed. PUSH_END trigger for stream "+p.StreamName+" was "+p.PushStatus, nil)
		return
	}

	// Try to clean up the trigger and stream from Mist. If these fail then we only log, since we still want to do any
	// further cleanup stages and callbacks
	if err := d.MistClient.DeleteTrigger(p.StreamName, TRIGGER_PUSH_END); err != nil {
		_ = config.Logger.Log("msg", "Failed to delete PUSH_END trigger", "err", err.Error(), "stream_name", p.StreamName)
	}
	if err := d.MistClient.DeleteStream(p.StreamName); err != nil {
		_ = config.Logger.Log("msg", "Failed to delete stream", "err", err.Error(), "stream_name", p.StreamName)
	}

	// Let Studio know that we've finished the Segmenting phase
	if err := clients.DefaultCallbackClient.SendTranscodeStatus(callbackUrl, clients.TranscodeStatusPreparingCompleted, 1); err != nil {
		_ = config.Logger.Log("msg", "Failed to send transcode status callback", "err", err.Error(), "stream_name", p.StreamName)
	}

	// Get the source stream's detailed track info before kicking off transcode
	infoJson, err := d.MistClient.GetStreamInfo(p.StreamName)
	if err != nil {
		_ = config.Logger.Log("msg", "Failed to get stream info", "err", err.Error(), "stream_name", p.StreamName)
	}

	si := cache.DefaultStreamCache.Segmenting.Get(p.StreamName)
	transcodeRequest := handlers.TranscodeSegmentRequest{
		SourceFile:       si.SourceFile,
		CallbackURL:      si.CallbackURL,
		AccessToken:      si.AccessToken,
		TranscodeAPIUrl:  si.TranscodeAPIUrl,
		SourceStreamInfo: infoJson,
		UploadURL:        si.UploadURL,
	}
	go func() {
		err := handlers.RunTranscodeProcess(d.MistClient, transcodeRequest)
		if err != nil {
			_ = config.Logger.Log("msg", "RunTranscodeProcess returned an error", "err", err.Error(), "stream_name", p.StreamName)
		}
	}()
}

func uuidFromPushUrl(uri string) (string, error) {
	pushUrl, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	path := strings.Split(pushUrl.EscapedPath(), "/")
	if len(path) < 4 {
		return "", fmt.Errorf("push url path malformed: element count %d %s", len(path), pushUrl.EscapedPath())
	}
	return path[len(path)-2], nil
}

func createDtsh(destination string) error {
	url, err := url.Parse(destination)
	if err != nil {
		return err
	}
	url.RawQuery = ""
	url.Fragment = ""
	headerPrepare := exec.Command(path.Join(config.PathMistDir, "MistInHLS"), "-H", url.String())
	if err = subprocess.LogOutputs(headerPrepare); err != nil {
		return err
	}
	if err = headerPrepare.Start(); err != nil {
		return err
	}
	go func() {
		if err := headerPrepare.Wait(); err != nil {
			_ = config.Logger.Log("msg", "createDtsh return code", "code", err, "destination", destination)
		}
	}()
	return nil
}
