package misttriggers

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/pipeline"
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
	case Segmenting:
		d.VODEngine.TriggerPushEnd(pipeline.PushEndPayload{
			StreamName:     p.StreamName,
			PushStatus:     p.PushStatus,
			Last10LogLines: p.Last10LogLines,
		})
	case Recording:
		d.RecordingPushEnd(w, req, p.StreamName, p.ActualDestination, p.PushStatus)
	default:
		// Not related to API logic
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
		log.LogNoRequestID("RecordingPushEnd extract uuid failed %v", err)
		return
	}
	clients.SendRecordingEventCallback(event)
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
