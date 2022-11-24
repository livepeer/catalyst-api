package misttriggers

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/errors"
)

// TriggerPushOutStart responds to PUSH_OUT_START trigger
// This trigger is run right before an outgoing push is started. This trigger is stream-specific and must be blocking.
// The payload for this trigger is multiple lines, each separated by a single newline character (without an ending newline), containing data:
//
// stream name (string)
// push target URI (string)
func (d *MistCallbackHandlersCollection) TriggerPushOutStart(w http.ResponseWriter, req *http.Request, payload []byte) {
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	if len(lines) != 2 {
		errors.WriteHTTPBadRequest(w, "Bad request payload", fmt.Errorf("unknown payload '%s'", string(payload)))
		return
	}
	streamName := lines[0]
	destination := lines[1]
	var destinationToReturn string
	switch streamNameToPipeline(streamName) {
	case Recording:
		destinationToReturn = d.RecordingPushOutStart(w, req, streamName, destination)
	default:
		destinationToReturn = destination
	}
	if _, err := w.Write([]byte(destinationToReturn)); err != nil {
		log.Printf("TriggerPushOutStart failed to send rewritten url: %v", err)
	}
}

func (d *MistCallbackHandlersCollection) RecordingPushOutStart(w http.ResponseWriter, req *http.Request, streamName, destination string) string {
	event := &clients.RecordingEvent{
		Event:       "start",
		Timestamp:   time.Now().UnixMilli(),
		StreamName:  streamName,
		RecordingId: uuid.New().String(),
		Hostname:    req.Host,
	}
	pushUrl, err := url.Parse(destination)
	if err != nil {
		log.Printf("RecordingPushOutStart url.Parse %v", err)
		return destination
	}
	// Add uuid after stream name
	pushUrl.Path = strings.Replace(pushUrl.Path, "$stream", "$stream/"+event.RecordingId, 1)
	clients.SendRecordingEventCallback(event)
	return pushUrl.String()
}
