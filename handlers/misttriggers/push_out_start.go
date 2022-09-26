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
		When:        "start",
		Timestamp:   time.Now().UnixMilli(),
		StreamId:    streamName,
		RecordingId: uuid.New().String(),
		Hostname:    req.Host,
	}
	pushUrl, err := url.Parse(destination)
	if err != nil {
		log.Printf("RecordingPushOutStart url.Parse %v", err)
		return destination
	}
	if err = addUuidToUrl(pushUrl, event.RecordingId); err != nil {
		log.Printf("RecordingPushOutStart addUuidToUrl() %v", err)
		return destination
	}
	go clients.DefaultCallbackClient.SendRecordingEvent(event)
	return pushUrl.String()
}

// addUuidToUrl modifies pushUrl path from:
//
//	s3://livepeer-recordings-bucket/$stream/index.m3u8
//
// to:
//
//	s3://livepeer-recordings-bucket/$stream/<UUID>/index.m3u8
func addUuidToUrl(pushUrl *url.URL, recordingUUID string) error {
	path := strings.Split(pushUrl.EscapedPath(), "/")
	path = append(path, "")
	last := len(path) - 1
	path[last] = path[last-1]
	path[last-1] = recordingUUID
	if path[last-2] != "$stream" {
		// Additional check to ensure we rewrite proper streams
		return fmt.Errorf("stream-id variable not found in proper place")
	}
	pushUrl.Path = strings.Join(path, "/")
	return nil
}
