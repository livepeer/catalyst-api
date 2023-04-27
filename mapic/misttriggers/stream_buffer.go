package misttriggers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
)

// This is written as catalalyst-api-native code, since that's where it's
// supposed to go. Currently, catalyst-api has no connection with Mist anymore
// though, so it's easier to plug this into mist-api-connector instead for now.
// TODO: Move this to catalyst-api when its got the Mist plumbing back.

// This trigger is run whenever the live buffer state of a stream changes. It is
// not ran for VoD streams. This trigger is stream-specific and non-blocking.
//
// The payload for this trigger is multiple lines, each separated by a single
// newline character (without an ending newline), containing data as such:
//
// stream name
// stream state (one of: FULL, EMPTY, DRY, RECOVER)
// {JSON object with stream details, only when state is not EMPTY}
//
// Read the Mist documentation for more details on each of the stream states.
func TriggerStreamBuffer(w http.ResponseWriter, req *http.Request, payload []byte) {
	body, err := ParseStreamBufferPayload(string(payload))
	if err != nil {
		log.LogNoRequestID("Error parsing STREAM_BUFFER payload", "error", err, "payload", string(payload))
		errors.WriteHTTPBadRequest(w, "Error parsing STREAM_BUFFER payload", err)
		return
	}

	headers := req.Header
	headersStr := ""
	for key, values := range headers {
		headersStr += fmt.Sprintf("%s=%v;, ", key, values)
	}
	rawBody, _ := json.Marshal(body)
	log.LogNoRequestID("Got STREAM_BUFFER trigger", "headers", headersStr, "payload", rawBody)
}

type StreamBufferPayload struct {
	StreamName    string
	StreamState   string
	StreamDetails *StreamDetails
}

func ParseStreamBufferPayload(payload string) (*StreamBufferPayload, error) {
	lines := strings.Split(strings.TrimSuffix(payload, "\n"), "\n")

	if len(lines) < 2 || len(lines) > 3 {
		return nil, fmt.Errorf("invalid payload: expected 2 or 3 lines but got %d", len(lines))
	}

	streamName := lines[0]
	streamState := lines[1]
	streamDetailsStr := lines[2]

	streamDetails, err := ParseStreamDetails(streamState, []byte(streamDetailsStr))
	if err != nil {
		return nil, fmt.Errorf("error parsing stream details JSON: %w", err)
	}

	return &StreamBufferPayload{
		StreamName:    streamName,
		StreamState:   streamState,
		StreamDetails: streamDetails,
	}, nil
}

type TrackDetails struct {
	Codec  string                 `json:"codec"`
	Kbits  int                    `json:"kbits"`
	Keys   map[string]interface{} `json:"keys"`
	Fpks   int                    `json:"fpks,omitempty"`
	Height int                    `json:"height,omitempty"`
	Width  int                    `json:"width,omitempty"`
}

type StreamDetails struct {
	Tracks map[string]TrackDetails
	Issues string
}

func ParseStreamDetails(streamState string, data []byte) (*StreamDetails, error) {
	if streamState == "EMPTY" {
		return nil, nil
	}

	var tracksAndIssues map[string]interface{}
	err := json.Unmarshal(data, &tracksAndIssues)
	if err != nil {
		return nil, fmt.Errorf("error parsing stream details JSON: %w", err)
	}

	issues, ok := tracksAndIssues["issues"].(string)
	if raw, keyExists := tracksAndIssues["issues"]; keyExists && !ok {
		return nil, fmt.Errorf("issues field is not a string: %v", raw)
	}
	delete(tracksAndIssues, "issues")

	tracksJSON, err := json.Marshal(tracksAndIssues) // only tracks now
	if err != nil {
		return nil, fmt.Errorf("error marshalling stream details tracks: %w", err)
	}

	var tracks map[string]TrackDetails
	if err = json.Unmarshal(tracksJSON, &tracks); err != nil {
		return nil, fmt.Errorf("eror parsing stream details tracks: %w", err)
	}

	return &StreamDetails{tracks, issues}, nil
}
