package misttriggers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang/glog"
)

// This was originally written as catalalyst-api code, since that's where it's
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
func TriggerStreamBuffer(req *http.Request, lines []string) error {
	body, err := ParseStreamBufferPayload(lines)
	if err != nil {
		glog.Infof("Error parsing STREAM_BUFFER payload error=%q payload=%s", err, strings.Join(lines, "\n"))
		return err
	}

	headers := req.Header
	headersStr := ""
	for key, values := range headers {
		headersStr += fmt.Sprintf("%s=%v, ", key, values)
	}
	rawBody, _ := json.Marshal(body)
	glog.Infof("Got STREAM_BUFFER trigger headers=%q payload=%s", headersStr, rawBody)

	return nil
}

type StreamBufferPayload struct {
	StreamName string                  `json:"streamName"`
	State      string                  `json:"state"`
	Tracks     map[string]TrackDetails `json:"tracks"`
	Issues     string                  `json:"issues"`
}

type TrackDetails struct {
	Codec  string                 `json:"codec"`
	Kbits  int                    `json:"kbits"`
	Keys   map[string]interface{} `json:"keys"`
	Fpks   int                    `json:"fpks,omitempty"`
	Height int                    `json:"height,omitempty"`
	Width  int                    `json:"width,omitempty"`
}

func ParseStreamBufferPayload(lines []string) (*StreamBufferPayload, error) {
	if len(lines) < 2 || len(lines) > 3 {
		return nil, fmt.Errorf("invalid payload: expected 2 or 3 lines but got %d", len(lines))
	}

	streamName := lines[0]
	streamState := lines[1]
	streamDetailsStr := lines[2]

	streamDetails, err := ParseMistStreamDetails(streamState, []byte(streamDetailsStr))
	if err != nil {
		return nil, fmt.Errorf("error parsing stream details JSON: %w", err)
	}

	return &StreamBufferPayload{
		StreamName: streamName,
		State:      streamState,
		Tracks:     streamDetails.Tracks,
		Issues:     streamDetails.Issues,
	}, nil
}

type MistStreamDetails struct {
	Tracks map[string]TrackDetails
	Issues string
}

// Mists saves the tracks and issues in the same JSON object, so we need to
// parse them separately. e.g. {track-id-1: {...}, issues: "..."}
func ParseMistStreamDetails(streamState string, data []byte) (*MistStreamDetails, error) {
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

	return &MistStreamDetails{tracks, issues}, nil
}
