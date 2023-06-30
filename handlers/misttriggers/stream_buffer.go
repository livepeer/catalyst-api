package misttriggers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/errors"
)

var hookClient *http.Client

func init() {
	client := retryablehttp.NewClient()
	client.RetryMax = 2                   // Attempte request a maximum of this+1 times
	client.RetryWaitMin = 1 * time.Second // Wait at least this long between retries
	client.RetryWaitMax = 5 * time.Second // Wait at most this long between retries (exponential backoff)
	client.HTTPClient = &http.Client{
		Timeout: 5 * time.Second, // Give up on requests that take more than this long
	}

	hookClient = client.StandardClient()
}

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
func (d *MistCallbackHandlersCollection) TriggerStreamBuffer(ctx context.Context, w http.ResponseWriter, req *http.Request, payload MistTriggerBody) {
	sessionID := req.Header.Get("X-UUID")

	body, err := ParseStreamBufferPayload(payload)
	if err != nil {
		glog.Infof("Error parsing STREAM_BUFFER payload error=%q payload=%q", err, string(payload))
		errors.WriteHTTPBadRequest(w, "Error parsing PUSH_END payload", err)
		return
	}

	rawBody, _ := json.Marshal(body)
	go d.broker.TriggerStreamBuffer(ctx, body)
	if d.cli.StreamHealthHookURL == "" {
		glog.Infof("Stream health hook URL not set, skipping trigger sessionId=%q payload=%s", sessionID, rawBody)
		return
	}
	glog.Infof("Got STREAM_BUFFER trigger sessionId=%q payload=%s", sessionID, rawBody)

	streamHealth := StreamHealthPayload{
		StreamName: body.StreamName,
		SessionID:  sessionID,
		IsActive:   !body.IsEmpty(),
		IsHealthy:  body.IsFull() || body.IsRecover(),
	}
	if details := body.Details; details != nil {
		streamHealth.IsHealthy = streamHealth.IsHealthy && details.Issues == ""
		streamHealth.Tracks = details.Tracks
		streamHealth.Issues = details.Issues
		streamHealth.HumanIssues = details.HumanIssues
		streamHealth.Extra = details.Extra
	}

	err = d.PostStreamHealthPayload(streamHealth)
	if err != nil {
		glog.Infof("Error pushing STREAM_HEALTH payload error=%q payload=%s", err, rawBody)
		errors.WriteHTTPInternalServerError(w, "Error pushing STREAM_HEALTH payload", err)
		return
	}
}

type StreamHealthPayload struct {
	StreamName string `json:"stream_name"`
	SessionID  string `json:"session_id"`
	IsActive   bool   `json:"is_active"`

	IsHealthy   bool     `json:"is_healthy"`
	Issues      string   `json:"issues,omitempty"`
	HumanIssues []string `json:"human_issues,omitempty"`

	Tracks map[string]TrackDetails `json:"tracks,omitempty"`
	Extra  map[string]any          `json:"extra,omitempty"`
}

func (d *MistCallbackHandlersCollection) PostStreamHealthPayload(payload StreamHealthPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshalling stream health payload: %w", err)
	}
	req, err := http.NewRequest("POST", d.cli.StreamHealthHookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("error creating stream health request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+d.cli.APIToken)

	resp, err := hookClient.Do(req)
	if err != nil {
		return fmt.Errorf("error pushing stream health to hook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading stream health hook response: %w", err)
		}
		glog.Warningf("Error pushing stream health to hook status=%d body=%q", resp.StatusCode, respBody)
	}

	return nil
}

type StreamBufferPayload struct {
	StreamName string
	State      string
	Details    *MistStreamDetails
}

func (s *StreamBufferPayload) IsEmpty() bool {
	return s.State == "EMPTY"
}

func (s *StreamBufferPayload) IsFull() bool {
	return s.State == "FULL"
}

func (s *StreamBufferPayload) IsRecover() bool {
	return s.State == "RECOVER"
}

type TrackDetails struct {
	Codec  string         `json:"codec"`
	Kbits  int            `json:"kbits"`
	Keys   map[string]any `json:"keys"`
	Fpks   int            `json:"fpks,omitempty"`
	Height int            `json:"height,omitempty"`
	Width  int            `json:"width,omitempty"`
}

func ParseStreamBufferPayload(payload MistTriggerBody) (*StreamBufferPayload, error) {
	lines := payload.Lines()
	if len(lines) < 2 || len(lines) > 3 {
		return nil, fmt.Errorf("invalid payload: expected 2 or 3 lines but got %d", len(lines))
	}

	streamName := lines[0]
	streamState := lines[1]
	var streamDetailsStr string
	if len(lines) == 3 {
		streamDetailsStr = lines[2]
	}

	streamDetails, err := ParseMistStreamDetails(streamState, []byte(streamDetailsStr))
	if err != nil {
		return nil, fmt.Errorf("error parsing stream details JSON: %w", err)
	}

	return &StreamBufferPayload{
		StreamName: streamName,
		State:      streamState,
		Details:    streamDetails,
	}, nil
}

type MistStreamDetails struct {
	Tracks      map[string]TrackDetails
	Issues      string
	HumanIssues []string
	Extra       map[string]any
}

// Mists sends the track detail objects in the same JSON object as other
// non-object fields (string and array issues and numeric metrics). So we need
// to parse them separately and do a couple of JSON juggling here.
// e.g. {track-id-1: {...}, issues: "a string", human_issues: ["a", "b"], "jitter": 32}
func ParseMistStreamDetails(streamState string, data []byte) (*MistStreamDetails, error) {
	if streamState == "EMPTY" {
		return nil, nil
	}

	var issues struct {
		Issues      string   `json:"issues"`
		HumanIssues []string `json:"human_issues"`
	}
	err := json.Unmarshal(data, &issues)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling issues JSON: %w", err)
	}

	var tracksAndIssues map[string]any
	err = json.Unmarshal(data, &tracksAndIssues)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %w", err)
	}
	delete(tracksAndIssues, "issues")
	delete(tracksAndIssues, "human_issues")

	extra := map[string]any{}
	for key, val := range tracksAndIssues {
		if _, isObj := val.(map[string]any); isObj {
			// this is a track, it will be parsed from the serialized obj below
			continue
		} else {
			extra[key] = val
			delete(tracksAndIssues, key)
		}
	}

	tracksJSON, err := json.Marshal(tracksAndIssues) // only tracks now
	if err != nil {
		return nil, fmt.Errorf("error marshalling stream details tracks: %w", err)
	}

	var tracks map[string]TrackDetails
	if err = json.Unmarshal(tracksJSON, &tracks); err != nil {
		return nil, fmt.Errorf("error parsing stream details tracks: %w", err)
	}

	return &MistStreamDetails{tracks, issues.Issues, issues.HumanIssues, extra}, nil
}
