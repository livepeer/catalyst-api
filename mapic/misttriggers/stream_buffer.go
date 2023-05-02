package misttriggers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/config"
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
func TriggerStreamBuffer(cli *config.Cli, req *http.Request, lines []string) error {
	sessionID := req.Header.Get("X-UUID")

	body, err := ParseStreamBufferPayload(lines)
	if err != nil {
		glog.Infof("Error parsing STREAM_BUFFER payload error=%q payload=%q", err, strings.Join(lines, "\n"))
		return err
	}

	rawBody, _ := json.Marshal(body)
	if cli.StreamHealthHookURL == "" {
		glog.Infof("Stream health hook URL not set, skipping trigger sessionId=%q payload=%s", sessionID, rawBody)
		return nil
	}
	glog.Infof("Got STREAM_BUFFER trigger sessionId=%q payload=%s", sessionID, rawBody)

	streamHealth := StreamHealthPayload{
		StreamName: body.StreamName,
		SessionID:  sessionID,
		IsActive:   body.State != "EMPTY",
		IsHealthy:  body.State == "FULL" || body.State == "RECOVER",
	}
	if details := body.Details; details != nil {
		streamHealth.IsHealthy = streamHealth.IsHealthy && details.Issues == ""
		streamHealth.Tracks = details.Tracks
		streamHealth.Issues = details.Issues
		streamHealth.HumanIssues = details.HumanIssues
		streamHealth.Extra = details.Extra
	}

	err = PostStreamHealthPayload(cli.StreamHealthHookURL, cli.APIToken, streamHealth)
	if err != nil {
		glog.Infof("Error pushing STREAM_HEALTH payload error=%q payload=%s", err, rawBody)
		return err
	}

	return nil
}

type StreamHealthPayload struct {
	StreamName string `json:"stream_name"`
	SessionID  string `json:"session_id"`
	IsActive   bool   `json:"is_active"`

	IsHealthy   bool   `json:"is_healthy"`
	Issues      string `json:"issues,omitempty"`
	HumanIssues string `json:"human_issues,omitempty"`

	Tracks map[string]TrackDetails `json:"tracks,omitempty"`
	Extra  map[string]interface{}  `json:"extra,omitempty"`
}

func PostStreamHealthPayload(url, apiToken string, payload StreamHealthPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshalling stream health payload: %w", err)
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("error creating stream health request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiToken)

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
	Tracks              map[string]TrackDetails
	Issues, HumanIssues string
	Extra               map[string]interface{}
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

	var (
		issues, humanIssues string
		extra               = map[string]interface{}{}
	)
	for key, val := range tracksAndIssues {
		switch tval := val.(type) {
		case map[string]interface{}:
			// this is a track, it will be parsed from the serialized obj below
			continue
		case string:
			if key == "issues" {
				issues = tval
			} else if key == "human_issues" {
				humanIssues = tval
			} else {
				extra[key] = val
			}
		default:
			extra[key] = val
		}
		delete(tracksAndIssues, key)
	}

	tracksJSON, err := json.Marshal(tracksAndIssues) // only tracks now
	if err != nil {
		return nil, fmt.Errorf("error marshalling stream details tracks: %w", err)
	}

	var tracks map[string]TrackDetails
	if err = json.Unmarshal(tracksJSON, &tracks); err != nil {
		return nil, fmt.Errorf("eror parsing stream details tracks: %w", err)
	}

	return &MistStreamDetails{tracks, issues, humanIssues, extra}, nil
}
