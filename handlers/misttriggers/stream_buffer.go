package misttriggers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
)

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
func (d *MistCallbackHandlersCollection) TriggerStreamBuffer(cli *config.Cli, req *http.Request, lines []string) error {
	sessionID := req.Header.Get("X-UUID")

	body, err := ParseStreamBufferPayload(lines)
	if err != nil {
		glog.Infof("Error parsing STREAM_BUFFER payload error=%q payload=%q", err, strings.Join(lines, "\n"))
		return err
	}
	body.SessionID = sessionID

	rawBody, _ := json.Marshal(body)
	if cli.StreamHealthHookURL == "" {
		glog.Infof("Stream health hook URL not set, skipping trigger sessionId=%q payload=%s", sessionID, rawBody)
		return nil
	}
	glog.Infof("Got STREAM_BUFFER trigger sessionId=%q payload=%s", sessionID, rawBody)

	streamHealth := clients.StreamHealthPayload{
		StreamName: body.StreamName,
		SessionID:  body.SessionID,
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

	err = d.StreamHealthClient.PostStreamHealthPayload(streamHealth)
	if err != nil {
		glog.Infof("Error pushing STREAM_HEALTH payload error=%q payload=%s", err, rawBody)
		return err
	}

	return nil
}

func ParseStreamBufferPayload(lines []string) (*clients.StreamBufferPayload, error) {
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

	return &clients.StreamBufferPayload{
		StreamName: streamName,
		State:      streamState,
		Details:    streamDetails,
	}, nil
}

// Mists sends the track detail objects in the same JSON object as other
// non-object fields (string and array issues and numeric metrics). So we need
// to parse them separately and do a couple of JSON juggling here.
// e.g. {track-id-1: {...}, issues: "a string", human_issues: ["a", "b"], "jitter": 32}
func ParseMistStreamDetails(streamState string, data []byte) (*clients.MistStreamDetails, error) {
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

	var tracks map[string]clients.TrackDetails
	if err = json.Unmarshal(tracksJSON, &tracks); err != nil {
		return nil, fmt.Errorf("error parsing stream details tracks: %w", err)
	}

	return &clients.MistStreamDetails{tracks, issues.Issues, issues.HumanIssues, extra}, nil
}
