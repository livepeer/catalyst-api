package clients

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/config"
)

type StreamHealthClient interface {
	PostStreamHealthPayload(streamHealth StreamHealthPayload) error
}

type streamHealthClient struct {
	hookURL, apiToken string
	client            *http.Client
}

func NewStreamHealthClient(cli *config.Cli) StreamHealthClient {
	if cli.StreamHealthHookURL == "" || cli.APIToken == "" {
		glog.Infof("Missing --stream-health-hook-url or --api-token, not sending stream health events")
		return &streamHealthClientStub{}
	}
	client := retryablehttp.NewClient()
	client.RetryMax = 2                   // Attempte request a maximum of this+1 times
	client.RetryWaitMin = 1 * time.Second // Wait at least this long between retries
	client.RetryWaitMax = 5 * time.Second // Wait at most this long between retries (exponential backoff)
	client.HTTPClient = &http.Client{
		Timeout: 5 * time.Second, // Give up on requests that take more than this long
	}

	var hookClient = client.StandardClient()
	return &streamHealthClient{
		client:   hookClient,
		hookURL:  cli.StreamHealthHookURL,
		apiToken: cli.APIToken,
	}
}

func (c *streamHealthClient) PostStreamHealthPayload(streamHealth StreamHealthPayload) error {
	body, err := json.Marshal(streamHealth)
	if err != nil {
		return fmt.Errorf("error marshalling stream health payload: %w", err)
	}
	req, err := http.NewRequest("POST", c.hookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("error creating stream health request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiToken)

	resp, err := c.client.Do(req)
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

type streamHealthClientStub struct{}

func (c *streamHealthClientStub) PostStreamHealthPayload(streamHealth StreamHealthPayload) error {
	return nil
}

type StreamBufferPayload struct {
	StreamName string
	State      string
	Details    *MistStreamDetails
	SessionID  string
}

type MistStreamDetails struct {
	Tracks      map[string]TrackDetails
	Issues      string
	HumanIssues []string
	Extra       map[string]any
}

type TrackDetails struct {
	Codec  string         `json:"codec"`
	Kbits  int            `json:"kbits"`
	Keys   map[string]any `json:"keys"`
	Fpks   int            `json:"fpks,omitempty"`
	Height int            `json:"height,omitempty"`
	Width  int            `json:"width,omitempty"`
}
