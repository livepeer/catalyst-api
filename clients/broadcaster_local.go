package clients

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
)

// Currently only implemented by LocalBroadcasterClient
// TODO: Try to come up with a unified interface across Local and Remote
type BroadcasterClient interface {
	TranscodeSegment(segment io.Reader, sequenceNumber int64, durationMillis int64, manifestID string, conf LivepeerTranscodeConfiguration) (TranscodeResult, error)
}

type LocalBroadcasterClient struct {
	broadcasterURL url.URL
}

func NewLocalBroadcasterClient(broadcasterURL string) (BroadcasterClient, error) {
	u, err := url.Parse(broadcasterURL)
	if err != nil {
		return &LocalBroadcasterClient{}, fmt.Errorf("error parsing local broadcaster URL %q: %s", broadcasterURL, err)
	}
	return &LocalBroadcasterClient{
		broadcasterURL: *u,
	}, nil
}

func (c *LocalBroadcasterClient) TranscodeSegment(segment io.Reader, sequenceNumber int64, durationMillis int64, manifestID string, conf LivepeerTranscodeConfiguration) (TranscodeResult, error) {
	if err := validateProfiles(conf.Profiles); err != nil {
		return TranscodeResult{}, err
	}
	transcodeConfig, err := json.Marshal(&conf)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("for local B, profiles json encode failed: %v", err)
	}
	return transcodeSegment(segment, sequenceNumber, durationMillis, c.broadcasterURL, manifestID, string(transcodeConfig))
}
