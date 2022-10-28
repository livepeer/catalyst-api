package clients

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"

	"github.com/livepeer/catalyst-api/config"
)

type LocalBroadcasterClient struct {
	broadcasterURL url.URL
}

func NewLocalBroadcasterClient(broadcasterURL string) (LocalBroadcasterClient, error) {
	u, err := url.Parse(broadcasterURL)
	if err != nil {
		return LocalBroadcasterClient{}, fmt.Errorf("error parsing local broadcaster URL %q: %s", config.DefaultBroadcasterURL, err)
	}
	return LocalBroadcasterClient{
		broadcasterURL: *u,
	}, nil
}

func (c *LocalBroadcasterClient) TranscodeSegment(segment io.Reader, sequenceNumber int64, profiles []EncodedProfile, durationMillis int64) (TranscodeResult, error) {
	conf := LivepeerTranscodeConfiguration{}
	conf.Profiles = append(conf.Profiles, profiles...)
	transcodeConfig, err := json.Marshal(&conf)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("for local B, profiles json encode failed: %v", err)
	}

	return transcodeSegment(segment, sequenceNumber, durationMillis, c.broadcasterURL, config.RandomTrailer(), profiles, string(transcodeConfig))
}
