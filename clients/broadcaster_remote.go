package clients

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/video"
)

type RemoteBroadcasterClient struct {
	credentials Credentials
}

var broadcasterClient = &http.Client{
	Timeout: API_TIMEOUT,
	Transport: &http.Transport{
		DisableKeepAlives:  true,
		DisableCompression: true,
	},
}
var broadcaserRetryableClient = newRetryableClient(broadcasterClient)
var apiRetryableClient = newRetryableClient(&http.Client{Timeout: API_TIMEOUT})

func NewRemoteBroadcasterClient(credentials Credentials) (RemoteBroadcasterClient, error) {
	if credentials.AccessToken == "" || credentials.CustomAPIURL == "" {
		return RemoteBroadcasterClient{}, fmt.Errorf("error parsing credentials: empty access-token or api URL")
	}
	return RemoteBroadcasterClient{
		credentials: credentials,
	}, nil
}

func (c *RemoteBroadcasterClient) TranscodeSegmentWithRemoteBroadcaster(segment io.Reader, sequenceNumber int64, profiles []video.EncodedProfile, streamName string, durationMillis int64) (TranscodeResult, error) {
	// Get available broadcasters
	bList, err := findBroadcaster(c.credentials)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("findBroadcaster failed %v", err)
	}

	manifestId, err := CreateStream(c.credentials, streamName, profiles)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("CreateStream(): %v", err)
	}
	defer func() {
		err := ReleaseManifestID(c.credentials, manifestId)
		if err != nil {
			log.LogNoRequestID("Error calling ReleaseManifestID", "error", err)
		}
	}()

	// Select one broadcaster
	broadcasterURL, err := pickRandomBroadcaster(bList)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("pickRandomBroadcaster failed %v", err)
	}

	return transcodeSegment(segment, sequenceNumber, durationMillis, broadcasterURL, manifestId, profiles, "")
}

// findBroadcaster contacts Livepeer API for a broadcaster to use if localBroadcaster is not defined
func findBroadcaster(c Credentials) (BroadcasterList, error) {
	if c.AccessToken == "" || c.CustomAPIURL == "" {
		return BroadcasterList{}, fmt.Errorf("empty credentials")
	}
	requestURL, err := url.JoinPath(c.CustomAPIURL, "broadcaster")
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("appending broadcaster to api url %s: %v", c.CustomAPIURL, err)
	}
	req, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("NewRequest GET for url %s: %v", requestURL, err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))
	res, err := metrics.MonitorRequest(metrics.Metrics.BroadcasterClient, broadcaserRetryableClient, req)
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("http do(%s): %v", requestURL, err)
	}
	if !httpOk(res.StatusCode) {
		return BroadcasterList{}, fmt.Errorf("http GET(%s) returned %d %s", requestURL, res.StatusCode, res.Status)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("http GET(%s) read body failed: %v", requestURL, err)
	}

	bList := BroadcasterList{}
	err = json.Unmarshal(body, &bList)
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("http GET(%s) response %s json parsing: %v", requestURL, string(body), err)
	}
	return bList, nil
}

// CreateStream registers new stream on Livepeer infra and returns manifestId
// Call `ReleaseManifestId(manifestId)` after use
func CreateStream(c Credentials, streamName string, profiles []video.EncodedProfile) (string, error) {
	requestURL, err := url.JoinPath(c.CustomAPIURL, "stream")
	if err != nil {
		return "", fmt.Errorf("appending stream to api url %s: %v", c.CustomAPIURL, err)
	}
	// prepare payload
	payload := createStreamPayload{Name: streamName}
	payload.Profiles = append(payload.Profiles, profiles...)
	payloadBytes, err := json.Marshal(&payload)
	if err != nil {
		return "", fmt.Errorf("POST url=%s json encode error %v struct=%v", requestURL, err, payload)
	}
	req, err := http.NewRequest(http.MethodPost, requestURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", fmt.Errorf("NewRequest POST for url %s: %v", requestURL, err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))
	req.Header.Add("Content-Type", "application/json")
	res, err := metrics.MonitorRequest(metrics.Metrics.BroadcasterClient, broadcaserRetryableClient, req)
	if err != nil {
		return "", fmt.Errorf("http do(%s): %v", requestURL, err)
	}
	if !httpOk(res.StatusCode) {
		return "", fmt.Errorf("http POST(%s) returned %d %s", requestURL, res.StatusCode, res.Status)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("http POST(%s) read body failed: %v", requestURL, err)
	}
	response := StreamAllocResponse{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return "", fmt.Errorf("http POST(%s) response %s json parsing: %v", requestURL, string(body), err)
	}
	return response.ManifestId, nil
}

// ReleaseManifestID deletes manifestId created by prior call to CreateStream()
func ReleaseManifestID(c Credentials, manifestId string) error {
	requestURL, err := url.JoinPath(c.CustomAPIURL, fmt.Sprintf("stream/%s", manifestId))
	if err != nil {
		return fmt.Errorf("Error construction API URL. API: %s, manifestID: %s", c.CustomAPIURL, manifestId)
	}

	req, err := http.NewRequest(http.MethodDelete, requestURL, nil)
	if err != nil {
		return fmt.Errorf("Creating HTTP request to release manifest ID failed. URL: %s, manifestID: %s", requestURL, manifestId)
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))
	res, err := metrics.MonitorRequest(metrics.Metrics.BroadcasterClient, apiRetryableClient, req)
	if err != nil {
		return fmt.Errorf("Releasing Manifest ID failed. URL: %s, manifestID: %s, err: %s", requestURL, manifestId, err)
	}

	if !httpOk(res.StatusCode) {
		return fmt.Errorf("Releasing Manifest ID failed. URL: %s, manifestID: %s, HTTP Code: %s", requestURL, manifestId, res.Status)
	}

	return nil
}

func pickRandomBroadcaster(list BroadcasterList) (url.URL, error) {
	chosen := list[rand.Intn(len(list))]
	result, err := url.Parse(chosen.Address)
	if err != nil {
		return url.URL{}, fmt.Errorf("broadcaster entry %s is not a URL %v", chosen.Address, err)
	}
	return *result, nil
}

func newRetryableClient(httpClient *http.Client) *http.Client {
	client := retryablehttp.NewClient()
	client.RetryMax = 2
	client.RetryWaitMin = 200 * time.Millisecond
	client.RetryWaitMax = 1 * time.Second
	client.CheckRetry = metrics.HttpRetryHook
	if httpClient != nil {
		client.HTTPClient = httpClient
	}

	return client.StandardClient()
}
