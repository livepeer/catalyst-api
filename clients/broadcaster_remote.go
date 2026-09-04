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
	apiClient   *http.Client
	mediaClient *http.Client
}

var (
	remoteBroadcasterAPIClient   = newRetryableClient(newPublicHTTPClientForSurface(API_TIMEOUT, "transcode_api", false, false))
	remoteBroadcasterMediaClient = newRetryableClient(
		newPublicHTTPClientForSurface(TRANSCODE_TIMEOUT, "broadcaster", false, false),
	)
)

func NewRemoteBroadcasterClient(credentials Credentials) (RemoteBroadcasterClient, error) {
	return newRemoteBroadcasterClient(credentials, remoteBroadcasterAPIClient, remoteBroadcasterMediaClient)
}

// newRemoteBroadcasterClient keeps the production policy while allowing tests
// to replace network I/O with an in-memory transport.
func newRemoteBroadcasterClient(credentials Credentials, apiClient, mediaClient *http.Client) (RemoteBroadcasterClient, error) {
	if credentials.AccessToken == "" || credentials.CustomAPIURL == "" {
		return RemoteBroadcasterClient{}, fmt.Errorf("error parsing credentials: empty access-token or api URL")
	}
	apiURL, err := url.Parse(credentials.CustomAPIURL)
	if err != nil {
		return RemoteBroadcasterClient{}, rejectDestination("transcode_api", "transcode API URL is invalid")
	}
	if err := ValidatePublicURL(apiURL, "transcode_api", false, "https"); err != nil {
		return RemoteBroadcasterClient{}, err
	}
	return RemoteBroadcasterClient{
		credentials: credentials,
		apiClient:   apiClient,
		mediaClient: mediaClient,
	}, nil
}

func (c *RemoteBroadcasterClient) TranscodeSegmentWithRemoteBroadcaster(segment io.Reader, sequenceNumber int64, profiles []video.EncodedProfile, streamName string, durationMillis int64) (TranscodeResult, error) {
	// Get available broadcasters
	bList, err := c.findBroadcaster()
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("findBroadcaster failed %v", err)
	}

	manifestId, err := c.createStream(streamName, profiles)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("CreateStream(): %v", err)
	}
	defer func() {
		err := c.releaseManifestID(manifestId)
		if err != nil {
			log.LogNoRequestID("Error calling ReleaseManifestID", "error", err)
		}
	}()

	// Select one broadcaster
	broadcasterURL, err := c.pickRandomBroadcaster(bList)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("pickRandomBroadcaster failed %v", err)
	}

	return transcodeSegmentWithClient(segment, sequenceNumber, durationMillis, broadcasterURL, manifestId, "", c.mediaClient)
}

// findBroadcaster contacts Livepeer API for a broadcaster to use if localBroadcaster is not defined

func (c *RemoteBroadcasterClient) findBroadcaster() (BroadcasterList, error) {
	credentials := c.credentials
	if credentials.AccessToken == "" || credentials.CustomAPIURL == "" {
		return BroadcasterList{}, fmt.Errorf("empty credentials")
	}
	requestURL, err := url.JoinPath(credentials.CustomAPIURL, "broadcaster")
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("appending broadcaster to api url: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("NewRequest GET for url %s: %v", log.RedactURL(requestURL), err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", credentials.AccessToken))
	res, err := metrics.MonitorRequest(metrics.Metrics.BroadcasterClient, c.apiClient, req)
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("http do(%s): %v", log.RedactURL(requestURL), err)
	}
	defer res.Body.Close() // nolint:errcheck

	if !httpOk(res.StatusCode) {
		return BroadcasterList{}, fmt.Errorf("http GET(%s) returned %d %s", log.RedactURL(requestURL), res.StatusCode, res.Status)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("http GET(%s) read body failed: %v", log.RedactURL(requestURL), err)
	}

	bList := BroadcasterList{}
	err = json.Unmarshal(body, &bList)
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("http GET(%s) response JSON parsing failed: %v", log.RedactURL(requestURL), err)
	}
	return bList, nil
}

// CreateStream registers new stream on Livepeer infra and returns manifestId
// Call `ReleaseManifestId(manifestId)` after use
func (c *RemoteBroadcasterClient) createStream(streamName string, profiles []video.EncodedProfile) (string, error) {
	requestURL, err := url.JoinPath(c.credentials.CustomAPIURL, "stream")
	if err != nil {
		return "", fmt.Errorf("appending stream to api url: %v", err)
	}
	if err := validateProfiles(profiles); err != nil {
		return "", err
	}
	payload := createStreamPayload{
		Name:     streamName,
		Profiles: append([]video.EncodedProfile(nil), profiles...),
	}
	payloadBytes, err := json.Marshal(&payload)
	if err != nil {
		return "", fmt.Errorf("POST url=%s json encode error %v struct=%v", log.RedactURL(requestURL), err, payload)
	}
	req, err := http.NewRequest(http.MethodPost, requestURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", fmt.Errorf("NewRequest POST for url %s: %v", log.RedactURL(requestURL), err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.credentials.AccessToken))
	req.Header.Add("Content-Type", "application/json")
	res, err := metrics.MonitorRequest(metrics.Metrics.BroadcasterClient, c.apiClient, req)
	if err != nil {
		return "", fmt.Errorf("http do(%s): %v", log.RedactURL(requestURL), err)
	}
	defer res.Body.Close() // nolint:errcheck

	if !httpOk(res.StatusCode) {
		return "", fmt.Errorf("http POST(%s) returned %d %s", log.RedactURL(requestURL), res.StatusCode, res.Status)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("http POST(%s) read body failed: %v", log.RedactURL(requestURL), err)
	}
	response := StreamAllocResponse{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return "", fmt.Errorf("http POST(%s) response JSON parsing failed: %v", log.RedactURL(requestURL), err)
	}
	return response.ManifestId, nil
}

// ReleaseManifestID deletes manifestId created by prior call to CreateStream()
func (c *RemoteBroadcasterClient) releaseManifestID(manifestId string) error {
	requestURL, err := url.JoinPath(c.credentials.CustomAPIURL, fmt.Sprintf("stream/%s", manifestId))
	if err != nil {
		return fmt.Errorf("error constructing API URL for manifest ID %s", manifestId)
	}

	req, err := http.NewRequest(http.MethodDelete, requestURL, nil)
	if err != nil {
		return fmt.Errorf("Creating HTTP request to release manifest ID failed. URL: %s, manifestID: %s", log.RedactURL(requestURL), manifestId)
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.credentials.AccessToken))
	res, err := metrics.MonitorRequest(metrics.Metrics.BroadcasterClient, c.apiClient, req)
	if err != nil {
		return fmt.Errorf("Releasing Manifest ID failed. URL: %s, manifestID: %s, err: %s", log.RedactURL(requestURL), manifestId, err)
	}
	defer res.Body.Close() // nolint:errcheck

	if !httpOk(res.StatusCode) {
		return fmt.Errorf("Releasing Manifest ID failed. URL: %s, manifestID: %s, HTTP Code: %s", log.RedactURL(requestURL), manifestId, res.Status)
	}

	return nil
}

func (c *RemoteBroadcasterClient) pickRandomBroadcaster(list BroadcasterList) (url.URL, error) {
	if len(list) == 0 {
		return url.URL{}, fmt.Errorf("broadcaster list is empty")
	}
	chosen := list[rand.Intn(len(list))]
	result, err := url.Parse(chosen.Address)
	if err != nil {
		return url.URL{}, rejectDestination("broadcaster", "broadcaster entry is not a URL")
	}
	if err := ValidatePublicURL(result, "broadcaster", false, "https"); err != nil {
		return url.URL{}, err
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
	client.Logger = log.NewRetryableHTTPLogger()

	standardClient := client.StandardClient()
	if httpClient != nil {
		standardClient.CheckRedirect = httpClient.CheckRedirect
		standardClient.Jar = httpClient.Jar
		standardClient.Timeout = httpClient.Timeout
	}
	return standardClient
}
