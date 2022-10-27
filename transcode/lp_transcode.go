package transcode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"time"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/config"
)

const TRANSCODE_TIMEOUT = 10 * time.Second
const API_TIMEOUT = 10 * time.Second

type TranscodeResult struct {
	Renditions []*RenditionSegment
}

type RenditionSegment struct {
	Name      string
	MediaData []byte
	MediaUrl  *string
}

type createStreamPayload struct {
	Name     string                 `json:"name,omitempty"`
	Profiles []cache.EncodedProfile `json:"profiles"`
}

type LivepeerTranscodeConfiguration struct {
	Profiles []cache.EncodedProfile `json:"profiles"`
}

type Credentials struct {
	AccessToken  string `json:"access_token"`
	CustomAPIURL string `json:"custom_api_url"`
}

type BroadcasterList []struct {
	Address string `json:"address"`
}

type StreamAllocResponse struct {
	ManifestId string `json:"id"`
}

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

func (c *LocalBroadcasterClient) TranscodeSegment(segment io.Reader, sequenceNumber int64, durationMillis int64, profiles []cache.EncodedProfile) (TranscodeResult, error) {
	conf := LivepeerTranscodeConfiguration{}
	conf.Profiles = append(conf.Profiles, profiles...)
	transcodeConfig, err := json.Marshal(&conf)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("for local B, profiles json encode failed: %v", err)
	}

	return transcodeSegment(segment, sequenceNumber, durationMillis, c.broadcasterURL, config.RandomTrailer(), true, profiles, string(transcodeConfig))
}

type RemoteBroadcasterClient struct {
	credentials Credentials
}

func (c *RemoteBroadcasterClient) TranscodeSegmentWithRemoteBroadcaster(segment io.Reader, sequenceNumber int64, durationMillis int64, profiles []cache.EncodedProfile, streamName string) (TranscodeResult, error) {
	// Get available broadcasters
	bList, err := findBroadcaster(c.credentials)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("findBroadcaster failed %v", err)
	}

	manifestId, err := CreateStream(c.credentials, streamName, profiles)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("CreateStream(): %v", err)
	}
	defer ReleaseManifestId(c.credentials, manifestId)

	// Select one broadcaster
	broadcasterURL, err := pickRandomBroadcaster(bList)
	if err != nil {
		return TranscodeResult{}, fmt.Errorf("pickRandomBroadcaster failed %v", err)
	}

	return transcodeSegment(segment, sequenceNumber, durationMillis, broadcasterURL, manifestId, false, profiles, "")
}

// TranscodeSegment sends media to Livepeer network and returns rendition segments
// If manifestId == "" one will be created and deleted after use, pass real value to reuse across multiple calls
func transcodeSegment(inputSegment io.Reader, sequenceNumber, mediaDurationMillis int64, broadcasterURL url.URL, manifestId string, localBroadcaster bool, profiles []cache.EncodedProfile, transcodeConfigHeader string) (TranscodeResult, error) {
	t := TranscodeResult{}

	// Send segment to be transcoded
	client := &http.Client{
		Timeout: TRANSCODE_TIMEOUT,
		Transport: &http.Transport{
			DisableKeepAlives:  true,
			DisableCompression: true,
		},
	}
	requestURL, err := broadcasterURL.Parse(fmt.Sprintf("live/%s/%d.ts", manifestId, sequenceNumber))
	if err != nil {
		return t, fmt.Errorf("appending stream to broadcaster url %s: %v", broadcasterURL.String(), err)
	}
	req, err := http.NewRequest(http.MethodPost, requestURL.String(), inputSegment)
	if err != nil {
		return t, fmt.Errorf("NewRequest POST for url %s: %v", requestURL.String(), err)
	}
	req.Close = true
	req.ContentLength = -1
	req.TransferEncoding = append(req.TransferEncoding, "chunked")
	req.Header.Add("Content-Type", "video/mp2t")
	req.Header.Add("Accept", "multipart/mixed")
	req.Header.Add("Content-Duration", fmt.Sprintf("%d", mediaDurationMillis))
	if transcodeConfigHeader != "" {
		req.Header.Add("Livepeer-Transcode-Configuration", transcodeConfigHeader)

	}
	res, err := client.Do(req)
	if err != nil {
		return t, fmt.Errorf("http do(%s): %v", requestURL, err)
	}
	if !httpOk(res.StatusCode) {
		return t, fmt.Errorf("http POST(%s) returned %d %s", requestURL, res.StatusCode, res.Status)
	}
	mediaType, params, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if err != nil {
		return t, fmt.Errorf("http POST(%s) ParseMediaType(%s): %v", requestURL, res.Header.Get("Content-Type"), err)
	}
	if mediaType != "multipart/mixed" {
		return t, fmt.Errorf("http POST(%s) mediaType === %s", requestURL, mediaType)
	}
	// parse multipart body and return response
	mr := multipart.NewReader(res.Body, params["boundary"])
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return t, fmt.Errorf("multipart.NextPart() error: %v", err)
		}
		mediaType, _, err = mime.ParseMediaType(part.Header.Get("Content-Type"))
		if err != nil {
			return t, fmt.Errorf("multipart mime.ParseMediaType() error: %v; headers=%v", err, part.Header)
		}
		body, err := io.ReadAll(part)
		if err != nil {
			return t, fmt.Errorf("multipart io.ReadAll(part) error: %v; headers=%v", err, part.Header)
		}
		if mediaType == "application/vnd+livepeer.uri" {
			renditionUrl := string(body)
			rendition := RenditionSegment{
				Name:     part.Header.Get("Rendition-Name"),
				MediaUrl: &renditionUrl,
			}
			t.Renditions = append(t.Renditions, &rendition)
		} else {
			rendition := RenditionSegment{
				Name:      part.Header.Get("Rendition-Name"),
				MediaData: body,
			}
			t.Renditions = append(t.Renditions, &rendition)
		}
	}
	return t, nil
}

// findBroadcaster contacts Livepeer API for a broadcaster to use if localBroadcaster is not defined
func findBroadcaster(c Credentials) (BroadcasterList, error) {
	if c.AccessToken == "" || c.CustomAPIURL == "" {
		return BroadcasterList{}, fmt.Errorf("empty credentials")
	}
	client := &http.Client{
		Timeout: API_TIMEOUT,
		Transport: &http.Transport{
			DisableKeepAlives:  true,
			DisableCompression: true,
		},
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
	res, err := client.Do(req)
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
func CreateStream(c Credentials, streamName string, profiles []cache.EncodedProfile) (string, error) {
	client := &http.Client{
		Timeout: API_TIMEOUT,
		Transport: &http.Transport{
			DisableKeepAlives:  true,
			DisableCompression: true,
		},
	}
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
	res, err := client.Do(req)
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

// ReleaseManifestId deletes manifestId created by prior call to CreateStream()
func ReleaseManifestId(c Credentials, manifestId string) {
	client := &http.Client{
		Timeout: API_TIMEOUT,
		Transport: &http.Transport{
			DisableKeepAlives:  true,
			DisableCompression: true,
		},
	}
	requestURL, err := url.JoinPath(c.CustomAPIURL, fmt.Sprintf("stream/%s", manifestId))
	if err != nil {
		_ = config.Logger.Log("msg", "error construct api url", "api", c.CustomAPIURL, "manifestId", manifestId)
		return
	}
	req, err := http.NewRequest(http.MethodDelete, requestURL, nil)
	if err != nil {
		_ = config.Logger.Log("msg", "NewRequest DELETE", "url", requestURL, "manifestId", manifestId)
		return
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))
	res, err := client.Do(req)
	if err != nil {
		_ = config.Logger.Log("msg", "error deleting stream", "url", requestURL, "manifestId", manifestId, "err", err)
		return
	}
	if !httpOk(res.StatusCode) {
		_ = config.Logger.Log("msg", "error deleting stream", "url", requestURL, "manifestId", manifestId, "status", res.StatusCode, "txt", res.Status)
		return
	}
}

func pickRandomBroadcaster(list BroadcasterList) (url.URL, error) {
	chosen := list[rand.Intn(len(list))]
	result, err := url.Parse(chosen.Address)
	if err != nil {
		return url.URL{}, fmt.Errorf("broadcaster entry %s is not a URL %v", chosen.Address, err)
	}
	return *result, nil
}

func httpOk(statusCode int) bool {
	return statusCode >= 200 && statusCode < 300
}
