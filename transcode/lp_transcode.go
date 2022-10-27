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
	"strconv"
	"time"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/config"
)

// TranscodeSegment sends media to Livepeer network and returns rendition segments
// If manifestId == "" one will be created and deleted after use, pass real value to reuse across multiple calls
func TranscodeSegment(r SegmentTranscodeRequest, manifestId string) (TranscodeResult, error) {
	t := TranscodeResult{}
	// Get available bradcasters
	bList, err := findBroadcaster(r)
	if err != nil {
		return t, fmt.Errorf("findBroadcaster failed %v", err)
	}

	if manifestId == "" {
		// Get streamName (manifestId)
		if r.isLocalBroadcaster() {
			manifestId = config.RandomTrailer()
		} else {
			manifestId, err = CreateStream(r)
			if err != nil {
				return t, fmt.Errorf("CreateStream(): %v", err)
			}
			defer ReleaseManifestId(r, manifestId)
		}
	}

	// Select one broadcaster
	bUrl, err := pickRandomBroadcaster(bList)
	if err != nil {
		return t, fmt.Errorf("pickRandomBroadcaster failed %v", err)
	}

	// Send segment to be transcoded
	client := &http.Client{
		Timeout: r.TranscodeTimeout,
		Transport: &http.Transport{
			DisableKeepAlives:  true,
			DisableCompression: true,
		},
	}
	requestURL, err := bUrl.Parse(fmt.Sprintf("live/%s/%d.ts", manifestId, r.SequenceNumber))
	if err != nil {
		return t, fmt.Errorf("appending stream to api url %s: %v", r.Credentials.CustomApiUrl, err)
	}
	req, err := http.NewRequest(http.MethodPost, requestURL.String(), r.MediaDataReader)
	if err != nil {
		return t, fmt.Errorf("NewRequest POST for url %s: %v", requestURL.String(), err)
	}
	req.Close = true
	req.ContentLength = -1
	req.TransferEncoding = append(req.TransferEncoding, "chunked")
	req.Header.Add("Content-Type", "video/mp2t")
	req.Header.Add("Accept", "multipart/mixed")
	req.Header.Add("Content-Duration", strconv.FormatInt(r.MediaDurationMs, 10))
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
func findBroadcaster(r SegmentTranscodeRequest) (BroadcasterList, error) {
	if r.isLocalBroadcaster() {
		// We use random ManifestId
		return *r.LocalBroadcaster, nil
	}
	if r.Credentials.AccessToken == "" || r.Credentials.CustomApiUrl == "" {
		return BroadcasterList{}, fmt.Errorf("empty credentials")
	}
	client := &http.Client{
		Timeout: r.ApiTimeout,
		Transport: &http.Transport{
			DisableKeepAlives:  true,
			DisableCompression: true,
		},
	}
	requestURL, err := url.JoinPath(r.Credentials.CustomApiUrl, "broadcaster")
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("appending broadcaster to api url %s: %v", r.Credentials.CustomApiUrl, err)
	}
	req, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		return BroadcasterList{}, fmt.Errorf("NewRequest GET for url %s: %v", requestURL, err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", r.Credentials.AccessToken))
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
func CreateStream(r SegmentTranscodeRequest) (string, error) {
	client := &http.Client{
		Timeout: r.ApiTimeout,
		Transport: &http.Transport{
			DisableKeepAlives:  true,
			DisableCompression: true,
		},
	}
	requestURL, err := url.JoinPath(r.Credentials.CustomApiUrl, "stream")
	if err != nil {
		return "", fmt.Errorf("appending stream to api url %s: %v", r.Credentials.CustomApiUrl, err)
	}
	// prepare payload
	payload := createStreamPayload{Name: r.StreamName}
	payload.Profiles = append(payload.Profiles, r.TargetProfiles...)
	payloadBytes, err := json.Marshal(&payload)
	if err != nil {
		return "", fmt.Errorf("POST url=%s json encode error %v struct=%v", requestURL, err, payload)
	}
	req, err := http.NewRequest(http.MethodPost, requestURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", fmt.Errorf("NewRequest POST for url %s: %v", requestURL, err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", r.Credentials.AccessToken))
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
func ReleaseManifestId(r SegmentTranscodeRequest, manifestId string) {
	client := &http.Client{
		Timeout: r.ApiTimeout,
		Transport: &http.Transport{
			DisableKeepAlives:  true,
			DisableCompression: true,
		},
	}
	requestURL, err := url.JoinPath(r.Credentials.CustomApiUrl, fmt.Sprintf("stream/%s", manifestId))
	if err != nil {
		_ = config.Logger.Log("msg", "error construct api url", "api", r.Credentials.CustomApiUrl, "manifestId", manifestId)
		return
	}
	req, err := http.NewRequest(http.MethodDelete, requestURL, nil)
	if err != nil {
		_ = config.Logger.Log("msg", "NewRequest DELETE", "url", requestURL, "manifestId", manifestId)
		return
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", r.Credentials.AccessToken))
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

type SegmentTranscodeRequest struct {
	Credentials      Credentials
	StreamName       string
	SequenceNumber   int64
	MediaDataReader  io.Reader // mpegts encoded segment Reader
	MediaDurationMs  int64
	TargetProfiles   []cache.EncodedProfile
	LocalBroadcaster *BroadcasterList
	TranscodeTimeout time.Duration
	ApiTimeout       time.Duration
}

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

func (r *SegmentTranscodeRequest) isLocalBroadcaster() bool { return r.LocalBroadcaster != nil }

type Credentials struct {
	AccessToken  string `json:"access_token"`
	CustomApiUrl string `json:"custom_api_url"`
}

type BroadcasterList []struct {
	Address string `json:"address"`
}

type StreamAllocResponse struct {
	ManifestId string `json:"id"`
}
