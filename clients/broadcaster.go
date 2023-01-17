package clients

import (
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/livepeer/catalyst-api/metrics"
)

// Broadcaster-node already has built-in retry logic. We want to rely on that and set generous timeout here:
const TRANSCODE_TIMEOUT = 3 * time.Minute

const API_TIMEOUT = 10 * time.Second

const (
	MIN_VIDEO_BITRATE          = 100_000
	ABSOLUTE_MIN_VIDEO_BITRATE = 5_000
)

type TranscodeResult struct {
	Renditions []*RenditionSegment
}

type RenditionSegment struct {
	Name      string
	MediaData []byte
	MediaUrl  *string
}

type createStreamPayload struct {
	Name     string           `json:"name,omitempty"`
	Profiles []EncodedProfile `json:"profiles"`
}

type LivepeerTranscodeConfiguration struct {
	Profiles          []EncodedProfile `json:"profiles"`
	TimeoutMultiplier int              `json:"timeoutMultiplier"`
}

type Credentials struct {
	AccessToken  string `json:"access_token"`
	CustomAPIURL string `json:"custom_url"`
}

type BroadcasterList []struct {
	Address string `json:"address"`
}

type StreamAllocResponse struct {
	ManifestId string `json:"id"`
}

type EncodedProfile struct {
	Name         string `json:"name,omitempty"`
	Width        int64  `json:"width,omitempty"`
	Height       int64  `json:"height,omitempty"`
	Bitrate      int64  `json:"bitrate,omitempty"`
	FPS          int64  `json:"fps"`
	FPSDen       int64  `json:"fpsDen,omitempty"`
	Profile      string `json:"profile,omitempty"`
	GOP          string `json:"gop,omitempty"`
	Encoder      string `json:"encoder,omitempty"`
	ColorDepth   int64  `json:"colorDepth,omitempty"`
	ChromaFormat int64  `json:"chromaFormat,omitempty"`
}

// DefaultTranscodeProfiles defines the default set of encoding profiles to use when none are specified
var DefaultTranscodeProfiles = []EncodedProfile{
	// {
	// 	Name:    "240p0",
	// 	FPS:     0,
	// 	Bitrate: 250_000,
	// 	Width:   426,
	// 	Height:  240,
	// },
	{
		Name:    "360p0",
		FPS:     0,
		Bitrate: 800_000,
		Width:   640,
		Height:  360,
	},
	// {
	// 	Name:    "480p0",
	// 	FPS:     0,
	// 	Bitrate: 1_600_000,
	// 	Width:   854,
	// 	Height:  480,
	// },
	{
		Name:    "720p0",
		FPS:     0,
		Bitrate: 3_000_000,
		Width:   1280,
		Height:  720,
	},
}

func GetPlaybackProfiles(iv InputVideo) ([]EncodedProfile, error) {
	video, err := iv.GetVideoTrack()
	if err != nil {
		return nil, fmt.Errorf("no video track found in input video: %w", err)
	}
	profiles := make([]EncodedProfile, 0, len(DefaultTranscodeProfiles)+1)
	for _, profile := range DefaultTranscodeProfiles {
		// transcoding job will adjust the width to match aspect ratio. no need to
		// check it here.
		lowerQualityThanSrc := profile.Height <= video.Height && profile.Bitrate < video.Bitrate
		if lowerQualityThanSrc {
			profiles = append(profiles, profile)
		}
	}
	if len(profiles) == 0 {
		profiles = []EncodedProfile{lowBitrateProfile(video)}
	}
	profiles = append(profiles, EncodedProfile{
		Name:    strconv.FormatInt(video.Height, 10) + "p0",
		Bitrate: video.Bitrate,
		FPS:     0,
		Width:   video.Width,
		Height:  video.Height,
	})
	return profiles, nil
}

func lowBitrateProfile(video InputTrack) EncodedProfile {
	bitrate := int64(float64(video.Bitrate) * (1.0 / 2.0))
	if bitrate < MIN_VIDEO_BITRATE && video.Bitrate > MIN_VIDEO_BITRATE {
		bitrate = MIN_VIDEO_BITRATE
	} else if bitrate < ABSOLUTE_MIN_VIDEO_BITRATE {
		bitrate = ABSOLUTE_MIN_VIDEO_BITRATE
	}
	return EncodedProfile{
		Name:    "low-bitrate",
		FPS:     0,
		Bitrate: bitrate,
		Width:   video.Width,
		Height:  video.Height,
	}
}

var client = newRetryableClient(&http.Client{Timeout: TRANSCODE_TIMEOUT})

// TranscodeSegment sends media to Livepeer network and returns rendition segments
// If manifestId == "" one will be created and deleted after use, pass real value to reuse across multiple calls
func transcodeSegment(inputSegment io.Reader, sequenceNumber, mediaDurationMillis int64, broadcasterURL url.URL, manifestId string, profiles []EncodedProfile, transcodeConfigHeader string) (TranscodeResult, error) {
	t := TranscodeResult{}

	// Send segment to be transcoded
	requestURL, err := broadcasterURL.Parse(fmt.Sprintf("live/%s/%d.ts", manifestId, sequenceNumber))
	if err != nil {
		return t, fmt.Errorf("appending stream to broadcaster url %s: %v", broadcasterURL.String(), err)
	}
	req, err := http.NewRequest(http.MethodPost, requestURL.String(), inputSegment)
	if err != nil {
		return t, fmt.Errorf("NewRequest POST for url %s: %v", requestURL.String(), err)
	}
	req.TransferEncoding = append(req.TransferEncoding, "chunked")
	req.Header.Add("Content-Type", "video/mp2t")
	req.Header.Add("Accept", "multipart/mixed")
	req.Header.Add("Content-Duration", fmt.Sprintf("%d", mediaDurationMillis))
	if transcodeConfigHeader != "" {
		req.Header.Add("Livepeer-Transcode-Configuration", transcodeConfigHeader)

	}
	res, err := metrics.MonitorRequest(metrics.Metrics.BroadcasterClient, client, req)
	if err != nil {
		return t, fmt.Errorf("http do(%s): %v", requestURL, err)
	}
	defer res.Body.Close()

	if !httpOk(res.StatusCode) {
		// Read the body, because the B sometimes returns error information in there
		// Swallow any error reading the body since there's nothing we can do about it
		b, _ := io.ReadAll(res.Body)
		bodyString := string(b)

		// Don't include the body in the error if it's too long, since this gets used in logs and callbacks
		if len(bodyString) > 10_000 {
			bodyString = "<Too long to include in error>"
		}

		return t, fmt.Errorf("http POST(%s) returned %d %s. Response Body: %s", requestURL, res.StatusCode, res.Status, bodyString)
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

func httpOk(statusCode int) bool {
	return statusCode >= 200 && statusCode < 300
}
