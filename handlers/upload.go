package handlers

import (
	"encoding/json"
	errors2 "errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/catalyst-api/video"
	"github.com/xeipuuv/gojsonschema"
)

type UploadVODRequestOutputLocationOutputs struct {
	HLS           string `json:"hls"`
	MP4           string `json:"mp4"`
	FragmentedMP4 string `json:"fragmented_mp4"`
	Clip          string `json:"clip"`
	SourceMp4     bool   `json:"source_mp4"`
}

type UploadVODRequestOutputLocation struct {
	Type            string                                `json:"type"`
	URL             string                                `json:"url"`
	PinataAccessKey string                                `json:"pinata_access_key"`
	Outputs         UploadVODRequestOutputLocationOutputs `json:"outputs,omitempty"`
}

type UploadVODRequest struct {
	ExternalID      string                           `json:"external_id,omitempty"`
	Url             string                           `json:"url"`
	CallbackUrl     string                           `json:"callback_url"`
	OutputLocations []UploadVODRequestOutputLocation `json:"output_locations,omitempty"`
	AccessToken     string                           `json:"accessToken"`
	TranscodeAPIUrl string                           `json:"transcodeAPIUrl"`
	Encryption      *pipeline.EncryptionPayload      `json:"encryption,omitempty"`

	// Forwarded to transcoding stage:
	TargetSegmentSizeSecs int64                  `json:"target_segment_size_secs"`
	Profiles              []video.EncodedProfile `json:"profiles"`
	PipelineStrategy      pipeline.Strategy      `json:"pipeline_strategy"`

	// Forwarded to clipping stage:
	ClipStrategy video.ClipStrategy `json:"clip_strategy"`
}

type UploadVODResponse struct {
	RequestID string `json:"request_id"`
}

func HasContentType(r *http.Request, mimetype string) bool {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		return mimetype == "application/octet-stream"
	}

	for _, v := range strings.Split(contentType, ",") {
		t, _, err := mime.ParseMediaType(v)
		if err != nil {
			break
		}
		if t == mimetype {
			return true
		}
	}

	return false
}

func (r UploadVODRequest) IsProfileValid() bool {
	// an empty profile is valid and tells us to use the default ABR ladder
	if len(r.Profiles) == 0 {
		return true
	}

	// a special case where only the bitrate is set which tells us to
	// generate a profile that matches the input video's specs with the
	// user specified target bitrate
	if len(r.Profiles) == 1 {
		profile := r.Profiles[0]
		if profile.Width == 0 && profile.Height == 0 {
			return profile.Bitrate > 0
		}
	}

	// verify that width/height/bitrate is provided in cases where the
	// user wants to use their own transcode profile
	for _, profile := range r.Profiles {
		if profile.Width == 0 || profile.Height == 0 || profile.Bitrate == 0 {
			return false
		}
	}

	return true
}

func (r UploadVODRequest) IsClipValid() bool {
	startTime := r.ClipStrategy.StartTime
	endTime := r.ClipStrategy.EndTime
	if startTime < 0 || endTime <= 0 || startTime == endTime || startTime > endTime {
		return false
	}
	return true
}

func (r UploadVODRequest) getTargetHlsOutput() UploadVODRequestOutputLocation {
	for _, o := range r.OutputLocations {
		if o.Outputs.HLS == "enabled" {
			return o
		}
	}
	return UploadVODRequestOutputLocation{}
}

func (r UploadVODRequest) getTargetMp4Output() (UploadVODRequestOutputLocation, bool) {
	for _, o := range r.OutputLocations {
		if o.Outputs.MP4 == "enabled" {
			return o, false
		} else if o.Outputs.MP4 == "only_short" {
			return o, true
		}
	}
	return UploadVODRequestOutputLocation{}, false
}

func (r UploadVODRequest) getTargetFragMp4Output() UploadVODRequestOutputLocation {
	for _, o := range r.OutputLocations {
		if o.Outputs.FragmentedMP4 == "enabled" {
			return o
		}
	}
	return UploadVODRequestOutputLocation{}
}

func (r UploadVODRequest) getTargetClipOutput() UploadVODRequestOutputLocation {
	for _, o := range r.OutputLocations {
		if o.Outputs.Clip == "enabled" {
			return o
		}
	}
	return UploadVODRequestOutputLocation{}
}

func (r UploadVODRequest) getSourceCopyEnabled() bool {
	for _, o := range r.OutputLocations {
		if o.Outputs.SourceMp4 {
			return true
		}
	}
	return false
}

func (d *CatalystAPIHandlersCollection) UploadVOD() httprouter.Handle {
	schema := inputSchemasCompiled["UploadVOD"]
	m := metrics.Metrics

	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		m.UploadVODRequestCount.Inc()

		startTime := time.Now()
		success, apiError := d.handleUploadVOD(w, req, schema)

		status := http.StatusOK
		if !success {
			status = apiError.Status
		}
		m.UploadVODRequestDurationSec.
			WithLabelValues(strconv.FormatBool(success), fmt.Sprint(status), config.Version).
			Observe(time.Since(startTime).Seconds())
	}
}

func (d *CatalystAPIHandlersCollection) handleUploadVOD(w http.ResponseWriter, req *http.Request, schema *gojsonschema.Schema) (bool, errors.APIError) {
	var uploadVODRequest UploadVODRequest

	if !HasContentType(req, "application/json") {
		return false, errors.WriteHTTPUnsupportedMediaType(w, "Requires application/json content type", nil)
	} else if payload, err := io.ReadAll(req.Body); err != nil {
		return false, errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
	} else if result, err := schema.Validate(gojsonschema.NewBytesLoader(payload)); err != nil {
		return false, errors.WriteHTTPInternalServerError(w, "Cannot validate payload", err)
	} else if !result.Valid() {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("%s", result.Errors()))
	} else if err := json.Unmarshal(payload, &uploadVODRequest); err != nil {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
	}

	// Generate a Request ID that will be used throughout all logging
	var requestID = config.RandomTrailer(8)
	log.AddContext(requestID, "source", uploadVODRequest.Url, "external_id", uploadVODRequest.ExternalID)

	if err := CheckSourceURLValid(uploadVODRequest.Url); err != nil {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
	}

	if !uploadVODRequest.IsProfileValid() {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("invalid transcode profile requested"))
	}

	// If the segment size isn't being overridden then use the default
	if uploadVODRequest.TargetSegmentSizeSecs <= 0 {
		uploadVODRequest.TargetSegmentSizeSecs = config.DefaultSegmentSizeSecs
	}
	// Check that the override isn't too big
	if uploadVODRequest.TargetSegmentSizeSecs >= config.MaxSegmentSizeSecs {
		uploadVODRequest.TargetSegmentSizeSecs = config.MaxSegmentSizeSecs
	}
	log.AddContext(requestID, "target_segment_size_secs", uploadVODRequest.TargetSegmentSizeSecs)

	// Check if this is a clipping request
	var clipTargetURL *url.URL
	var err error
	if uploadVODRequest.IsClipValid() {
		clipTargetOutput := uploadVODRequest.getTargetClipOutput()
		clipTargetURL, err = toTargetURL(clipTargetOutput, requestID)
		if err != nil {
			return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
		}
		uploadVODRequest.ClipStrategy.Enabled = true
	}

	// Get target locatons for HLS, MP4, FMP4 outputs
	hlsTargetOutput := uploadVODRequest.getTargetHlsOutput()
	hlsTargetURL, err := toTargetURL(hlsTargetOutput, requestID)
	if err != nil {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
	}
	mp4TargetOutput, mp4OnlyShort := uploadVODRequest.getTargetMp4Output()
	mp4TargetURL, err := toTargetURL(mp4TargetOutput, requestID)
	if err != nil {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
	}
	fragMp4TargetOutput := uploadVODRequest.getTargetFragMp4Output()
	fragMp4TargetURL, err := toTargetURL(fragMp4TargetOutput, requestID)
	if err != nil {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
	}
	if hlsTargetURL == nil && mp4TargetURL == nil && fragMp4TargetURL == nil {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", errors2.New("none of output enabled: hls or mp4 or f-mp4"))
	}

	// Verify pipeline strategy
	if strat := uploadVODRequest.PipelineStrategy; strat != "" && !strat.IsValid() {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("invalid value provided for pipeline strategy: %q", uploadVODRequest.PipelineStrategy))
	}

	log.Log(requestID, "Received VOD Upload request", "pipeline_strategy", uploadVODRequest.PipelineStrategy, "num_profiles", len(uploadVODRequest.Profiles), "hlsTargetURL", hlsTargetURL)

	// Once we're happy with the request, do the rest of the Segmenting stage asynchronously to allow us to
	// from the API call and free up the HTTP connection
	d.VODEngine.StartUploadJob(pipeline.UploadJobPayload{
		SourceFile:            uploadVODRequest.Url,
		CallbackURL:           uploadVODRequest.CallbackUrl,
		HlsTargetURL:          hlsTargetURL,
		Mp4TargetURL:          mp4TargetURL,
		FragMp4TargetURL:      fragMp4TargetURL,
		ClipTargetURL:         clipTargetURL,
		Mp4OnlyShort:          mp4OnlyShort,
		AccessToken:           uploadVODRequest.AccessToken,
		TranscodeAPIUrl:       uploadVODRequest.TranscodeAPIUrl,
		RequestID:             requestID,
		ExternalID:            uploadVODRequest.ExternalID,
		Profiles:              uploadVODRequest.Profiles,
		PipelineStrategy:      uploadVODRequest.PipelineStrategy,
		TargetSegmentSizeSecs: uploadVODRequest.TargetSegmentSizeSecs,
		Encryption:            uploadVODRequest.Encryption,
		SourceCopy:            uploadVODRequest.getSourceCopyEnabled(),
		ClipStrategy:          uploadVODRequest.ClipStrategy,
	})

	respBytes, err := json.Marshal(UploadVODResponse{RequestID: requestID})
	if err != nil {
		log.LogError(requestID, "Failed to build a /upload HTTP API response", err)
		return false, errors.WriteHTTPInternalServerError(w, "Failed marshaling response", err)
	}

	if _, err := w.Write(respBytes); err != nil {
		log.LogError(requestID, "Failed to write a /upload HTTP API response", err)
		return false, errors.WriteHTTPInternalServerError(w, "Failed writing response", err)
	}

	return true, errors.APIError{}
}

func toTargetURL(ol UploadVODRequestOutputLocation, reqID string) (*url.URL, error) {
	if ol.URL != "" {
		tURL, err := url.Parse(ol.URL)
		if err != nil {
			return nil, err
		}

		// Hack for web3.storage to distinguish different jobs, before calling Publish()
		// Can be removed after we address this issue: https://github.com/livepeer/go-tools/issues/16
		if tURL.Scheme == "w3s" {
			tURL.Host = reqID
			log.AddContext(reqID, "w3s-url", tURL.String())
		}
		return tURL, nil
	}
	return nil, nil
}

func CheckSourceURLValid(sourceURL string) error {
	if sourceURL == "" {
		return fmt.Errorf("empty source URL")
	}

	u, err := url.Parse(sourceURL)
	if err != nil {
		return err
	}

	if strings.HasSuffix(u.Hostname(), ".local") {
		return fmt.Errorf(".local domains are not valid")
	}

	return nil
}
