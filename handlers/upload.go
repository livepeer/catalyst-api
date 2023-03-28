package handlers

import (
	"encoding/json"
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
	HLS string `json:"hls"`
	MP4 string `json:"mp4"`
}

type UploadVODRequestOutputLocation struct {
	Type            string                                `json:"type"`
	URL             string                                `json:"url"`
	PinataAccessKey string                                `json:"pinata_access_key"`
	Outputs         UploadVODRequestOutputLocationOutputs `json:"outputs,omitempty"`
}

type UploadVODRequest struct {
	Url             string                           `json:"url"`
	CallbackUrl     string                           `json:"callback_url"`
	OutputLocations []UploadVODRequestOutputLocation `json:"output_locations,omitempty"`
	AccessToken     string                           `json:"accessToken"`
	TranscodeAPIUrl string                           `json:"transcodeAPIUrl"`

	// Forwarded to transcoding stage:
	TargetSegmentSizeSecs int64                  `json:"target_segment_size_secs"`
	Profiles              []video.EncodedProfile `json:"profiles"`
	PipelineStrategy      pipeline.Strategy      `json:"pipeline_strategy"`
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
	log.AddContext(requestID, "source", uploadVODRequest.Url)

	if err := CheckSourceURLValid(uploadVODRequest.Url); err != nil {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
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

	var hlsTargetURL *url.URL
	hlsTargetOutput := uploadVODRequest.getTargetHlsOutput()
	if hlsTargetOutput.URL != "" {
		var err error
		hlsTargetURL, err = url.Parse(hlsTargetOutput.URL)
		if err != nil {
			return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
		}
		// Hack for web3.storage to distinguish different jobs, before calling Publish()
		// Can be removed after we address this issue: https://github.com/livepeer/go-tools/issues/16
		if hlsTargetURL.Scheme == "w3s" {
			hlsTargetURL.Host = requestID
			log.AddContext(requestID, "w3s-url", hlsTargetURL.String())
		}
	}

	var mp4TargetURL *url.URL
	mp4TargetOutput, mp4OnlyShort := uploadVODRequest.getTargetMp4Output()
	if mp4TargetOutput.URL != "" {
		var err error
		mp4TargetURL, err = url.Parse(mp4TargetOutput.URL)
		if err != nil {
			return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
		}
		// Hack for web3.storage to distinguish different jobs, before calling Publish()
		// Can be removed after we address this issue: https://github.com/livepeer/go-tools/issues/16
		if mp4TargetURL.Scheme == "w3s" {
			mp4TargetURL.Host = requestID
			log.AddContext(requestID, "w3s-url", mp4TargetURL.String())
		}
	}

	if strat := uploadVODRequest.PipelineStrategy; strat != "" && !strat.IsValid() {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("invalid value provided for pipeline strategy: %q", uploadVODRequest.PipelineStrategy))
	}

	log.Log(requestID, "Received VOD Upload request", "pipeline_strategy", uploadVODRequest.PipelineStrategy, "num_profiles", len(uploadVODRequest.Profiles))

	// Once we're happy with the request, do the rest of the Segmenting stage asynchronously to allow us to
	// from the API call and free up the HTTP connection

	d.VODEngine.StartUploadJob(pipeline.UploadJobPayload{
		SourceFile:            uploadVODRequest.Url,
		CallbackURL:           uploadVODRequest.CallbackUrl,
		HlsTargetURL:          hlsTargetURL,
		Mp4TargetURL:          mp4TargetURL,
		Mp4OnlyShort:          mp4OnlyShort,
		AccessToken:           uploadVODRequest.AccessToken,
		TranscodeAPIUrl:       uploadVODRequest.TranscodeAPIUrl,
		RequestID:             requestID,
		Profiles:              uploadVODRequest.Profiles,
		PipelineStrategy:      uploadVODRequest.PipelineStrategy,
		TargetSegmentSizeSecs: uploadVODRequest.TargetSegmentSizeSecs,
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
