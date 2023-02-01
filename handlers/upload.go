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

type UploadVODRequest struct {
	Url             string `json:"url"`
	CallbackUrl     string `json:"callback_url"`
	OutputLocations []struct {
		Type            string `json:"type"`
		URL             string `json:"url"`
		PinataAccessKey string `json:"pinata_access_key"`
		Outputs         struct {
			SourceMp4          bool `json:"source_mp4"`
			SourceSegments     bool `json:"source_segments"`
			TranscodedSegments bool `json:"transcoded_segments"`
		} `json:"outputs,omitempty"`
	} `json:"output_locations,omitempty"`
	AccessToken     string `json:"accessToken"`
	TranscodeAPIUrl string `json:"transcodeAPIUrl"`
	// Forwarded to transcoding stage:
	Profiles         []video.EncodedProfile `json:"profiles"`
	PipelineStrategy pipeline.Strategy      `json:"pipeline_strategy"`
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

	// find output storage URLs for source and target segments
	var useTargetForSourceOutput bool
	var tURL string
	for _, o := range uploadVODRequest.OutputLocations {
		tURL = o.URL
		if o.Outputs.SourceSegments {
			useTargetForSourceOutput = true
			break
		}
	}
	if tURL == "" {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("no output URL in request"))
	}

	// Create a separate subdirectory for the source segments
	// Use the output directory specified in request as the output directory of transcoded renditions
	targetURL, err := url.Parse(tURL)
	if err != nil {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("target output file should end in .m3u8 extension"))
	}
	// Hack for web3.storage to distinguish different jobs, before calling Publish()
	// Can be removed after we address this issue: https://github.com/livepeer/go-tools/issues/16
	if targetURL.Scheme == "w3s" {
		targetURL.Host = requestID
	}

	var sourceOutputURL *url.URL
	if useTargetForSourceOutput {
		sourceOutputURL = targetURL
	}
	if strat := uploadVODRequest.PipelineStrategy; strat != "" && !strat.IsValid() {
		return false, errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("invalid value provided for pipeline strategy: %q", uploadVODRequest.PipelineStrategy))
	}

	log.Log(requestID, "Received VOD Upload request", "pipeline_strategy", uploadVODRequest.PipelineStrategy, "num_profiles", len(uploadVODRequest.Profiles))

	// Once we're happy with the request, do the rest of the Segmenting stage asynchronously to allow us to
	// from the API call and free up the HTTP connection
	d.VODEngine.StartUploadJob(pipeline.UploadJobPayload{
		SourceFile:       uploadVODRequest.Url,
		CallbackURL:      uploadVODRequest.CallbackUrl,
		SourceOutputURL:  sourceOutputURL,
		TargetURL:        targetURL,
		AccessToken:      uploadVODRequest.AccessToken,
		TranscodeAPIUrl:  uploadVODRequest.TranscodeAPIUrl,
		RequestID:        requestID,
		Profiles:         uploadVODRequest.Profiles,
		PipelineStrategy: uploadVODRequest.PipelineStrategy,
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
