package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/pipeline"
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
	Profiles []clients.EncodedProfile `json:"profiles"`
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
		var uploadVODRequest UploadVODRequest

		if !HasContentType(req, "application/json") {
			errors.WriteHTTPUnsupportedMediaType(w, "Requires application/json content type", nil)
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusBadRequest)).Inc()
			return
		} else if payload, err := io.ReadAll(req.Body); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusInternalServerError)).Inc()
			return
		} else if result, err := schema.Validate(gojsonschema.NewBytesLoader(payload)); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot validate payload", err)
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusInternalServerError)).Inc()
			return
		} else if !result.Valid() {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("%s", result.Errors()))
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusBadRequest)).Inc()
			return
		} else if err := json.Unmarshal(payload, &uploadVODRequest); err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusBadRequest)).Inc()
			return
		}

		// Generate a Request ID that will be used throughout all logging
		var requestID = config.RandomTrailer(8)
		log.AddContext(requestID, "source", uploadVODRequest.Url)

		httpURL, err := dStorageToHTTP(uploadVODRequest.Url)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "error in applyInputGateway()", err)
			return
		}
		uploadVODRequest.Url = httpURL

		// find source segment URL
		var tURL string
		for _, o := range uploadVODRequest.OutputLocations {
			if o.Outputs.SourceSegments {
				tURL = o.URL
				break
			}
		}
		if tURL == "" {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("no source segment URL in request"))
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusBadRequest)).Inc()
			return
		}

		// Create a separate subdirectory for the source segments
		// Use the output directory specified in request as the output directory of transcoded renditions
		targetURL, err := url.Parse(tURL)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("target output file should end in .m3u8 extension"))
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusBadRequest)).Inc()
			return
		}

		targetManifestFilename := path.Base(targetURL.String())
		targetExtension := path.Ext(targetManifestFilename)
		if targetExtension != ".m3u8" {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("target output file should end in .m3u8 extension"))
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusBadRequest)).Inc()
			return
		}

		targetSegmentedOutputURL, err := pipeline.InSameDirectory(targetURL, "source", targetManifestFilename)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot create targetSegmentedOutputURL", err)
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusInternalServerError)).Inc()
			return
		}
		log.AddContext(requestID, "segmented_url", targetSegmentedOutputURL.String())

		// Once we're happy with the request, do the rest of the Segmenting stage asynchronously to allow us to
		// from the API call and free up the HTTP connection
		d.VODEngine.CreateUploadJob(pipeline.UploadJobPayload{
			SourceFile:          uploadVODRequest.Url,
			CallbackURL:         uploadVODRequest.CallbackUrl,
			TargetURL:           targetURL,
			SegmentingTargetURL: targetSegmentedOutputURL.String(),
			AccessToken:         uploadVODRequest.AccessToken,
			TranscodeAPIUrl:     uploadVODRequest.TranscodeAPIUrl,
			RequestID:           requestID,
			Profiles:            uploadVODRequest.Profiles,
		})

		respBytes, err := json.Marshal(UploadVODResponse{RequestID: requestID})
		if err != nil {
			log.LogError(requestID, "Failed to build a /upload HTTP API response", err)
			errors.WriteHTTPInternalServerError(w, "Failed marshaling response", err)
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusInternalServerError)).Inc()
			return
		}

		if _, err := w.Write(respBytes); err != nil {
			log.LogError(requestID, "Failed to write a /upload HTTP API response", err)
			errors.WriteHTTPInternalServerError(w, "Failed writing response", err)
			m.UploadVODFailureCount.WithLabelValues(fmt.Sprint(http.StatusInternalServerError)).Inc()
			return
		}

		m.UploadVODSuccessCount.Inc()
	}
}

const SCHEME_IPFS = "ipfs"
const SCHEME_ARWEAVE = "ar"

func dStorageToHTTP(inputUrl string) (string, error) {
	sourceUrl, err := url.Parse(inputUrl)
	if err != nil {
		return inputUrl, err
	}

	switch sourceUrl.Scheme {
	case SCHEME_IPFS:
		return fmt.Sprintf("https://cloudflare-ipfs.com/ipfs/%s", sourceUrl.Host), nil
	case SCHEME_ARWEAVE:
		return fmt.Sprintf("https://arweave.net/%s", sourceUrl.Host), nil
	}
	return inputUrl, nil
}
