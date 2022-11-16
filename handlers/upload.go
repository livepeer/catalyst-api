package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os/exec"
	"path"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/subprocess"
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

	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		var uploadVODRequest UploadVODRequest

		if !HasContentType(req, "application/json") {
			errors.WriteHTTPUnsupportedMediaType(w, "Requires application/json content type", nil)
			return
		} else if payload, err := io.ReadAll(req.Body); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		} else if result, err := schema.Validate(gojsonschema.NewBytesLoader(payload)); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot validate payload", err)
			return
		} else if !result.Valid() {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("%s", result.Errors()))
			return
		} else if err := json.Unmarshal(payload, &uploadVODRequest); err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}

		// Generate a Request ID that will be used throughout all logging
		var requestID = config.RandomTrailer(8)
		log.AddContext(requestID, "source", uploadVODRequest.Url)

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
			return
		}

		// Create a separate subdirectory for the source segments
		// Use the output directory specified in request as the output directory of transcoded renditions
		targetURL, err := url.Parse(tURL)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("target output file should end in .m3u8 extension"))
			return
		}

		targetDirPath := path.Dir(targetURL.Path)
		targetManifestFilename := path.Base(targetURL.String())
		targetExtension := path.Ext(targetManifestFilename)
		if targetExtension != ".m3u8" {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("target output file should end in .m3u8 extension"))
			return
		}

		targetSegmentedOutputPath := path.Join(targetDirPath, "source", targetManifestFilename)
		sout, err := url.Parse(targetSegmentedOutputPath)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot parse targetSegmentedOutputPath", err)
			return
		}

		targetSegmentedOutputURL := targetURL.ResolveReference(sout)
		log.AddContext(requestID, "segmented_url", targetSegmentedOutputURL.String())

		streamName := config.SegmentingStreamName(requestID)
		log.AddContext(requestID, "stream_name", streamName)

		// Arweave URLs don't support HTTP Range requests and so Mist can't natively handle them for segmenting
		// This workaround copies the file from Arweave to S3 and then tells Mist to use the S3 URL
		if clients.IsArweaveURL(uploadVODRequest.Url) {
			newSourceOutputPath := path.Join(targetDirPath, "source", "arweave-source.mp4")
			newSourceOutputPathURL, err := url.Parse(newSourceOutputPath)
			if err != nil {
				errors.WriteHTTPInternalServerError(w, "Cannot parse newSourceOutputPath", err)
				return
			}
			newSourceURL := targetURL.ResolveReference(newSourceOutputPathURL)
			log.AddContext(requestID, "new_source_url", newSourceURL.String())

			if err := clients.CopyArweaveToS3(uploadVODRequest.Url, newSourceURL.String()); err != nil {
				errors.WriteHTTPBadRequest(w, "Invalid Arweave URL", err)
				return
			}
			uploadVODRequest.Url = newSourceURL.String()
		}

		cache.DefaultStreamCache.Segmenting.Store(streamName, cache.StreamInfo{
			SourceFile:      uploadVODRequest.Url,
			CallbackURL:     uploadVODRequest.CallbackUrl,
			UploadURL:       targetSegmentedOutputURL.String(),
			AccessToken:     uploadVODRequest.AccessToken,
			TranscodeAPIUrl: uploadVODRequest.TranscodeAPIUrl,
			RequestID:       requestID,
		})

		// Attempt an out-of-band call to generate the dtsh headers using MistIn*
		err = createDtsh(requestID, uploadVODRequest.Url)
		if err != nil {
			log.LogError(requestID, "Failed to create dtsh", err, "destination", uploadVODRequest.Url)
		}

		log.Log(requestID, "Beginning segmenting")
		// Tell Mist to do the segmenting. Upon completion / error, Mist will call Triggers to notify us.
		if err := d.processUploadVOD(streamName, uploadVODRequest.Url, targetSegmentedOutputURL.String()); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot process upload VOD request", err)
			return
		}

		if err := clients.DefaultCallbackClient.SendTranscodeStatus(uploadVODRequest.CallbackUrl, clients.TranscodeStatusPreparing, 0.0); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot send transcode status", err)
		}

		respBytes, err := json.Marshal(UploadVODResponse{RequestID: requestID})
		if err != nil {
			log.LogError(requestID, "Failed to build a /upload HTTP API response", err)
			return
		}

		if _, err := w.Write(respBytes); err != nil {
			log.LogError(requestID, "Failed to write a /upload HTTP API response", err)
			return
		}

	}
}

func (d *CatalystAPIHandlersCollection) processUploadVOD(streamName, sourceURL, targetURL string) error {
	sourceURL = "mp4:" + sourceURL
	if err := d.MistClient.AddStream(streamName, sourceURL); err != nil {
		return err
	}
	if err := d.MistClient.PushStart(streamName, targetURL); err != nil {
		return err
	}

	return nil
}

func createDtsh(requestID, destination string) error {
	log.Log(requestID, "Generating DTSH header for ", destination)
	url, err := url.Parse(destination)
	if err != nil {
		return err
	}
	url.RawQuery = ""
	url.Fragment = ""
	headerPrepare := exec.Command(path.Join(config.PathMistDir, "MistInMP4"), "-H", url.String(), "-g", "5")
	if err = subprocess.LogOutputs(headerPrepare); err != nil {
		return err
	}
	if err = headerPrepare.Start(); err != nil {
		return err
	}
	go func() {
		if err := headerPrepare.Wait(); err != nil {
			log.LogError(requestID, "createDtsh return code", err, "destination", destination)
		}
	}()
	return nil
}
