package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/xeipuuv/gojsonschema"
)

type StreamInfo struct {
	callbackUrl string
}

type CatalystAPIHandlersCollection struct {
	MistClient  MistAPIClient
	StreamCache map[string]StreamInfo
}

func (d *CatalystAPIHandlersCollection) Ok() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		io.WriteString(w, "OK")
	}
}

func (d *CatalystAPIHandlersCollection) TranscodeSegment() httprouter.Handle {
	schema := inputSchemasCompiled["TranscodeSegment"]

	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		var transcodeRequest TranscodeSegmentRequest
		payload, err := ioutil.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read body", err)
			return
		}
		result, err := schema.Validate(gojsonschema.NewBytesLoader(payload))
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "body schema validation failed", err)
			return
		}
		if !result.Valid() {
			errors.WriteHTTPBadBodySchema("TranscodeSegment", w, result.Errors())
			return
		}
		if err := json.Unmarshal(payload, &transcodeRequest); err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}

		callbackClient := clients.NewCallbackClient()
		if err := callbackClient.SendTranscodeStatusError(transcodeRequest.CallbackUrl, "NYI - not yet implemented"); err != nil {
			errors.WriteHTTPInternalServerError(w, "error send transcode error", err)
		}
		io.WriteString(w, "OK") // TODO later
	}
}

func (d *CatalystAPIHandlersCollection) UploadVOD() httprouter.Handle {
	schema := inputSchemasCompiled["UploadVOD"]

	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		var uploadVODRequest UploadVODRequest

		if !HasContentType(req, "application/json") {
			errors.WriteHTTPUnsupportedMediaType(w, "Requires application/json content type", nil)
			return
		} else if payload, err := ioutil.ReadAll(req.Body); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		} else if result, err := schema.Validate(gojsonschema.NewBytesLoader(payload)); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot validate payload", err)
			return
		} else if !result.Valid() {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", nil)
			return
		} else if err := json.Unmarshal(payload, &uploadVODRequest); err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}

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

		streamName := randomStreamName("catalyst_vod_")
		d.StreamCache[streamName] = StreamInfo{callbackUrl: uploadVODRequest.CallbackUrl}

		// process the request
		if err := d.processUploadVOD(streamName, uploadVODRequest.Url, tURL); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot process upload VOD request", err)
		}

		callbackClient := clients.NewCallbackClient()
		if err := callbackClient.SendTranscodeStatus(uploadVODRequest.CallbackUrl, clients.TranscodeStatusPreparing, 0.0); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot send transcode status", err)
		}

		io.WriteString(w, fmt.Sprint(len(uploadVODRequest.OutputLocations)))
	}
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

func (d *CatalystAPIHandlersCollection) processUploadVOD(streamName, sourceURL, targetURL string) error {
	if err := d.MistClient.AddStream(streamName, sourceURL); err != nil {
		return err
	}
	if err := d.MistClient.AddTrigger(streamName, "PUSH_END"); err != nil {
		return err
	}
	if err := d.MistClient.PushStart(streamName, targetURL); err != nil {
		return err
	}

	return nil
}

func randomStreamName(prefix string) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	const length = 8
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = charset[r.Intn(length)]
	}
	return fmt.Sprintf("%s%s", prefix, string(res))
}

type MistCallbackHandlersCollection struct {
	MistClient  MistAPIClient
	StreamCache map[string]StreamInfo
}

func (d *MistCallbackHandlersCollection) Trigger() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		if t := req.Header.Get("X-Trigger"); t != "PUSH_END" {
			errors.WriteHTTPBadRequest(w, "Unsupported X-Trigger", fmt.Errorf("unknown trigger '%s'", t))
			return
		}
		payload, err := ioutil.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		}
		lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
		if len(lines) < 2 {
			errors.WriteHTTPBadRequest(w, "Bad request payload", fmt.Errorf("unknown payload '%s'", string(payload)))
			return
		}

		// stream name is the second line in the Mist Trigger payload
		s := lines[1]
		// when uploading is done, remove trigger and stream from Mist
		errT := d.MistClient.DeleteTrigger(s, "PUSH_END")
		errS := d.MistClient.DeleteStream(s)
		if errT != nil {
			errors.WriteHTTPInternalServerError(w, fmt.Sprintf("Cannot remove PUSH_END trigger for stream '%s'", s), errT)
			return
		}
		if errS != nil {
			errors.WriteHTTPInternalServerError(w, fmt.Sprintf("Cannot remove stream '%s'", s), errS)
			return
		}

		callbackClient := clients.NewCallbackClient()
		if err := callbackClient.SendTranscodeStatus(d.StreamCache[s].callbackUrl, clients.TranscodeStatusTranscoding, 0.0); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot send transcode status", err)
		}

		delete(d.StreamCache, s)

		// TODO: add timeout for the stream upload
		// TODO: start transcoding
	}
}
