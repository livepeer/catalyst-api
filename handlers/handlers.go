package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/dms-api/errors"
	"github.com/xeipuuv/gojsonschema"
)

type DMSAPIHandlersCollection struct{}

var DMSAPIHandlers = DMSAPIHandlersCollection{}

func (d *DMSAPIHandlersCollection) Ok() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		io.WriteString(w, "OK")
	}
}

func (d *DMSAPIHandlersCollection) UploadVOD() httprouter.Handle {
	schemaLoader := gojsonschema.NewStringLoader(`{
		"type": "object",
		"properties": {
			"url": { "type": "string", "format": "uri" },
		  	"callback_url": { "type": "string", "format": "uri" },
		  	"mp4_output": { "type": "boolean" },
		  	"output_locations": {
				"type": "array",
				"items": {
					"oneOf": [
						{
							"type": "object",
			  				"properties": {
								"type": { "type": "string", "const": "object_store" },
								"url": { "type": "string", "format": "uri" }
				  			},
							"required": [ "type", "url" ],
							"additional_properties": false
						},
						{
							"type": "object",
			  				"properties": {
								"type": { "type": "string", "const": "pinata" },
								"pinata_access_key": { "type": "string", "minLength": 1 }
				  			},
							"required": [ "type", "pinata_access_key" ],
							"additional_properties": false
						}
					]
				},
				"minItems": 1
		  	}
		},
		"required": [ "url", "callback_url", "output_locations" ],
		"additional_properties": false
	}`)

	schema, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		panic(err)
	}

	type UploadVODRequest struct {
		Url             string `json:"url"`
		CallbackUrl     string `json:"callback_url"`
		Mp4Output       bool   `json:"mp4_output"`
		OutputLocations []struct {
			uploadSource    bool
			Type            string `json:"type"`
			URL             string `json:"url"`
			PinataAccessKey string `json:"pinata_access_key"`
		} `json:"output_locations,omitempty"`
	}

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

		if err := processUploadVOD(uploadVODRequest.Url); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot process upload VOD request", err)
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

func processUploadVOD(url string) error {
	// TODO: Update hostnames and ports
	mc := MistClient{apiUrl: "http://localhost:4242/api2", triggerCallback: "http://host.docker.internal:8080/api/mist/trigger"}

	streamName := randomStreamName("catalyst_vod_")
	if err := mc.AddStream(streamName, url); err != nil {
		return err
	}

	// TODO: Move it to `Trigger()`
	defer mc.DeleteStream(streamName)

	if err := mc.AddTrigger(streamName, "PUSH_END"); err != nil {
		return err
	}
	// TODO: Move it to `Trigger()`
	defer mc.DeleteTrigger(streamName, "PUSH_END")

	if err := mc.AddTrigger(streamName, "RECORDING_END"); err != nil {
		return err
	}

	// TODO: Move it to `Trigger()`
	defer mc.DeleteTrigger(streamName, "RECORDING_END")

	// TODO: Change the output to the value from the request instead of the hardcoded "/media/recording/result.ts"
	if err := mc.PushStart(streamName, "/media/recording/result.ts"); err != nil {
		return err
	}

	// TODO: After moving cleanup to `Trigger()`, this is no longer needed
	time.Sleep(10 * time.Second)

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

type MistCallbackHandlersCollection struct{}

var MistCallbackHandlers = MistCallbackHandlersCollection{}

func (d *MistCallbackHandlersCollection) Trigger() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		log.Println("Received Mist Trigger")
		payload, err := ioutil.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		}

		// TODO: Handle trigger results: 1) Check the trigger name, 2) Call callbackURL, 3) Perform stream cleanup
		fmt.Println(string(payload))
		io.WriteString(w, "OK")
	}
}
