package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/xeipuuv/gojsonschema"
)

type CatalystAPIHandlersCollection struct{}

var CatalystAPIHandlers = CatalystAPIHandlersCollection{}

func (d *CatalystAPIHandlersCollection) Ok() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		io.WriteString(w, "OK")
	}
}

func (d *CatalystAPIHandlersCollection) UploadVOD() httprouter.Handle {
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

		// Do something with uploadVODRequest
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
