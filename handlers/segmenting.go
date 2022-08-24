package handlers

import (
	"mime"
	"net/http"
	"strings"
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

var UploadVODRequestSchemaDefinition string = `{
	"type": "object",
	"properties": {
		"url": { "type": "string", "format": "uri" },
			"callback_url": { "type": "string", "format": "uri" },
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
}`
