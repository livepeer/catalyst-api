package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/livepeer/dms-api/errors"
	"github.com/livepeer/go-livepeer/drivers"
)

type DMSAPIHandlersCollection struct{}

var DMSAPIHandlers = DMSAPIHandlersCollection{}

func (d *DMSAPIHandlersCollection) Ok() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		io.WriteString(w, "OK")
	})
}

type UploadVOD struct {
	Url             url.URL
	CallbackUrl     url.URL
	Mp4Output       bool
	OutputLocations []drivers.OSDriver
}

func (d *DMSAPIHandlersCollection) UploadVOD() http.HandlerFunc {
	type UploadVODRequest struct {
		Url             *string   `json:"url,omitempty"`
		CallbackUrl     *string   `json:"callback_url,omitempty"`
		Mp4Output       *bool     `json:"mp4_output,omitempty"`
		OutputLocations *[]string `json:"output_locations,omitempty"`
	}

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		drivers.Testing = true
		if req.Method != "POST" {
			w.Header().Add("Allow", "POST")
			errors.WriteHTTPMethodNotAlloed(w, "Method not allowed", nil)
			return
		}

		if req.Header.Get("Content-Type") != "application/json" {
			errors.WriteHTTPBadRequest(w, "Unsupported content type", nil)
			return
		}

		var uploadVODRequest UploadVODRequest
		var payload []byte
		var err error

		if payload, err = ioutil.ReadAll(req.Body); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		}

		if err = json.Unmarshal(payload, &uploadVODRequest); err != nil {
			errors.WriteHTTPBadRequest(w, "Cannot unmarshal JSON to UploadVODRequest struct", err)
			return
		}

		if uploadVODRequest.Url == nil {
			errors.WriteHTTPBadRequest(w, "Missing url", nil)
			return
		}

		if uploadVODRequest.CallbackUrl == nil {
			errors.WriteHTTPBadRequest(w, "Missing callback_url", nil)
			return
		}

		if uploadVODRequest.OutputLocations == nil {
			errors.WriteHTTPBadRequest(w, "Missing output_locations", nil)
			return
		}

		var mp4Output bool
		if uploadVODRequest.Mp4Output == nil {
			mp4Output = false
		}

		var sourceUrl *url.URL
		sourceUrl, err = url.Parse(*uploadVODRequest.Url)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid url", err)
			return
		}

		var callbackUrl *url.URL
		callbackUrl, err = url.Parse(*uploadVODRequest.CallbackUrl)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid callback_url", err)
			return
		}

		if len(*uploadVODRequest.OutputLocations) == 0 {
			errors.WriteHTTPBadRequest(w, "Empty output_locations", nil)
			return
		}

		outputLocations := []drivers.OSDriver{}
		for _, location := range *uploadVODRequest.OutputLocations {
			if driver, err := drivers.ParseOSURL(location, true); err == nil {
				outputLocations = append(outputLocations, driver)
			} else {
				errors.WriteHTTPBadRequest(w, "Invalid output_locations entry", err)
				return
			}
		}

		uploadVod := UploadVOD{
			Url:             *sourceUrl,
			CallbackUrl:     *callbackUrl,
			Mp4Output:       mp4Output,
			OutputLocations: outputLocations,
		}

		// Do something with uploadVOD
		io.WriteString(w, fmt.Sprint(len(uploadVod.OutputLocations)))
	})
}
