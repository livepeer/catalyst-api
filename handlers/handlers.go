package handlers

import (
	"encoding/json"
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

type OSURL struct {
	drivers.OSDriver
}

type URL struct {
	*url.URL
}

func (j *OSURL) UnmarshalJSON(b []byte) error {
	driver, err := drivers.ParseOSURL(string(b[1:len(b)-1]), true)

	if err == nil {
		j.OSDriver = driver
	}

	return err
}

func (j *URL) UnmarshalJSON(b []byte) error {
	url, err := url.Parse(string(b[1 : len(b)-1]))

	if err == nil {
		j.URL = url
	}

	return err
}

type UploadVODRequest struct {
	Url             URL     `json:"url,omitempty"`
	CallbackUrl     URL     `json:"callback_url,omitempty"`
	OutputLocations []OSURL `json:"output_locations,omitempty"`
	Mp4Output       bool    `json:"mp4_output,omitempty"`
}

func (d *DMSAPIHandlersCollection) UploadVOD() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			w.Header().Add("Allow", "POST")
			errors.WriteHTTPMethodNotAlloed(w, "Method Not Allowed", nil)
			return
		}

		if req.Header.Get("Content-Type") != "application/json" {
			errors.WriteHTTPBadRequest(w, "Unsupported content type", nil)
			return
		}

		var uploadVOD UploadVODRequest
		var payload []byte
		var err error

		if payload, err = ioutil.ReadAll(req.Body); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		}

		if err = json.Unmarshal(payload, &uploadVOD); err != nil {
			errors.WriteHTTPBadRequest(w, "Cannot unmarshal JSON to UploadVODRequest struct", err)
			return
		}

		if len(uploadVOD.OutputLocations) == 0 {
			errors.WriteHTTPBadRequest(w, "Empty output locations", nil)
			return

		}

		// Do something with uploadVOD
		w.WriteHeader(200)
		io.WriteString(w, "OK")
	})
}
