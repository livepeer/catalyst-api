package handlers

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

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
			replyError(405, w)
			return
		}

		if req.Header.Get("Content-Type") != "application/json" {
			replyError(400, w)
			return
		}

		var uploadVOD UploadVODRequest
		var payload []byte
		var err error

		if payload, err = ioutil.ReadAll(req.Body); err != nil {
			replyError(500, w)
			return
		}

		if err = json.Unmarshal(payload, &uploadVOD); err != nil {
			replyError(400, w)
			return
		}

		if len(uploadVOD.OutputLocations) == 0 {
			replyError(400, w)
			return

		}

		// Do something with uploadVOD
		log.Println(len(uploadVOD.OutputLocations))
		replyOK("OK", w)
	})
}

func replyError(code int, w http.ResponseWriter) {
	var response string
	switch code {
	case 400:
		response = "Bad Request"
	case 405:
		response = "Method Not Allowed"
	default:
		code = 500
		response = "Internal Server Error"
	}

	w.WriteHeader(code)
	io.WriteString(w, response)
}

func replyOK(payload string, w http.ResponseWriter) {
	w.WriteHeader(200)
	io.WriteString(w, payload)
}
