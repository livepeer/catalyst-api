package handlers

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/livepeer/go-livepeer/drivers"
	"github.com/stretchr/testify/require"
)

func TestOKHandler(t *testing.T) {
	require := require.New(t)

	req, _ := http.NewRequest("GET", "/ok", nil)
	rr := httptest.NewRecorder()
	h := DMSAPIHandlers.Ok()
	h.ServeHTTP(rr, req)

	require.Equal(rr.Body.String(), "OK")
}

func TestSuccessfulVODUploadHandler(t *testing.T) {
	require := require.New(t)
	drivers.Testing = true
	defer (func() { drivers.Testing = false })()

	var jsonData = []byte(`{
		"url": "http://localhost/input",
		"callback_url": "http://localhost/callback",
		"output_locations": [
			"memory://localhost/output"
		]
	}`)

	req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h := DMSAPIHandlers.UploadVOD()
	h.ServeHTTP(rr, req)

	require.Equal(rr.Result().StatusCode, 200)
	require.Equal(rr.Body.String(), "1")
}

func TestInvalidPayloadVODUploadHandler(t *testing.T) {
	require := require.New(t)
	drivers.Testing = true
	defer (func() { drivers.Testing = false })()

	badRequests := map[string][]byte{
		"Missing url": []byte(`{
			"callback_url": "http://localhost/callback",
			"output_locations": [
				"memory://localhost/output"
			]
		}`),
		"Missing callback_url": []byte(`{
			"url": "http://localhost/input",
			"output_locations": [
				"memory://localhost/output"
			]
		}`),
		"Missing output_locations": []byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback"
		}`),
		"Invalid url": []byte(`{
			"url": "x://}]:&7@localhost/",
			"callback_url": "http://localhost/callback",
			"output_locations": [
				"memory://localhost/output"
			]
		}`),
		"Invalid callback_url": []byte(`{
			"url": "http://localhost/input",
			"callback_url": "x://}]:&7@localhost/",
			"output_locations": [
				"memory://localhost/output"
			]
		}`),
		"Empty output_locations": []byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback",
			"output_locations": []
		}`),
		"Invalid output_locations entry": []byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback",
			"output_locations": [
				"foo://localhost/output"
			]
		}`),
	}

	h := DMSAPIHandlers.UploadVOD()
	for err, payload := range badRequests {
		req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(rr.Result().StatusCode, 400)
		require.JSONEq(rr.Body.String(), fmt.Sprintf(`{"error":"%s"}`, err))
	}
}

func TestBadHTTPMethodVODUploadHandler(t *testing.T) {
	require := require.New(t)
	drivers.Testing = true
	defer (func() { drivers.Testing = false })()

	req, _ := http.NewRequest("GET", "/api/vod", nil)
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h := DMSAPIHandlers.UploadVOD()
	h.ServeHTTP(rr, req)

	require.Equal(rr.Result().StatusCode, 405)
	require.JSONEq(rr.Body.String(), `{"error": "Method not allowed"}`)
}

func TestWrongContentTypeVODUploadHandler(t *testing.T) {
	require := require.New(t)
	drivers.Testing = true
	defer (func() { drivers.Testing = false })()

	var jsonData = []byte(`{
		"url": "http://localhost/input",
		"callback_url": "http://localhost/callback",
		"output_locations": [
			"memory://localhost/output"
		]
	}`)

	req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "json")

	rr := httptest.NewRecorder()
	h := DMSAPIHandlers.UploadVOD()
	h.ServeHTTP(rr, req)

	require.Equal(rr.Result().StatusCode, 400)
	require.JSONEq(rr.Body.String(), `{"error": "Unsupported content type"}`)
}
