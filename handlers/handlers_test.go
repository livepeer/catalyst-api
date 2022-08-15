package handlers

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/require"
)

func TestOKHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{}
	router := httprouter.New()
	req, _ := http.NewRequest("GET", "/ok", nil)
	rr := httptest.NewRecorder()
	router.GET("/ok", catalystApiHandlers.Ok())
	router.ServeHTTP(rr, req)

	require.Equal(rr.Body.String(), "OK")
}

func TestSuccessfulVODUploadHandler(t *testing.T) {
	require := require.New(t)

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer callbackServer.Close()

	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}, StreamCache: make(map[string]StreamInfo)}
	var jsonData = `{
		"url": "http://0.0.0.0/input",
		"callback_url": "CALLBACK_URL",
		"output_locations": [
			{
				"type": "object_store",
				"url": "memory://0.0.0.0/output",
 				"outputs": {
					"source_segments": true
				}
			},
			{
				"type": "pinata",
				"pinata_access_key": "abc",
 				"outputs": {
					"transcoded_segments": true
				}
			}
		]
	}`
	jsonData = strings.ReplaceAll(jsonData, "CALLBACK_URL", callbackServer.URL)

	router := httprouter.New()

	req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer([]byte(jsonData)))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	router.POST("/api/vod", catalystApiHandlers.UploadVOD())
	router.ServeHTTP(rr, req)

	require.Equal(http.StatusOK, rr.Result().StatusCode)
	require.Equal("2", rr.Body.String())
}

func TestInvalidPayloadVODUploadHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}}
	badRequests := [][]byte{
		// missing url
		[]byte(`{
			"callback_url": "http://0.0.0.0/callback",
			"output_locations": [ { "type": "object_store", "url": "memory://0.0.0.0/output" } ]
		}`),
		// missing callback_url
		[]byte(`{
			"url": "http://0.0.0.0/input",
			"output_locations": [ { "type": "object_store", "url": "memory://0.0.0.0/output" } ]
		}`),
		// missing output_locations
		[]byte(`{
			"url": "http://0.0.0.0/input",
			"callback_url": "http://0.0.0.0/callback"
		}`),
		// invalid url
		[]byte(`{
			"url": "x://}]:&7@0.0.0.0/",
			"callback_url": "http://0.0.0.0/callback",
			"output_locations": [ { "type": "object_store", "url": "memory://0.0.0.0/output" } ]
		}`),
		// invalid callback_url
		[]byte(`{
			"url": "http://0.0.0.0/input",
			"callback_url": "x://}]:&7@0.0.0.0/",
			"output_locations": [ { "type": "object_store", "url": "memory://0.0.0.0/output" } ]
		}`),
		// invalid output_location's object_store url
		[]byte(`{
			"url": "http://0.0.0.0/input",
			"callback_url": "http://0.0.0.0/callback",
			"output_locations": [ { "type": "object_store", "url": "x://}]:&7@0.0.0.0/" } ]
		}`),
		// invalid output_location type
		[]byte(`{
			"url": "http://0.0.0.0/input",
			"callback_url": "http://0.0.0.0/callback",
			"output_locations": [ { "type": "foo", "url": "http://0.0.0.0/" } ]
		}`),
		// invalid output_location's pinata params
		[]byte(`{
			"url": "http://0.0.0.0/input",
			"callback_url": "http://0.0.0.0/callback",
			"output_locations": [ { "type": "pinata", "pinata_access_key": "" } ]
		}`),
	}

	router := httprouter.New()

	router.POST("/api/vod", catalystApiHandlers.UploadVOD())
	for _, payload := range badRequests {
		req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		require.Equal(rr.Result().StatusCode, 400)
	}
}

func TestWrongContentTypeVODUploadHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}}
	var jsonData = []byte(`{
		"url": "http://0.0.0.0/input",
		"callback_url": "http://0.0.0.0/callback",
		"output_locations": [
			{
				"type": "object_store",
				"url": "http://0.0.0.0/"
			}
		]
	}`)

	router := httprouter.New()
	req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "json")

	rr := httptest.NewRecorder()
	router.POST("/api/vod", catalystApiHandlers.UploadVOD())
	router.ServeHTTP(rr, req)

	require.Equal(rr.Result().StatusCode, 415)
	require.JSONEq(rr.Body.String(), `{"error": "Requires application/json content type"}`)
}
