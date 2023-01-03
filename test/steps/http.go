package steps

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

func (s *StepContext) CreateGetRequest(endpoint string) error {
	r, err := http.NewRequest(http.MethodGet, s.BaseURL+endpoint, nil)
	if err != nil {
		return err
	}

	s.pendingRequest = r

	return nil
}

func (s *StepContext) CreatePostRequest(endpoint, payload string) error {
	if payload == "a valid upload vod request" {
		payload = s.getDefaultUploadRequestPayload()
	}

	if payload == "an invalid upload vod request" {
		payload = "{}"
	}

	r, err := http.NewRequest(http.MethodPost, s.BaseURL+endpoint, strings.NewReader(payload))
	r.Header.Set("Authorization", s.authHeaders)
	r.Header.Set("Content-Type", "application/json")
	if err != nil {
		return err
	}

	s.pendingRequest = r

	return nil
}

func (s *StepContext) SetAuthHeaders() {
	s.authHeaders = "Bearer IAmAuthorized"
}

func (s *StepContext) getDefaultUploadRequestPayload() string {
	return `{
		"url": "http://localhost/input",
		"callback_url": "http://localhost:3000/cb",
		"output_locations": [
			{
				"type": "object_store",
				"url": "memory://localhost/output.m3u8",
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
}

func (s *StepContext) SetTimeout(timeoutSecs int64) {
	s.timeoutSecs = timeoutSecs
}

func (s *StepContext) CallAPI() error {
	client := http.DefaultClient
	client.Timeout = time.Duration(s.timeoutSecs) * time.Second

	resp, err := client.Do(s.pendingRequest)
	if err != nil {
		return err
	}

	s.latestResponse = resp
	s.pendingRequest = nil

	return nil
}

func (s *StepContext) CheckHTTPResponseCodeAndBody(code int, expectedBody string) error {
	err := s.CheckHTTPResponseCode(code)
	if err != nil {
		return err
	}

	err = s.CheckHTTPResponseBody(expectedBody)
	if err != nil {
		return err
	}

	return nil
}

func (s *StepContext) CheckHTTPResponseCode(code int) error {
	if s.latestResponse.StatusCode != code {
		body, _ := io.ReadAll(s.latestResponse.Body)
		return fmt.Errorf("expected HTTP response code %d but got %d. Body: %s", code, s.latestResponse.StatusCode, body)
	}
	return nil
}

func (s *StepContext) CheckHTTPResponseBody(expectedBody string) error {
	b, err := io.ReadAll(s.latestResponse.Body)
	if err != nil {
		return err
	}

	actualBody := strings.TrimSpace(string(b))
	if actualBody != expectedBody {
		return fmt.Errorf("expected a response body of %q but got %q", expectedBody, actualBody)
	}

	return nil
}

func (s *StepContext) SetRequestPayload(payload string) {
	s.pendingRequestPayload = payload
}

func (s *StepContext) CheckRecordedMetrics(metricsType string) error {
	var url = "http://localhost:2112/metrics"

	res, err := http.Get(url)
	if err != nil {
		return err
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if metricsType == "failure" {
		r := regexp.MustCompile(`\nupload_vod_request_duration_seconds_count{status_code="500",success="false",version="cucumber-test-version"} 1\n`)
		if !r.Match(body) {
			return fmt.Errorf("not a valid failure upload_vod_request_duration_seconds_count")
		}
	}

	if metricsType == "successful" {
		r := regexp.MustCompile(`\nupload_vod_request_duration_seconds_count{status_code="200",success="true",version="cucumber-test-version"} 1\n`)
		if !r.Match(body) {
			return fmt.Errorf("not a valid success upload_vod_request_duration_seconds: %q", body)
		}
	}

	r := regexp.MustCompile(`\nupload_vod_request_count 1\n`)
	if !r.Match(body) {
		return fmt.Errorf("not a valid upload_vod_request_count metric")
	}

	return nil
}
