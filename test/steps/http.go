package steps

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
)

func (s *StepContext) CreateGetRequest(endpoint string) error {
	return s.getRequest(s.BaseURL, endpoint)
}

func (s *StepContext) CreateGetRequestInternal(endpoint string) error {
	return s.getRequest(s.BaseInternalURL, endpoint)
}

func (s *StepContext) getRequest(baseURL, endpoint string) error {
	r, err := http.NewRequest(http.MethodGet, baseURL+endpoint, nil)
	if err != nil {
		return err
	}

	s.pendingRequest = r

	return nil
}

func (s *StepContext) CreatePostRequest(endpoint, payload string) error {
	return s.postRequest(s.BaseURL, endpoint, payload)
}

func (s *StepContext) CreatePostRequestInternal(endpoint, payload string) error {
	return s.postRequest(s.BaseInternalURL, endpoint, payload)
}

func (s *StepContext) postRequest(baseURL, endpoint, payload string) error {
	sourceFile, err := os.CreateTemp(os.TempDir(), "source*.mp4")
	if err != nil {
		return fmt.Errorf("failed to create a source file: %s", err)
	}
	sourceBytes, err := os.ReadFile("fixtures/tiny.mp4")
	if err != nil {
		return fmt.Errorf("failed to read example source file: %s", err)
	}
	if _, err = sourceFile.Write(sourceBytes); err != nil {
		return fmt.Errorf("failed to write to source file: %s", err)
	}

	if payload == "a valid upload vod request" {
		req := DefaultUploadRequest
		req.URL = "file://" + sourceFile.Name()
		if payload, err = req.ToJSON(); err != nil {
			return fmt.Errorf("failed to build upload request JSON: %s", err)
		}
	}
	if payload == "a valid upload vod request with a custom segment size" {
		req := DefaultUploadRequest
		req.URL = "file://" + sourceFile.Name()
		req.TargetSegmentSizeSecs = 3
		if payload, err = req.ToJSON(); err != nil {
			return fmt.Errorf("failed to build upload request JSON: %s", err)
		}
	}
	if payload == "an invalid upload vod request" {
		payload = "{}"
	}

	r, err := http.NewRequest(http.MethodPost, baseURL+endpoint, strings.NewReader(payload))
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

func (s *StepContext) CheckMist(segmentSize int) error {
	timeoutSecs := 5
	for counter := 0; counter < timeoutSecs; counter++ {
		urls := s.GetMistPushStartURLs()
		if len(urls) > 1 {
			return fmt.Errorf("received too many Mist segmenting requests (%d)", len(urls))
		}
		if len(urls) == 1 {
			expectedTargetURL := fmt.Sprintf("memory://localhost/source/$currentMediaTime.ts?m3u8=output.m3u8&split=%d", segmentSize)
			actualTargetURL := urls[0]
			if expectedTargetURL != actualTargetURL {
				return fmt.Errorf("incorrect Mist segmenting URL - expected %s but got %s", expectedTargetURL, actualTargetURL)
			}
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("did not receive a Mist segmenting request within %d seconds", timeoutSecs)
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

func (s *StepContext) CheckHTTPResponseBodyFromFile(expectedBodyFilePath string) error {
	file, err := os.Open(path.Join("fixtures", expectedBodyFilePath))
	if err != nil {
		return err
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	return s.CheckHTTPResponseBody(string(bytes))
}

func (s *StepContext) SetRequestPayload(payload string) {
	s.pendingRequestPayload = payload
}

func (s *StepContext) SetGateAPIResponse(allow string) {
	s.GateAPICallCount = 0
	if allow == "allow" {
		s.GateAPIStatus = 200
		return
	}
	s.GateAPIStatus = 401
}

func (s *StepContext) CheckGateAPICallCount(expectedCount int) error {
	if s.GateAPICallCount != expectedCount {
		return fmt.Errorf("expected %d Gate API calls, got %d", expectedCount, s.GateAPICallCount)
	}
	return nil
}

func (s *StepContext) CheckRecordedMetrics(metricsType string) error {
	var url = s.BaseInternalURL + "/metrics"

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
