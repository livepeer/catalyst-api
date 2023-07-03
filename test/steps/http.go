package steps

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

type VODUploadResponse struct {
	RequestID string `json:"request_id"`
}

func (s *StepContext) CreateRequest(endpoint, _, method string) error {
	return s.request(s.BaseURL, endpoint, method)
}

func (s *StepContext) CreateGetRequestInternal(endpoint string) error {
	return s.request(s.BaseInternalURL, endpoint, "")
}

func (s *StepContext) request(baseURL, endpoint, method string) error {
	if method == "" {
		method = http.MethodGet
	}
	r, err := http.NewRequest(method, baseURL+endpoint, nil)
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
	// Copy our source MP4 to somewhere we can ingest it from
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

	// Copy our source manifest and segments to somewhere we can ingest them from
	sourceManifestDir, err := os.MkdirTemp(os.TempDir(), "sourcemanifest-*")
	if err != nil {
		return fmt.Errorf("failed to create a source manifest directory: %s", err)
	}
	for _, filename := range []string{"tiny.m3u8", "seg-0.ts", "seg-1.ts", "seg-2.ts"} {
		sourceBytes, err := os.ReadFile(filepath.Join("fixtures", filename))
		if err != nil {
			return fmt.Errorf("failed to read example source file %q: %s", filename, err)
		}
		sourceFile, err := os.Create(filepath.Join(sourceManifestDir, filename))
		if err != nil {
			return fmt.Errorf("failed to create a new source file: %s", err)
		}
		if _, err = sourceFile.Write(sourceBytes); err != nil {
			return fmt.Errorf("failed to write to source file %q: %s", filename, err)
		}
	}

	destinationDir, err := os.MkdirTemp(os.TempDir(), "transcoded*")
	if err != nil {
		return fmt.Errorf("failed to create a destination directory: %s", err)
	}
	s.TranscodedOutputDir = destinationDir

	if payload == "a valid upload vod request" {
		req := DefaultUploadRequest
		req.URL = "file://" + sourceFile.Name()
		if payload, err = req.ToJSON(); err != nil {
			return fmt.Errorf("failed to build upload request JSON: %s", err)
		}
	}
	if strings.HasPrefix(payload, "a valid ffmpeg upload vod request with a custom segment size") {
		req := DefaultUploadRequest
		req.URL = "file://" + sourceFile.Name()
		req.PipelineStrategy = "catalyst_ffmpeg"
		req.TargetSegmentSizeSecs = 9
		req.OutputLocations = []OutputLocation{
			{
				Type: "object_store",
				URL:  "file://" + destinationDir,
				Outputs: Output{
					HLS:       "enabled",
					SourceMp4: strings.Contains(payload, "and source copying"),
				},
			},
		}
		if payload, err = req.ToJSON(); err != nil {
			return fmt.Errorf("failed to build upload request JSON: %s", err)
		}
	}
	if strings.HasPrefix(payload, "a valid ffmpeg upload vod request with a source manifest") {
		req := DefaultUploadRequest
		req.URL = "file://" + filepath.Join(sourceManifestDir, "tiny.m3u8")
		req.PipelineStrategy = "catalyst_ffmpeg"
		req.OutputLocations = []OutputLocation{
			{
				Type: "object_store",
				URL:  "file://" + destinationDir,
				Outputs: Output{
					HLS:       "enabled",
					SourceMp4: strings.Contains(payload, "and source copying"),
				},
			},
		}
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

func (s *StepContext) SaveRequestID() error {
	body, err := io.ReadAll(s.latestResponse.Body)
	if err != nil {
		return err
	}

	var resp VODUploadResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return err
	}
	if resp.RequestID == "" {
		return fmt.Errorf("did not receive a Request ID in the HTTP response")
	}

	s.latestRequestID = resp.RequestID

	return nil
}

func (s *StepContext) Wait() error {
	time.Sleep(time.Second * 5)
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

func (s *StepContext) CheckRecordedMetrics(metricsType, requestType string) error {
	var url = s.BaseInternalURL + "/metrics"

	res, err := http.Get(url)
	if err != nil {
		return err
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	metric := "upload_vod_request_duration_seconds_count"
	if requestType == "playback" {
		metric = "catalyst_playback_request_duration_seconds_count"
	}

	if metricsType == "failed" {
		r := regexp.MustCompile(fmt.Sprintf(`\n%s{status_code="\d+",success="false",version="cucumber-test-version"} .+\n`, metric))
		if !r.Match(body) {
			return fmt.Errorf("not a valid failure %s: %q", metric, body)
		}
	}

	if metricsType == "successful" {
		r := regexp.MustCompile(fmt.Sprintf(`\n%s{status_code="200",success="true",version="cucumber-test-version"} .+\n`, metric))
		if !r.Match(body) {
			return fmt.Errorf("not a valid success %s: %q", metric, body)
		}
	}

	if requestType == "vod" {
		r := regexp.MustCompile(`\nupload_vod_request_count 1\n`)
		if !r.Match(body) {
			return fmt.Errorf("not a valid upload_vod_request_count metric")
		}
	}

	return nil
}

func (s *StepContext) CheckHTTPHeaders(expectedHeaders *godog.Table) error {
	for i, row := range expectedHeaders.Rows {
		if i < 1 {
			continue // skip header row
		}
		if len(row.Cells) < 2 {
			continue
		}
		expectedKey := row.Cells[0]
		expectedValue := row.Cells[1]
		actualValue := s.latestResponse.Header.Get(expectedKey.Value)
		if expectedValue.Value != actualValue {
			return fmt.Errorf("expected to get header %s with value %s. got: %s", expectedKey.Value, expectedValue.Value, actualValue)
		}
	}
	return nil
}

func (s *StepContext) CheckGateAPICallValid() error {
	if s.GateAPICallType == "" {
		return errors.New("type field should not be empty on gate API request")
	}
	return nil
}
