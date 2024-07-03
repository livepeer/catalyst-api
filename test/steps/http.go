package steps

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cucumber/godog"
	"github.com/livepeer/catalyst-api/video"
)

var App *exec.Cmd

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
	return s.postRequest(s.BaseURL, endpoint, payload, map[string]string{})
}

func (s *StepContext) CreatePostRequestInternal(endpoint, payload string) error {
	return s.postRequest(s.BaseInternalURL, endpoint, payload, map[string]string{})
}

func (s *StepContext) CreateTriggerRequest(trigger, payloadFile, id string) error {
	triggerFile := filepath.Join("fixtures", "trigger-payloads", payloadFile)
	payloadBytes, err := os.ReadFile(triggerFile)
	if err != nil {
		return fmt.Errorf("failed to read trigger payload file %q: %s", triggerFile, err)
	}
	return s.postRequest(
		s.BaseInternalURL,
		"/api/mist/trigger",
		string(payloadBytes),
		map[string]string{
			"X-TRIGGER":      trigger,
			"X-Trigger-UUID": id,
		},
	)
}

func (s *StepContext) postRequest(baseURL, endpoint, payload string, headers map[string]string) error {
	// Copy our source MP4 to somewhere we can ingest it from
	sourceFile, err := os.CreateTemp(os.TempDir(), "source*.mp4")
	if err != nil {
		return fmt.Errorf("failed to create a source file: %s", err)
	}
	var sourceFixture = "fixtures/tiny.mp4"
	if strings.Contains(payload, "audio-only") {
		sourceFixture = "fixtures/audio.mp4"
	}
	sourceBytes, err := os.ReadFile(sourceFixture)
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

	var (
		req     = DefaultUploadRequest(destinationDir)
		reqBody string
	)
	if strings.HasPrefix(payload, "a valid upload vod request") {
		req.PipelineStrategy = "fallback_external"
		req.URL = "file://" + sourceFile.Name()
		if strings.Contains(payload, "with profiles") {
			req.Profiles = []video.EncodedProfile{
				{
					Name:    "hd",
					Width:   1024,
					Height:  768,
					Bitrate: 100_000,
					FPS:     60,
					Profile: "hd-profile",
				},
			}
		}
		if reqBody, err = req.ToJSON(); err != nil {
			return fmt.Errorf("failed to build upload request JSON: %s", err)
		}
	}
	if strings.HasPrefix(payload, "a valid ffmpeg upload vod request with a custom segment size") {
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
		if strings.Contains(payload, "and fmp4") {
			req.OutputLocations[0].Outputs.FMP4 = "enabled"
		}
		if strings.Contains(payload, "and thumbnails") {
			req.OutputLocations[0].Outputs.Thumbnails = "enabled"
		}
		if reqBody, err = req.ToJSON(); err != nil {
			return fmt.Errorf("failed to build upload request JSON: %s", err)
		}
	}
	if strings.HasPrefix(payload, "a valid ffmpeg upload vod request with a source manifest") {
		req.URL = "file://" + filepath.Join(sourceManifestDir, "tiny.m3u8")
		if strings.Contains(payload, "from object store") {
			req.URL = "http://" + minioAddress + "/rec-bucket/tiny.m3u8"
		}

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
		if strings.Contains(payload, "and thumbnails") {
			req.OutputLocations[0].Outputs.Thumbnails = "enabled"
		}
		if reqBody, err = req.ToJSON(); err != nil {
			return fmt.Errorf("failed to build upload request JSON: %s", err)
		}
	}
	if strings.HasSuffix(payload, "with no write permission") {
		req.OutputLocations[0].URL = "s3+https://u:p@gateway.storjshare.io/foo/bar"
		if reqBody, err = req.ToJSON(); err != nil {
			return fmt.Errorf("failed to build upload request JSON: %s", err)
		}
	}
	if payload == "an invalid upload vod request" {
		reqBody = "{}"
	}

	if reqBody == "" {
		reqBody = payload
	}
	r, err := http.NewRequest(http.MethodPost, baseURL+endpoint, strings.NewReader(reqBody))
	r.Header.Set("Authorization", s.authHeaders)
	r.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	if err != nil {
		return err
	}

	s.uploadRequest = req
	s.pendingRequest = r

	return nil
}

func (s *StepContext) StartApp() error {
	s.SourceOutputDir = fmt.Sprintf("file://%s/%s/", os.TempDir(), "livepeer/source")

	App = exec.Command(
		"./app",
		"-http-addr=127.0.0.1:18989",
		"-http-internal-addr=127.0.0.1:17979",
		"-cluster-addr=127.0.0.1:19935",
		"-broadcaster-url=http://127.0.0.1:18935",
		`-metrics-db-connection-string=`+DB_CONNECTION_STRING,
		"-private-bucket=fixtures/playback-bucket",
		"-gate-url=http://localhost:13000/api/access-control/gate",
		"-external-transcoder=mediaconverthttp://examplekey:examplepass@127.0.0.1:11111?region=us-east-1&role=arn:aws:iam::exampleaccountid:examplerole&s3_aux_bucket=s3://example-bucket",
		"-storage-fallback-urls=http://127.0.0.1:9000/rec-bucket=http://127.0.0.1:9000/rec-fallback-bucket",
		"-source-output",
		s.SourceOutputDir,
		"-no-mist",
	)
	outfile, err := os.Create("logs/app.log")
	if err != nil {
		return err
	}
	defer outfile.Close()
	App.Stdout = outfile
	App.Stderr = outfile
	if err := App.Start(); err != nil {
		return err
	}

	// Wait for app to start
	WaitForStartup(s.BaseURL + "/ok")

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

func (s *StepContext) CheckMetricEqual(metricName, value string) error {
	var url = s.BaseInternalURL + "/metrics"

	res, err := http.Get(url)
	if err != nil {
		return err
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	r := regexp.MustCompile(`\n` + metricName + ` ` + value + `\n`)
	if !r.Match(body) {
		// Try to build a more useful error message than "not found" in the case where the value is wrong
		r2 := regexp.MustCompile(`\n` + metricName + ` \d\n`)
		found := r2.Find(body)

		return fmt.Errorf("could not find metric %s equal to %s. Got: %s", metricName, value, strings.TrimSpace(string(found)))
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
