package steps

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func (s *StepContext) CallAPI(endpoint string, timeoutSecs int64) error {
	client := http.DefaultClient
	client.Timeout = time.Duration(timeoutSecs) * time.Second
	r, err := http.NewRequest(http.MethodGet, s.BaseURL+endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(r)
	if err != nil {
		return err
	}
	s.latestResponse = resp
	return nil
}

func (s *StepContext) CheckHTTPResponseCode(code int) error {
	if s.latestResponse.StatusCode != code {
		return fmt.Errorf("expected HTTP response code %d but got %d", code, s.latestResponse.StatusCode)
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
