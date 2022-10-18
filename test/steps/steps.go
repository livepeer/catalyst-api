package steps

import "net/http"

type StepContext struct {
	latestResponse *http.Response
	BaseURL        string
}
