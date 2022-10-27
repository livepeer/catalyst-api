package steps

import (
	"net/http"

	"github.com/minio/madmin-go"
)

type StepContext struct {
	latestResponse        *http.Response
	pendingRequest        *http.Request
	pendingRequestPayload string
	authHeaders           string
	timeoutSecs           int64
	BaseURL               string
	Mist                  http.Server
	Studio                http.Server
	MinioAdmin            *madmin.AdminClient
}
