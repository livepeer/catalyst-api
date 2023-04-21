package steps

import (
	"net/http"

	"github.com/minio/madmin-go"
)

type StepContext struct {
	latestResponse              *http.Response
	latestRequestID             string
	latestManifestID            string
	pendingRequest              *http.Request
	pendingRequestPayload       string
	authHeaders                 string
	timeoutSecs                 int64
	BaseURL                     string
	BaseInternalURL             string
	SourceOutputDir             string
	TranscodedOutputDir         string
	Studio                      http.Server
	Broadcaster                 http.Server
	BroadcasterSegmentsReceived map[string]int // Map of ManifestID -> Num Segments
	MinioAdmin                  *madmin.AdminClient
	GateAPIStatus               int
	GateAPICallCount            int
	GateAPICallType             interface{}
}
