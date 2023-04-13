package steps

import (
	"net/http"
	"sync"

	"github.com/minio/madmin-go"
)

type StepContext struct {
	latestResponse        *http.Response
	pendingRequest        *http.Request
	pendingRequestPayload string
	mistPushStartURLs     []string
	authHeaders           string
	timeoutSecs           int64
	BaseURL               string
	BaseInternalURL       string
	Mist                  http.Server
	Studio                http.Server
	MinioAdmin            *madmin.AdminClient
	GateAPIStatus         int
	GateAPICallCount      int
	GateAPICallType       interface{}
}

var mistPushStartURLMutex sync.Mutex

func (s *StepContext) AddMistPushStartURL(u string) {
	mistPushStartURLMutex.Lock()
	defer mistPushStartURLMutex.Unlock()
	s.mistPushStartURLs = append(s.mistPushStartURLs, u)
}

func (s *StepContext) GetMistPushStartURLs() []string {
	mistPushStartURLMutex.Lock()
	defer mistPushStartURLMutex.Unlock()
	return s.mistPushStartURLs
}
