package requests

import (
	"net/http"

	"github.com/livepeer/catalyst-api/config"
)

const requestIDParam = "requestID"

func GetRequestId(req *http.Request) string {
	requestID := req.Header.Get(requestIDParam)
	if requestID != "" {
		return requestID
	}
	requestID = config.RandomTrailer(8)
	req.Header.Set(requestIDParam, requestID)
	return requestID
}
