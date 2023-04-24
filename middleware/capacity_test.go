package middleware

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/stretchr/testify/require"
)

func TestItCallsNextMiddlewareWhenCapacityAvailable(t *testing.T) {
	// Create a next handler in the middleware chain, to confirm the request was passed onwards
	var nextCalled bool
	next := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		nextCalled = true
	}

	// Set up the HTTP handler
	handler := HasCapacity(pipeline.NewStubCoordinator(), next)

	// Call the handler
	responseRecorder := httptest.NewRecorder()
	handler(responseRecorder, nil, nil)

	// Confirm we got a success reponse and that the handler called the next middleware
	require.Equal(t, http.StatusOK, responseRecorder.Code)
	require.True(t, nextCalled)
}

func TestItErrorsWhenNoCapacityAvailable(t *testing.T) {
	// Create a next handler in the middleware chain, to confirm the request was passed onwards
	var nextCalled bool
	next := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		nextCalled = true
	}

	pipeFfmpeg, release := pipeline.NewBlockingStubHandler()
	defer release()
	coordinator := pipeline.NewStubCoordinatorOpts(pipeline.StrategyCatalystFfmpegDominance, nil, pipeFfmpeg, nil, "")
	coordinator.InputCopy = &clients.StubInputCopy{}

	// Create a lot of in-flight jobs
	for x := 0; x < 8; x++ {
		coordinator.StartUploadJob(pipeline.UploadJobPayload{
			RequestID: fmt.Sprintf("request-%d", x),
		})
	}
	time.Sleep(1 * time.Second)

	// Set up the HTTP handler
	handler := HasCapacity(coordinator, next)

	// Call the handler
	responseRecorder := httptest.NewRecorder()
	handler(responseRecorder, nil, nil)

	// Confirm we got an HTTP 429 response
	require.Equal(t, http.StatusTooManyRequests, responseRecorder.Code)

	// Confirm the handler didn't call the next middleware
	require.False(t, nextCalled)
}
