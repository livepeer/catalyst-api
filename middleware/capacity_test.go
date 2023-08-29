package middleware

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestItCallsNextMiddlewareWhenCapacityAvailable(t *testing.T) {
	// Create a next handler in the middleware chain, to confirm the request was passed onwards
	var nextCalled bool
	next := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		nextCalled = true
	}

	// Set up the HTTP handler
	cm := CapacityMiddleware{}
	handler := cm.HasCapacity(pipeline.NewStubCoordinator(), next)

	// Call the handler
	responseRecorder := httptest.NewRecorder()
	handler(responseRecorder, nil, nil)

	// Confirm we got a success reponse and that the handler called the next middleware
	require.Equal(t, http.StatusOK, responseRecorder.Code)
	require.True(t, nextCalled)
}

func TestItErrorsWhenNoJobCapacityAvailable(t *testing.T) {
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
	cm := CapacityMiddleware{}
	handler := cm.HasCapacity(coordinator, next)

	// Call the handler
	responseRecorder := httptest.NewRecorder()
	handler(responseRecorder, nil, nil)

	// Confirm we got an HTTP 429 response
	require.Equal(t, http.StatusTooManyRequests, responseRecorder.Code)

	// Confirm the handler didn't call the next middleware
	require.False(t, nextCalled)
}

// As well as looking at jobs in progress, we should also take into account
// in-flight HTTP requests to avoid the race condition where we get a lot of
// requests at once and let them all through
func TestItTakesIntoAccountInFlightHTTPRequests(t *testing.T) {
	// Create a next handler in the middleware chain, to confirm the request was passed onwards
	next := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		time.Sleep(2 * time.Second) // Sleep to simulate a request that doesn't immediately finish
	}

	pipeFfmpeg, release := pipeline.NewBlockingStubHandler()
	defer release()
	coordinator := pipeline.NewStubCoordinatorOpts(pipeline.StrategyCatalystFfmpegDominance, nil, pipeFfmpeg, nil, "")
	coordinator.InputCopy = &clients.StubInputCopy{}

	// Set up the HTTP handler
	cm := CapacityMiddleware{}
	handler := cm.HasCapacity(coordinator, next)

	// Call the handler
	timeout, _ := context.WithTimeout(context.Background(), 5*time.Second)
	g, _ := errgroup.WithContext(timeout)
	var responseCodes []int = make([]int, 100)
	for i := 0; i < 100; i++ {
		i := i
		g.Go(
			func() error {
				responseRecorder := httptest.NewRecorder()
				handler(responseRecorder, nil, nil)
				responseCodes[i] = responseRecorder.Code
				return nil
			},
		)
	}
	require.NoError(t, g.Wait())

	var rejectedRequestCount = 0
	for _, responseCode := range responseCodes {
		if responseCode == http.StatusTooManyRequests {
			rejectedRequestCount++
		}
	}

	// Confirm the handler didn't let too many requests through
	require.Equal(t, rejectedRequestCount, 100-config.MAX_JOBS_IN_FLIGHT+1)
}
