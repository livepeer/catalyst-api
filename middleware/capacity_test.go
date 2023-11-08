package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func setupRequest(clip bool) handlers.UploadVODRequest {
	if clip {
		clipStrategy := video.ClipStrategy{
			Enabled:    true,
			StartTime:  0,
			EndTime:    10,
			PlaybackID: "test-playback-id",
		}
		return handlers.UploadVODRequest{
			ClipStrategy: clipStrategy,
		}
	}
	return handlers.UploadVODRequest{}
}

func TestItCallsNextMiddlewareWhenCapacityAvailable(t *testing.T) {

	// Setup a regular-vod request
	vodReqBodyBytes, err := json.Marshal(setupRequest(false))
	require.NoError(t, err)
	vodReq, err := http.NewRequest("POST", "/one", bytes.NewBuffer(vodReqBodyBytes))
	require.NoError(t, err)
	vodReq.Header.Set("Content-Type", "application/json")

	// Setup a clip-vod request
	clipVodReqBodyBytes, err := json.Marshal(setupRequest(true))
	require.NoError(t, err)
	clipVodReq, err := http.NewRequest("POST", "/two", bytes.NewBuffer(clipVodReqBodyBytes))
	require.NoError(t, err)
	clipVodReq.Header.Set("Content-Type", "application/json")

	// Create a next handler in the middleware chain, to confirm the request was passed onwards
	var nextCalled bool
	next := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		nextCalled = true
	}

	// Set up the HTTP handler
	cm := CapacityMiddleware{}
	handler := cm.HasCapacity(pipeline.NewStubCoordinator(), next)
	responseRecorder := httptest.NewRecorder()

	// Call the handler for a regular vod request
	handler(responseRecorder, vodReq, nil)
	// Confirm we got a success reponse and that the handler called the next middleware
	require.Equal(t, http.StatusOK, responseRecorder.Code)
	require.True(t, nextCalled)

	// Call the handler for a clip vod request
	handler(responseRecorder, clipVodReq, nil)
	// Confirm we got a success reponse and that the handler called the next middleware
	require.Equal(t, http.StatusOK, responseRecorder.Code)
	require.True(t, nextCalled)
}

func TestItErrorsWhenNoRegularVodJobCapacityAvailable(t *testing.T) {

	// Setup a regular-vod request
	vodReqBodyBytes, err := json.Marshal(setupRequest(false))
	require.NoError(t, err)
	vodReq, err := http.NewRequest("POST", "/one", bytes.NewBuffer(vodReqBodyBytes))
	require.NoError(t, err)
	vodReq.Header.Set("Content-Type", "application/json")

	// Create a next handler in the middleware chain, to confirm the request was passed onwards
	var nextCalled bool
	next := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		nextCalled = true
	}

	pipeFfmpeg, release := pipeline.NewBlockingStubHandler()
	defer release()
	coordinator := pipeline.NewStubCoordinatorOpts(pipeline.StrategyCatalystFfmpegDominance, nil, pipeFfmpeg, nil)
	coordinator.InputCopy = &clients.StubInputCopy{}

	// Set up the HTTP handler
	cm := CapacityMiddleware{}
	handler := cm.HasCapacity(coordinator, next)
	responseRecorder := httptest.NewRecorder()

	// Create a lot of in-flight regular-vod jobs
	for x := 0; x < config.MaxInFlightJobs; x++ {
		coordinator.StartUploadJob(pipeline.UploadJobPayload{
			RequestID: fmt.Sprintf("request-%d", x),
		})
	}
	time.Sleep(1 * time.Second)
	// Call the handler
	handler(responseRecorder, vodReq, nil)
	// Confirm we got an HTTP 429 response
	require.Equal(t, http.StatusTooManyRequests, responseRecorder.Code)
	// Confirm the handler didn't call the next middleware
	require.False(t, nextCalled)

}

func TestItErrorsWhenNoClipVodJobCapacityAvailable(t *testing.T) {

	// Setup a clip-vod request
	clipVodReqBodyBytes, err := json.Marshal(setupRequest(true))
	require.NoError(t, err)
	clipVodReq, err := http.NewRequest("POST", "/two", bytes.NewBuffer(clipVodReqBodyBytes))
	require.NoError(t, err)
	clipVodReq.Header.Set("Content-Type", "application/json")

	// Create a next handler in the middleware chain, to confirm the request was passed onwards
	var nextCalled bool
	next := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		nextCalled = true
	}

	pipeFfmpeg, release := pipeline.NewBlockingStubHandler()
	defer release()
	coordinator := pipeline.NewStubCoordinatorOpts(pipeline.StrategyCatalystFfmpegDominance, nil, pipeFfmpeg, nil)
	coordinator.InputCopy = &clients.StubInputCopy{}

	// Set up the HTTP handler
	cm := CapacityMiddleware{}
	handler := cm.HasCapacity(coordinator, next)
	responseRecorder := httptest.NewRecorder()

	// Create a lot of in-flight clip jobs
	for x := 0; x < config.MaxInFlightClipJobs; x++ {
		coordinator.StartUploadJob(pipeline.UploadJobPayload{
			RequestID: fmt.Sprintf("request-%d", x),
			ClipStrategy: video.ClipStrategy{
				Enabled:    true,
				StartTime:  0,
				EndTime:    10,
				PlaybackID: "test-playback-id",
			},
		})
	}
	time.Sleep(1 * time.Second)
	// Call the handler
	handler(responseRecorder, clipVodReq, nil)
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
	coordinator := pipeline.NewStubCoordinatorOpts(pipeline.StrategyCatalystFfmpegDominance, nil, pipeFfmpeg, nil)
	coordinator.InputCopy = &clients.StubInputCopy{}

	// Set up the HTTP handler
	cm := CapacityMiddleware{}
	handler := cm.HasCapacity(coordinator, next)

	// Call the handler
	timeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g, _ := errgroup.WithContext(timeout)
	var responseCodes []int = make([]int, 100)
	for i := 0; i < 100; i++ {
		i := i
		g.Go(
			func() error {
				// Setup a regular-vod request
				vodReqBodyBytes, err := json.Marshal(setupRequest(false))
				require.NoError(t, err)
				vodReq, err := http.NewRequest("POST", "/one", bytes.NewBuffer(vodReqBodyBytes))
				require.NoError(t, err)
				vodReq.Header.Set("Content-Type", "application/json")

				responseRecorder := httptest.NewRecorder()
				handler(responseRecorder, vodReq, nil)
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
	require.Equal(t, 100-config.MaxInFlightJobs+1, rejectedRequestCount)
}
