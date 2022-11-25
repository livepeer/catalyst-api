package pipeline

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/stretchr/testify/require"
)

func TestCoordinatorDoesNotBlock(t *testing.T) {
	require := require.New(t)

	callbacks := make(chan clients.TranscodeStatusMessage, 10)
	callbackHandler := func(msg clients.TranscodeStatusMessage) {
		callbacks <- msg
	}
	barrier := make(chan struct{})
	var running atomic.Bool
	blockHandler := StubHandler{
		handleStartUploadJob: func(job *JobInfo) error {
			running.Store(true)
			defer running.Store(false)
			<-barrier
			return errors.New("test error")
		},
	}
	coord := NewStubCoordinatorOpts(callbackHandler, blockHandler, blockHandler)
	coord.StartUploadJob(UploadJobPayload{RequestID: "123"})
	time.Sleep(1 * time.Second)

	require.True(running.Load())
	require.Equal(1, len(callbacks))
	require.Equal(clients.TranscodeStatusPreparing.String(), (<-callbacks).Status)

	barrier <- struct{}{}
	select {
	case msg := <-callbacks:
		require.Equal(clients.TranscodeStatusError.String(), msg.Status)
	case <-time.After(1 * time.Second):
		require.Fail("should have received a callback")
	}
}
