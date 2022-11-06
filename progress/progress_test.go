package progress

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/stretchr/testify/require"
)

func TestProgressNotificationThrottling(t *testing.T) {
	var updateCount = 0
	mock, accumulator, cleanup := setup(func() { updateCount++ }, t)
	defer cleanup()

	accumulator.Accumulate(1)
	forward(mock, 1*time.Second)

	accumulator.Accumulate(1)
	forward(mock, 1*time.Second)

	require.Equal(t, 1, updateCount)
}

func TestProgressNotificationInterval(t *testing.T) {
	var updateCount = 0
	mock, accumulator, cleanup := setup(func() { updateCount++ }, t)
	defer cleanup()

	accumulator.Accumulate(1)
	forward(mock, 1*time.Second)

	accumulator.Accumulate(1)
	forward(mock, 10*time.Second)

	require.Equal(t, 2, updateCount)
}

func TestProgressBucketChange(t *testing.T) {
	var updateCount = 0
	mock, accumulator, cleanup := setup(func() { updateCount++ }, t)
	defer cleanup()

	accumulator.Accumulate(1)
	forward(mock, 1*time.Second)

	accumulator.Accumulate(25)
	forward(mock, 1*time.Second)

	require.Equal(t, 2, updateCount)
}

func TestFastProgressBucketChange(t *testing.T) {
	var updateCount = 0
	mock, accumulator, cleanup := setup(func() { updateCount++ }, t)
	defer cleanup()

	accumulator.Accumulate(1)
	forward(mock, 1*time.Second)

	accumulator.Accumulate(25)
	forward(mock, 500*time.Millisecond)

	require.Equal(t, 1, updateCount)
}

func setup(callback func(), t require.TestingT) (*clock.Mock, *Accumulator, func()) {
	var realClock = Clock
	var mock = clock.NewMock()
	Clock = mock

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		callback()
		w.WriteHeader(http.StatusOK)
	}))

	client := clients.NewCallbackClient()

	accumulator := NewAccumulator()
	reporter := NewProgressReporter(context.Background(), &client, server.URL, "taskid")
	reporter.TrackCount(accumulator.Size, 100, 1)

	return mock, accumulator, func() {
		Clock = realClock
		server.Close()
	}
}

func forward(mock *clock.Mock, duration time.Duration) {
	// give a chance to other goroutines to execute
	time.Sleep(1 * time.Millisecond)
	mock.Add(duration)
}
