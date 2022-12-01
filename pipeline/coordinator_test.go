package pipeline

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/stretchr/testify/require"
)

var (
	testHandlerResult = &HandlerOutput{
		Result: &UploadJobResult{clients.InputVideo{}, []clients.OutputVideo{}},
	}
	testJob = UploadJobPayload{
		RequestID:   "123",
		SourceFile:  "source-file",
		CallbackURL: "http://localhost:3000/dummy",
	}
)

func TestCoordinatorDoesNotBlock(t *testing.T) {
	require := require.New(t)

	callbackHandler, callbacks := callbacksRecorder()
	barrier := make(chan struct{})
	var running atomic.Bool
	blockHandler := StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			running.Store(true)
			defer running.Store(false)
			<-barrier
			return nil, errors.New("test error")
		},
	}
	coord := NewStubCoordinatorOpts("", callbackHandler, blockHandler, blockHandler)
	coord.StartUploadJob(testJob)
	time.Sleep(1 * time.Second)

	require.True(running.Load())
	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

	close(barrier)
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusError.String(), msg.Status)
	require.Contains(msg.Error, "test error")

	require.Zero(len(callbacks))
}

func TestCoordinatorPropagatesJobInfoChanges(t *testing.T) {
	require := require.New(t)

	barrier := make(chan struct{})
	done := make(chan struct{}, 1)
	blockHandler := StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			require.Equal("source-file", job.SourceFile)
			<-barrier
			job.SourceFile = "new-source-file"
			return ContinuePipeline, nil
		},
		handleRecordingEndTrigger: func(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
			defer func() { done <- struct{}{} }()
			require.Equal("new-source-file", job.SourceFile)
			return ContinuePipeline, nil
		},
	}
	coord := NewStubCoordinatorOpts("", nil, blockHandler, blockHandler)

	coord.StartUploadJob(testJob)
	time.Sleep(100 * time.Millisecond)

	coord.TriggerRecordingEnd(RecordingEndPayload{StreamName: config.SegmentingStreamName("123")})
	time.Sleep(1 * time.Second)

	// Make sure recording end trigger doesn't execute until the start upload returns
	require.Zero(len(done))

	close(barrier)
	requireReceive(t, done, 1*time.Second)
}

func TestCoordinatorResistsPanics(t *testing.T) {
	require := require.New(t)

	callbackHandler, callbacks := callbacksRecorder()
	blockHandler := StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			panic("oh no!")
		},
	}
	coord := NewStubCoordinatorOpts("", callbackHandler, blockHandler, blockHandler)

	coord.StartUploadJob(testJob)

	require.Equal(1, len(callbacks))
	require.Equal(clients.TranscodeStatusPreparing.String(), (<-callbacks).Status)

	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusError.String(), msg.Status)
	require.Contains(msg.Error, "oh no")
}

func TestCoordinatorCatalystDominance(t *testing.T) {
	require := require.New(t)

	mist, calls := recordingHandler(nil)
	external := allFailingHandler(t)
	coord := NewStubCoordinatorOpts(StrategyCatalystDominance, nil, mist, external)

	coord.StartUploadJob(testJob)

	job := requireReceive(t, calls, 1*time.Second)
	require.Equal("123", job.RequestID)

	time.Sleep(1 * time.Second)
	require.Zero(len(calls))
}

func TestCoordinatorBackgroundJobsStrategies(t *testing.T) {
	require := require.New(t)

	callbackHandler, callbacks := callbacksRecorder()
	fgHandler, foregroundCalls := recordingHandler(nil)
	backgroundCalls := make(chan *JobInfo, 10)
	bgHandler := StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			backgroundCalls <- job
			// Test that background job is really hidden: status callbacks are not reported (empty URL)
			job.ReportProgress(clients.TranscodeStatusPreparing, 0.2)

			time.Sleep(1 * time.Second)
			require.Zero(len(callbacks))
			return testHandlerResult, nil
		},
	}

	doTest := func(strategy Strategy) {
		var coord *Coordinator
		if strategy == StrategyBackgroundExternal {
			coord = NewStubCoordinatorOpts(strategy, callbackHandler, fgHandler, bgHandler)
		} else if strategy == StrategyBackgroundMist {
			coord = NewStubCoordinatorOpts(strategy, callbackHandler, bgHandler, fgHandler)
		} else {
			t.Fatalf("Unexpected strategy: %s", strategy)
		}

		coord.StartUploadJob(testJob)

		msg := requireReceive(t, callbacks, 1*time.Second)
		require.NotZero(msg.URL)
		require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

		fgJob := requireReceive(t, foregroundCalls, 1*time.Second)
		require.Equal("123", fgJob.RequestID)
		bgJob := requireReceive(t, backgroundCalls, 1*time.Second)
		require.Equal("bg_123", bgJob.RequestID)
		require.NotEqual(fgJob.StreamName, bgJob.StreamName)

		// Test that foreground job is the real one: status callbacks ARE reported
		msg = requireReceive(t, callbacks, 1*time.Second)
		require.NotZero(msg.URL)
		require.Equal(clients.TranscodeStatusCompleted.String(), msg.Status)
		require.Equal("123", msg.RequestID)

		time.Sleep(1 * time.Second)
		require.Zero(len(foregroundCalls))
		require.Zero(len(backgroundCalls))
		require.Zero(len(callbacks))
	}

	doTest(StrategyBackgroundExternal)
	doTest(StrategyBackgroundMist)
}

func TestCoordinatorFallbackStrategySuccess(t *testing.T) {
	require := require.New(t)

	callbackHandler, callbacks := callbacksRecorder()
	mist, mistCalls := recordingHandler(nil)
	external, externalCalls := recordingHandler(nil)

	coord := NewStubCoordinatorOpts(StrategyFallbackExternal, callbackHandler, mist, external)

	// Start a job that will complete successfully on mist, which should not
	// trigger the external pipeline
	coord.StartUploadJob(testJob)

	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

	mistJob := requireReceive(t, mistCalls, 1*time.Second)
	require.Equal("123", mistJob.RequestID)

	// Check successful completion of the mist event
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusCompleted.String(), msg.Status)

	time.Sleep(1 * time.Second)
	require.Zero(len(mistCalls))
	require.Zero(len(callbacks))
	// nothing should have happened on the external flow
	require.Zero(len(externalCalls))
}

func TestCoordinatorFallbackStrategyFailure(t *testing.T) {
	require := require.New(t)

	callbackHandler, callbacks := callbacksRecorder()
	mist, mistCalls := recordingHandler(errors.New("mist error"))
	externalCalls := make(chan *JobInfo, 10)
	external := StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			externalCalls <- job

			// Check that progress is still reported
			job.ReportProgress(clients.TranscodeStatusPreparing, 0.2)
			msg := requireReceive(t, callbacks, 1*time.Second)
			require.NotZero(msg.URL)
			require.Equal("123", msg.RequestID)
			require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

			return testHandlerResult, nil
		},
	}

	coord := NewStubCoordinatorOpts(StrategyFallbackExternal, callbackHandler, mist, external)

	// Start a job which mist will fail and only then call the external one
	coord.StartUploadJob(testJob)

	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

	mistJob := requireReceive(t, mistCalls, 1*time.Second)
	require.Equal("123", mistJob.RequestID)

	// TODO: This should actually not be received but filtered transparently.
	// Final caller should receive only one terminal status update (error or success).
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusError.String(), msg.Status)

	// External provider pipeline will trigger the initial preparing trigger as well
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

	meconJob := requireReceive(t, externalCalls, 1*time.Second)
	require.Equal("123", meconJob.RequestID)
	require.Equal(mistJob.StreamName, meconJob.StreamName)

	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusCompleted.String(), msg.Status)

	time.Sleep(1 * time.Second)
	require.Zero(len(mistCalls))
	require.Zero(len(externalCalls))
	require.Zero(len(callbacks))
}

func allFailingHandler(t *testing.T) Handler {
	return StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			require.Fail(t, "Unexpected handleStartUploadJob")
			panic("unreachable")
		},
		handleRecordingEndTrigger: func(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
			require.Fail(t, "Unexpected handleRecordingEndTrigger")
			panic("unreachable")
		},
		handlePushEndTrigger: func(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
			require.Fail(t, "Unexpected handlePushEndTrigger")
			panic("unreachable")
		},
	}
}

func callbacksRecorder() (clients.TranscodeStatusClient, <-chan clients.TranscodeStatusMessage) {
	callbacks := make(chan clients.TranscodeStatusMessage, 10)
	handler := func(msg clients.TranscodeStatusMessage) {
		// background jobs send updates without a callback URL, which are ignored by
		// the callbacks client. Only record the real ones here.
		if msg.URL != "" {
			callbacks <- msg
		}
	}
	return clients.TranscodeStatusFunc(handler), callbacks
}

func recordingHandler(err error) (Handler, <-chan *JobInfo) {
	jobs := make(chan *JobInfo, 10)
	handler := StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			jobs <- job
			if err != nil {
				return nil, err
			}
			return testHandlerResult, nil
		},
	}
	return handler, jobs
}

func requireReceive[T any](t *testing.T, ch <-chan T, timeout time.Duration) T {
	select {
	case job := <-ch:
		return job
	case <-time.After(timeout):
		require.Fail(t, "did not receive expected message")
		panic("unreachable")
	}
}
