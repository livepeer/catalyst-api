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
	coord := NewStubCoordinatorOpts(0, callbackHandler, blockHandler, blockHandler)
	coord.StartUploadJob(UploadJobPayload{RequestID: "123"})
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
		handleStartUploadJob: func(job *JobInfo) error {
			require.Equal("source-file", job.SourceFile)
			<-barrier
			job.SourceFile = "new-source-file"
			return nil
		},
		handleRecordingEndTrigger: func(job *JobInfo, p RecordingEndPayload) error {
			defer func() { done <- struct{}{} }()
			require.Equal("new-source-file", job.SourceFile)
			return nil
		},
	}
	coord := NewStubCoordinatorOpts(0, nil, blockHandler, blockHandler)

	coord.StartUploadJob(UploadJobPayload{RequestID: "123", SourceFile: "source-file"})
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

	callbacks := make(chan clients.TranscodeStatusMessage, 10)
	callbackHandler := func(msg clients.TranscodeStatusMessage) {
		callbacks <- msg
	}
	blockHandler := StubHandler{
		handleStartUploadJob: func(job *JobInfo) error {
			panic("oh no!")
		},
	}
	coord := NewStubCoordinatorOpts(0, callbackHandler, blockHandler, blockHandler)

	coord.StartUploadJob(UploadJobPayload{RequestID: "123", SourceFile: "source-file"})

	require.Equal(1, len(callbacks))
	require.Equal(clients.TranscodeStatusPreparing.String(), (<-callbacks).Status)

	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusError.String(), msg.Status)
	require.Contains(msg.Error, "oh no")
}

func TestCoordinatorCatalystDominance(t *testing.T) {
	require := require.New(t)

	mist, calls := recordingHandler()
	mediaConvert := allFailingHandler(t)
	coord := NewStubCoordinatorOpts(StrategyCatalystDominance, nil, mist, mediaConvert)

	coord.StartUploadJob(UploadJobPayload{RequestID: "123"})

	job := requireReceive(t, calls, 1*time.Second)
	require.Equal("123", job.RequestID)

	time.Sleep(1 * time.Second)
	require.Zero(len(calls))
}

func TestCoordinatorBackgroundJobsStrategies(t *testing.T) {
	require := require.New(t)

	callbacks := make(chan clients.TranscodeStatusMessage, 10)
	callbackHandler := func(msg clients.TranscodeStatusMessage) {
		callbacks <- msg
	}
	mist, mistCalls := recordingHandler()
	mediaConvert, mediaConvertCalls := recordingHandler()

	doTest := func(strategy Strategy) {
		coord := NewStubCoordinatorOpts(strategy, callbackHandler, mist, mediaConvert)
		foregroundCalls, backgroundCalls := mistCalls, mediaConvertCalls
		if strategy == StrategyBackgroundMist {
			foregroundCalls, backgroundCalls = mediaConvertCalls, mistCalls
		} else if strategy != StrategyBackgroundMediaConvert {
			t.Fatalf("Unexpected strategy: %d", strategy)
		}

		coord.StartUploadJob(UploadJobPayload{RequestID: "123"})

		msg := requireReceive(t, callbacks, 1*time.Second)
		require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

		fgJob := requireReceive(t, foregroundCalls, 1*time.Second)
		require.Equal("123", fgJob.RequestID)
		bgJob := requireReceive(t, backgroundCalls, 1*time.Second)
		require.Equal("bg_123", bgJob.RequestID)
		require.NotEqual(fgJob.StreamName, bgJob.StreamName)

		// Test that background job is really hidden: status callbacks are not reported
		bgJob.ReportStatus(clients.NewTranscodeStatusProgress("s3://b/f", "bg_123", clients.TranscodeStatusPreparing, 0.2))
		bgJob.ReportStatus(clients.NewTranscodeStatusCompleted("s3://b/f", "bg_123", clients.InputVideo{}, []clients.OutputVideo{}))
		time.Sleep(1 * time.Second)
		require.Zero(len(callbacks))

		// Test that foreground job is the real one: status callbacks ARE reported
		fgJob.ReportStatus(clients.NewTranscodeStatusCompleted("s3://b/f", "123", clients.InputVideo{}, []clients.OutputVideo{}))
		msg = requireReceive(t, callbacks, 1*time.Second)
		require.Equal(clients.TranscodeStatusCompleted.String(), msg.Status)
		require.Equal("123", msg.RequestID)

		time.Sleep(1 * time.Second)
		require.Zero(len(foregroundCalls))
		require.Zero(len(backgroundCalls))
		require.Zero(len(callbacks))
	}

	doTest(StrategyBackgroundMediaConvert)
	doTest(StrategyBackgroundMist)
}

func TestCoordinatorFallbackStrategySuccess(t *testing.T) {
	require := require.New(t)

	callbacks := make(chan clients.TranscodeStatusMessage, 10)
	callbackHandler := func(msg clients.TranscodeStatusMessage) {
		callbacks <- msg
	}
	mist, mistCalls := recordingHandler()
	mediaConvert, mediaConvertCalls := recordingHandler()

	coord := NewStubCoordinatorOpts(StrategyFallbackMediaConvert, callbackHandler, mist, mediaConvert)

	// Start a job that will complete successfully on mist, which should not trigger mediaconvert
	coord.StartUploadJob(UploadJobPayload{RequestID: "123"})

	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

	mistJob := requireReceive(t, mistCalls, 1*time.Second)
	require.Equal("123", mistJob.RequestID)

	// Send a successful completion status on the mist event
	mistJob.ReportStatus(clients.NewTranscodeStatusCompleted("s3://b/f", "123", clients.InputVideo{}, []clients.OutputVideo{}))
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusCompleted.String(), msg.Status)

	time.Sleep(1 * time.Second)
	require.Zero(len(mistCalls))
	require.Zero(len(callbacks))
	// nothing should have happened on the mediaconvert flow
	require.Zero(len(mediaConvertCalls))
}

func TestCoordinatorFallbackStrategyFailure(t *testing.T) {
	require := require.New(t)

	callbacks := make(chan clients.TranscodeStatusMessage, 10)
	callbackHandler := func(msg clients.TranscodeStatusMessage) {
		callbacks <- msg
	}
	mist, mistCalls := recordingHandler()
	mediaConvert, mediaConvertCalls := recordingHandler()

	coord := NewStubCoordinatorOpts(StrategyFallbackMediaConvert, callbackHandler, mist, mediaConvert)

	// Start a job which mist will fail and only then call mediaconvert
	coord.StartUploadJob(UploadJobPayload{RequestID: "123"})

	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

	mistJob := requireReceive(t, mistCalls, 1*time.Second)
	require.Equal("123", mistJob.RequestID)

	// TODO: This should actually not be received but filtered transparently.
	// Final caller should receive only one terminal status update (error or success).
	mistJob.ReportStatus(clients.NewTranscodeStatusError("s3://b/f", "123", "mist error!"))
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusError.String(), msg.Status)

	// Mediaconvert pipeline will trigger the initial preparing trigger as well
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

	meconJob := requireReceive(t, mediaConvertCalls, 1*time.Second)
	require.Equal("123", meconJob.RequestID)
	require.Equal(mistJob.StreamName, meconJob.StreamName)

	meconJob.ReportStatus(clients.NewTranscodeStatusProgress("s3://b/f", "123", clients.TranscodeStatusPreparing, 0.2))
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing.String(), msg.Status)

	meconJob.ReportStatus(clients.NewTranscodeStatusCompleted("s3://b/f", "123", clients.InputVideo{}, []clients.OutputVideo{}))
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusCompleted.String(), msg.Status)

	time.Sleep(1 * time.Second)
	require.Zero(len(mistCalls))
	require.Zero(len(mediaConvertCalls))
	require.Zero(len(callbacks))
}

func allFailingHandler(t *testing.T) Handler {
	return StubHandler{
		handleStartUploadJob: func(job *JobInfo) error {
			require.Fail(t, "Unexpected handleStartUploadJob")
			return nil
		},
		handleRecordingEndTrigger: func(job *JobInfo, p RecordingEndPayload) error {
			require.Fail(t, "Unexpected handleRecordingEndTrigger")
			return nil
		},
		handlePushEndTrigger: func(job *JobInfo, p PushEndPayload) error {
			require.Fail(t, "Unexpected handlePushEndTrigger")
			return nil
		},
	}
}

func recordingHandler() (Handler, <-chan *JobInfo) {
	jobs := make(chan *JobInfo, 10)
	handler := StubHandler{
		handleStartUploadJob: func(job *JobInfo) error {
			jobs <- job
			return nil
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
