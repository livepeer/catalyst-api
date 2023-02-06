package pipeline

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/video"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

var (
	testHandlerResult = &HandlerOutput{
		Result: &UploadJobResult{video.InputVideo{}, []clients.OutputVideo{
			{Type: "object_store", Manifest: "manifest", Videos: []clients.OutputVideoFile{{}}},
		}},
	}
	testJob = UploadJobPayload{
		RequestID:   "123",
		SourceFile:  "source-file",
		TargetURL:   &url.URL{Scheme: "s3+https", Host: "storage.google.com", Path: "/bucket/key", User: url.UserPassword("user", "pass")},
		CallbackURL: "http://localhost:3000/dummy",
	}
)

func TestCoordinatorDoesNotBlock(t *testing.T) {
	require := require.New(t)

	callbackHandler, callbacks := callbacksRecorder()
	barrier := make(chan struct{})
	var running atomic.Bool
	blockHandler := &StubHandler{
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
	require.Equal(clients.TranscodeStatusPreparing, msg.Status)

	close(barrier)
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusError, msg.Status)
	require.Contains(msg.Error, "test error")

	require.Zero(len(callbacks))
}

func TestCoordinatorPropagatesJobInfoChanges(t *testing.T) {
	require := require.New(t)

	barrier := make(chan struct{})
	done := make(chan struct{}, 1)
	blockHandler := &StubHandler{
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
	blockHandler := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			panic("oh no!")
		},
	}
	coord := NewStubCoordinatorOpts("", callbackHandler, blockHandler, blockHandler)

	coord.StartUploadJob(testJob)

	require.Equal(1, len(callbacks))
	require.Equal(clients.TranscodeStatusPreparing, (<-callbacks).Status)

	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusError, msg.Status)
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
	bgHandler := &StubHandler{
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
		require.Equal(clients.TranscodeStatusPreparing, msg.Status)

		fgJob := requireReceive(t, foregroundCalls, 1*time.Second)
		require.Equal("123", fgJob.RequestID)
		bgJob := requireReceive(t, backgroundCalls, 1*time.Second)
		require.Equal("bg_123", bgJob.RequestID)
		require.NotEqual(fgJob.StreamName, bgJob.StreamName)

		// Test that foreground job is the real one: status callbacks ARE reported
		msg = requireReceive(t, callbacks, 1*time.Second)
		require.NotZero(msg.URL)
		require.Equal(clients.TranscodeStatusCompleted, msg.Status)
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
	require.Equal(clients.TranscodeStatusPreparing, msg.Status)

	mistJob := requireReceive(t, mistCalls, 1*time.Second)
	require.Equal("123", mistJob.RequestID)

	// Check successful completion of the mist event
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusCompleted, msg.Status)

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
	external := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			externalCalls <- job
			job.ReportProgress(clients.TranscodeStatusPreparing, 0.2)
			return testHandlerResult, nil
		},
	}

	coord := NewStubCoordinatorOpts(StrategyFallbackExternal, callbackHandler, mist, external)

	// Start a job which mist will fail and only then call the external one
	coord.StartUploadJob(testJob)

	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing, msg.Status)

	mistJob := requireReceive(t, mistCalls, 1*time.Second)
	require.Equal("123", mistJob.RequestID)

	// External provider pipeline will trigger the initial preparing trigger as well
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing, msg.Status)

	meconJob := requireReceive(t, externalCalls, 1*time.Second)
	require.Equal("123", meconJob.RequestID)
	require.Equal(mistJob.StreamName, meconJob.StreamName)

	// Check that the progress reported in the fallback handler is still reported
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.NotZero(msg.URL)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing, msg.Status)
	require.Equal(clients.OverallCompletionRatio(clients.TranscodeStatusPreparing, 0.2), msg.CompletionRatio)

	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusCompleted, msg.Status)

	time.Sleep(1 * time.Second)
	require.Zero(len(mistCalls))
	require.Zero(len(externalCalls))
	require.Zero(len(callbacks))
}

func TestAllowsOverridingStrategyOnRequest(t *testing.T) {
	require := require.New(t)

	mist, mistCalls := recordingHandler(errors.New("mist error"))
	external, externalCalls := recordingHandler(nil)

	// create coordinator with strategy catalyst dominance (external should never be called)
	coord := NewStubCoordinatorOpts(StrategyCatalystDominance, nil, mist, external)

	// Override the strategy to background mist, which will call the external provider *and* the mist provider
	p := testJob
	p.PipelineStrategy = StrategyBackgroundMist
	coord.StartUploadJob(p)

	// Check that it was really called
	meconJob := requireReceive(t, externalCalls, 1*time.Second)
	require.Equal("123", meconJob.RequestID)
	require.Equal("catalyst_vod_123", meconJob.StreamName)

	// Sanity check that mist also ran (in background)
	mistJob := requireReceive(t, mistCalls, 1*time.Second)
	require.Equal("bg_"+meconJob.RequestID, mistJob.RequestID)
	require.Equal("catalyst_vod_bg_"+meconJob.RequestID, mistJob.StreamName)
}

func setJobInfoFields(job *JobInfo) {
	job.catalystRegion = "test region"
	job.sourceCodecVideo = "vid codec"
	job.sourceCodecAudio = "audio codec"
	job.numProfiles = 1
	job.sourceSegments = 2
	job.transcodedSegments = 3
	job.sourceBytes = 4
	job.sourceDurationMs = 5
	job.startTime = time.Unix(0, 0)
}

func TestPipelineCollectedMetrics(t *testing.T) {
	require := require.New(t)

	metricsServer := httptest.NewServer(promhttp.Handler())
	defer metricsServer.Close()

	mist := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			setJobInfoFields(job)
			return testHandlerResult, nil
		},
	}
	external := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			setJobInfoFields(job)
			return testHandlerResult, nil
		},
	}

	db, dbMock, err := sqlmock.New()
	require.NoError(err)
	dbMock.
		ExpectExec("insert into \"vod_completed\".*").
		WithArgs(sqlmock.AnyArg(), 0, sqlmock.AnyArg(), "vid codec", "audio codec", "mist", "test region", "completed", 1, sqlmock.AnyArg(), 2, 3, 4, 5, "source-file", "s3+https://user:xxxxx@storage.google.com/bucket/key").
		WillReturnResult(sqlmock.NewResult(1, 1))

	coord := NewStubCoordinatorOpts(StrategyBackgroundMist, nil, mist, external)
	coord.MetricsDB = db

	coord.StartUploadJob(testJob)

	res, err := http.Get(metricsServer.URL)
	require.NoError(err)

	b, err := io.ReadAll(res.Body)
	require.NoError(err)

	body := string(b)

	require.Contains(body, "# TYPE vod_count")
	require.Contains(body, "# TYPE vod_duration")
	require.Contains(body, "# TYPE vod_source_segments")
	require.Contains(body, "# TYPE vod_source_bytes")
	require.Contains(body, "# TYPE vod_source_duration")

	// the db metrics function is called in a separate go routine so there is a chance we need to wait here
	err = retry(3, 500*time.Millisecond, func() error {
		return dbMock.ExpectationsWereMet()
	})
	require.NoError(err)
}

func retry(attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(sleep)
			sleep *= 2
		}
		err = f()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("failed after %d attempts, last error: %s", attempts, err)
}

func allFailingHandler(t *testing.T) Handler {
	return &StubHandler{
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
	handler := &StubHandler{
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
