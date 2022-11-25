package pipeline

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
)

// Strategy indicates how the pipelines should be coordinated. Mainly changes
// which pipelines to execute, in what order, and which ones go in background.
// Background pipelines are only logged and are not reported back to the client.
type Strategy int

const (
	// Only execute the Catalyst (Mist) pipeline.
	StrategyCatalystDominance Strategy = iota
	// Execute the Mist pipeline in foreground and MediaConvert in background.
	StrategyBackgroundMediaConvert
	// Execute the MediaConvert pipeline in foreground and Mist in background.
	StrategyBackgroundMist
	// Execute the Mist pipeline fist and fallback to MediaConvert on errors.
	StrategyFallbackMediaConvert
)

// UploadJobPayload is the required payload to start an upload job.
type UploadJobPayload struct {
	SourceFile            string
	CallbackURL           string
	TargetURL             *url.URL
	SegmentingTargetURL   string
	AccessToken           string
	TranscodeAPIUrl       string
	HardcodedBroadcasters string
	RequestID             string
	Profiles              []clients.EncodedProfile
}

// RecordingEndPayload is the required payload from a recording end trigger.
type RecordingEndPayload struct {
	StreamName                string
	StreamMediaDurationMillis int64
	WrittenBytes              int
}

// PushsEndPayload is the required payload from a push end trigger.
type PushEndPayload struct {
	StreamName     string
	PushStatus     string
	Last10LogLines string
}

// TranscodeStatusReporter represents a function to report status on the job.
type TranscodeStatusReporter func(clients.TranscodeStatusMessage)

// JobInfo represents the state of a single upload job.
type JobInfo struct {
	mu sync.Mutex
	UploadJobPayload
	StreamName   string
	ReportStatus TranscodeStatusReporter

	handler Handler
}

// Handler represents a single pipeline handler to be plugged to the coordinator
// general job management logic.
//
// Implementers of the interface only need to worry about the logic they want to
// execute, already receiving the *JobInfo as an argument and running in a
// locked context on that object.
//
// Hence there is also the restriction that only one of these functions may
// execute concurrently. All functions run in a goroutine, so they can block as
// much as needed and they should not leave background jobs running after
// returning.
type Handler interface {
	// Handle start job request. This may start async processes like on mist an
	// wait for triggers or do the full job synchronously on exeution.
	HandleStartUploadJob(job *JobInfo) error
	// Handle the recording_end trigger in case a mist stream is created (only
	// used for segmenting today).
	HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) error
	// Handle the push_end trigger in case a mist stream is created (only used for
	// segmenting today).
	HandlePushEndTrigger(job *JobInfo, p PushEndPayload) error
}

// Coordinator is the main interface to handle the pipelines. It should be
// called directly from the API handlers and never blocks on execution but
// rather schedules some routines in background.
type Coordinator interface {
	// Starts a new upload job.
	StartUploadJob(p UploadJobPayload)
	// Handle RECORDING_END trigger from mist.
	TriggerRecordingEnd(p RecordingEndPayload)
	// Handle PUSH_END trigger from mist.
	TriggerPushEnd(p PushEndPayload)
}

func NewCoordinator(strategy Strategy, mistClient clients.MistAPIClient, statusClient TranscodeStatusReporter) Coordinator {
	return &coord{
		strategy:         strategy,
		statusClient:     statusClient,
		pipeMist:         &mist{mistClient},
		pipeMediaConvert: &mediaconvert{},
		Jobs:             cache.New[*JobInfo](),
	}
}

func NewStubCoordinator() *coord {
	return NewStubCoordinatorOpts(nil, nil, nil)
}

func NewStubCoordinatorOpts(statusClient TranscodeStatusReporter, pipeMist, pipeMediaConvert Handler) *coord {
	if statusClient == nil {
		statusClient = func(tsm clients.TranscodeStatusMessage) {}
	}
	if pipeMist == nil {
		pipeMist = &mist{clients.StubMistClient{}}
	}
	if pipeMediaConvert == nil {
		pipeMediaConvert = &mediaconvert{}
	}
	return &coord{
		strategy:         StrategyCatalystDominance,
		statusClient:     statusClient,
		pipeMist:         pipeMist,
		pipeMediaConvert: pipeMediaConvert,
		Jobs:             cache.New[*JobInfo](),
	}
}

type coord struct {
	strategy     Strategy
	statusClient TranscodeStatusReporter

	pipeMist, pipeMediaConvert Handler

	Jobs *cache.Cache[*JobInfo]
}

// This has the main logic regarding the pipeline strategy. It starts jobs and
// handles processing the response and triggering a fallback if appropriate.
func (c *coord) StartUploadJob(p UploadJobPayload) {
	switch c.strategy {
	case StrategyCatalystDominance:
		c.startOneUploadJob(p, c.pipeMist, true)
	case StrategyBackgroundMediaConvert:
		c.startOneUploadJob(p, c.pipeMist, true)
		c.startOneUploadJob(p, c.pipeMediaConvert, false)
	case StrategyBackgroundMist:
		c.startOneUploadJob(p, c.pipeMediaConvert, true)
		c.startOneUploadJob(p, c.pipeMist, false)
	case StrategyFallbackMediaConvert:
		// nolint:errcheck
		go recovered(func() error {
			// TODO: Also need to filter the error callback from the first pipeline so
			// we can silently do the MediaConvert flow underneath.
			success := <-c.startOneUploadJob(p, c.pipeMist, true)
			if !success {
				c.startOneUploadJob(p, c.pipeMediaConvert, true)
			}
			return nil
		})
	}
}

// Starts a single upload job with specified pipeline Handler. If the job is
// running in background (foreground=false) then:
//   - the job will have a different requestID
//   - no transcode status updates will be reported to the caller, only logged
//   - TODO: the output will go to a different location than the real job
func (c *coord) startOneUploadJob(p UploadJobPayload, handler Handler, foreground bool) <-chan bool {
	statusClient := c.statusClient
	if !foreground {
		p.RequestID = fmt.Sprintf("bg_%s", p.RequestID)
		statusClient = nil
		// TODO: change the output path as well
	}
	streamName := config.SegmentingStreamName(p.RequestID)
	log.AddContext(p.RequestID, "stream_name", streamName)
	result := make(chan bool, 1)
	si := &JobInfo{
		UploadJobPayload: p,
		StreamName:       streamName,
		ReportStatus:     c.transcodeStatusReporter(statusClient, p.RequestID, streamName, result),
		handler:          handler,
	}
	si.ReportStatus(clients.NewTranscodeStatusProgress(si.CallbackURL, si.RequestID, clients.TranscodeStatusPreparing, 0))

	c.Jobs.Store(streamName, si)
	log.Log(si.RequestID, "Wrote to jobs cache")

	runHandlerAsync(si, func() error {
		return si.handler.HandleStartUploadJob(si)
	})
	return result
}

func (c *coord) TriggerRecordingEnd(p RecordingEndPayload) {
	si := c.Jobs.Get(p.StreamName)
	if si == nil {
		log.Log(si.RequestID, "RECORDING_END trigger invoked for unknown stream")
		return
	}
	runHandlerAsync(si, func() error {
		return si.handler.HandleRecordingEndTrigger(si, p)
	})
}

func (c *coord) TriggerPushEnd(p PushEndPayload) {
	si := c.Jobs.Get(p.StreamName)
	if si == nil {
		log.Log(si.RequestID, "PUSH_END trigger invoked for unknown stream", "streamName", p.StreamName)
		return
	}
	runHandlerAsync(si, func() error {
		return si.handler.HandlePushEndTrigger(si, p)
	})
}

// transcodeStatusReporter that wraps the callback client into a function with
// some additional logic required on the coordinator. Especifically:
//   - Allows using it as a fake reporter, which doesn't acctually call any
//     callbacks and only logs the request.
//   - Logs every status update sent
//   - Handles terminal status updates and removes the job from the cache, as
//     well as sending the result back on a channel (&close it after sending).
func (c *coord) transcodeStatusReporter(sendStatus TranscodeStatusReporter, requestID, streamName string, result chan<- bool) TranscodeStatusReporter {
	return func(tsm clients.TranscodeStatusMessage) {
		if sendStatus != nil {
			sendStatus(tsm)
		}
		// nolint:errcheck
		go recovered(func() error {
			rawStatus, _ := json.Marshal(tsm)
			log.Log(requestID, "Pipeline coordinator status update",
				"timestamp", tsm.Timestamp, "status", tsm.Status, "completion_ratio", tsm.CompletionRatio,
				"error", tsm.Error, "raw", string(rawStatus))

			// Automatically delete jobs after terminal status updates
			if tsm.IsTerminal() {
				defer close(result)

				c.Jobs.Remove(streamName)
				log.Log(requestID, "Deleted from Jobs Cache")

				success := tsm.Status == clients.TranscodeStatusCompleted.String()
				metrics.Metrics.UploadVODPipelineResults.WithLabelValues(strconv.FormatBool(success)).Inc()
				result <- success
			}
			return nil
		})
	}
}

// runHandlerAsync starts a background go-routine to run the handler function
// safely. It locks on the JobInfo object to allow safe mutations inside the
// handler. It also handles panics and errors, turning them into a transcode
// status update with an error result.
func runHandlerAsync(job *JobInfo, handler func() error) {
	go func() {
		job.mu.Lock()
		defer job.mu.Unlock()
		err := recovered(handler)
		if err != nil {
			job.ReportStatus(clients.NewTranscodeStatusError(job.CallbackURL, job.RequestID, err.Error()))
		}
	}()
}

func recovered(f func() error) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			log.LogNoRequestID("panic in callback goroutine, recovering", "err", rec)
			err = fmt.Errorf("panic in callback goroutine: %v", rec)
		}
	}()
	return f()
}
