package pipeline

import (
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
	// Zero value
	StrategyInvalid Strategy = iota
	// Only execute the Catalyst (Mist) pipeline.
	StrategyCatalystDominance
	// Only execute the MediaConvert pipeline
	StrategyMediaConvertDominance
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

// JobInfo represents the state of a single upload job.
type JobInfo struct {
	mu sync.Mutex
	UploadJobPayload
	StreamName string

	handler      Handler
	statusClient clients.TranscodeStatusClient
	result       chan bool
}

func (j *JobInfo) ReportProgress(stage clients.TranscodeStatus, completionRatio float64) {
	tsm := clients.NewTranscodeStatusProgress(j.CallbackURL, j.RequestID, stage, completionRatio)
	j.statusClient.SendTranscodeStatus(tsm)
}

// Coordinator provides the main interface to handle the pipelines. It should be
// called directly from the API handlers and never blocks on execution, but
// rather schedules routines to do the actual work in background.
type Coordinator struct {
	strategy     Strategy
	statusClient clients.TranscodeStatusClient

	pipeMist, pipeMediaConvert Handler

	Jobs *cache.Cache[*JobInfo]
}

func NewCoordinator(strategy Strategy, mistClient clients.MistAPIClient, statusClient clients.TranscodeStatusClient) *Coordinator {
	return &Coordinator{
		strategy:         strategy,
		statusClient:     statusClient,
		pipeMist:         &mist{mistClient},
		pipeMediaConvert: &mediaconvert{},
		Jobs:             cache.New[*JobInfo](),
	}
}

func NewStubCoordinator() *Coordinator {
	return NewStubCoordinatorOpts(StrategyCatalystDominance, nil, nil, nil)
}

func NewStubCoordinatorOpts(strategy Strategy, statusClient clients.TranscodeStatusClient, pipeMist, pipeMediaConvert Handler) *Coordinator {
	if strategy == StrategyInvalid {
		strategy = StrategyCatalystDominance
	}
	if statusClient == nil {
		statusClient = clients.TranscodeStatusFunc(func(tsm clients.TranscodeStatusMessage) {})
	}
	if pipeMist == nil {
		pipeMist = &mist{clients.StubMistClient{}}
	}
	if pipeMediaConvert == nil {
		pipeMediaConvert = &mediaconvert{}
	}
	return &Coordinator{
		strategy:         strategy,
		statusClient:     statusClient,
		pipeMist:         pipeMist,
		pipeMediaConvert: pipeMediaConvert,
		Jobs:             cache.New[*JobInfo](),
	}
}

// Starts a new upload job.
//
// This has the main logic regarding the pipeline strategy. It starts jobs and
// handles processing the response and triggering a fallback if appropriate.
func (c *Coordinator) StartUploadJob(p UploadJobPayload) {
	switch c.strategy {
	case StrategyCatalystDominance:
		c.startOneUploadJob(p, c.pipeMist, true)
	case StrategyMediaConvertDominance:
		c.startOneUploadJob(p, c.pipeMediaConvert, true)
	case StrategyBackgroundMediaConvert:
		c.startOneUploadJob(p, c.pipeMist, true)
		c.startOneUploadJob(p, c.pipeMediaConvert, false)
	case StrategyBackgroundMist:
		c.startOneUploadJob(p, c.pipeMediaConvert, true)
		c.startOneUploadJob(p, c.pipeMist, false)
	case StrategyFallbackMediaConvert:
		// nolint:errcheck
		go recovered(func() (t bool, e error) {
			// TODO: Also need to filter the error callback from the first pipeline so
			// we can silently do the MediaConvert flow underneath.
			success := <-c.startOneUploadJob(p, c.pipeMist, true)
			if !success {
				c.startOneUploadJob(p, c.pipeMediaConvert, true)
			}
			return
		})
	}
}

// Starts a single upload job with specified pipeline Handler. If the job is
// running in background (foreground=false) then:
//   - the job will have a different requestID
//   - no transcode status updates will be reported to the caller, only logged
//   - TODO: the output will go to a different location than the real job
func (c *Coordinator) startOneUploadJob(p UploadJobPayload, handler Handler, foreground bool) <-chan bool {
	if !foreground {
		p.RequestID = fmt.Sprintf("bg_%s", p.RequestID)
		p.CallbackURL = ""
		// TODO: change the output path as well
	}
	streamName := config.SegmentingStreamName(p.RequestID)
	log.AddContext(p.RequestID, "stream_name", streamName)

	si := &JobInfo{
		UploadJobPayload: p,
		StreamName:       streamName,
		statusClient:     c.statusClient,
		handler:          handler,
		result:           make(chan bool, 1),
	}
	si.ReportProgress(clients.TranscodeStatusPreparing, 0)

	c.Jobs.Store(streamName, si)
	log.Log(si.RequestID, "Wrote to jobs cache")

	c.runHandlerAsync(si, func() (*HandlerOutput, error) {
		return si.handler.HandleStartUploadJob(si)
	})
	return si.result
}

// TriggerRecordingEnd handles RECORDING_END trigger from mist.
func (c *Coordinator) TriggerRecordingEnd(p RecordingEndPayload) {
	si := c.Jobs.Get(p.StreamName)
	if si == nil {
		log.LogNoRequestID("RECORDING_END trigger invoked for unknown stream", "stream_name", p.StreamName)
		return
	}
	c.runHandlerAsync(si, func() (*HandlerOutput, error) {
		return si.handler.HandleRecordingEndTrigger(si, p)
	})
}

// TriggerPushEnd handles PUSH_END trigger from mist.
func (c *Coordinator) TriggerPushEnd(p PushEndPayload) {
	si := c.Jobs.Get(p.StreamName)
	if si == nil {
		log.Log(si.RequestID, "PUSH_END trigger invoked for unknown stream", "streamName", p.StreamName)
		return
	}
	c.runHandlerAsync(si, func() (*HandlerOutput, error) {
		return si.handler.HandlePushEndTrigger(si, p)
	})
}

func (c *Coordinator) InFlightMistPipelineJobs() int {
	keys := c.Jobs.GetKeys()
	count := 0
	for _, k := range keys {
		if c.Jobs.Get(k).handler == c.pipeMist {
			count++
		}
	}
	return count
}

// runHandlerAsync starts a background go-routine to run the handler function
// safely. It locks on the JobInfo object to allow safe mutations inside the
// handler. It also handles panics and errors, turning them into a transcode
// status update with an error result.
func (c *Coordinator) runHandlerAsync(job *JobInfo, handler func() (*HandlerOutput, error)) {
	// nolint:errcheck
	go recovered(func() (t bool, e error) {
		job.mu.Lock()
		defer job.mu.Unlock()

		out, err := recovered(handler)
		if err != nil || !out.Continue {
			c.finishJob(job, out, err)
		}
		// dummy
		return
	})
}

func (c *Coordinator) finishJob(job *JobInfo, out *HandlerOutput, err error) {
	defer close(job.result)
	var tsm clients.TranscodeStatusMessage
	if err != nil {
		tsm = clients.NewTranscodeStatusError(job.CallbackURL, job.RequestID, err.Error())
	} else {
		tsm = clients.NewTranscodeStatusCompleted(job.CallbackURL, job.RequestID, out.Result.InputVideo, out.Result.Outputs)
	}
	job.statusClient.SendTranscodeStatus(tsm)

	// Automatically delete jobs after an error or result
	success := err == nil
	c.Jobs.Remove(job.StreamName)

	log.Log(job.RequestID, "Finished job and deleted from job cache", "success", success)
	metrics.Metrics.UploadVODPipelineResults.WithLabelValues(strconv.FormatBool(success)).Inc()

	job.result <- success
}

func recovered[T any](f func() (T, error)) (t T, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			log.LogNoRequestID("panic in pipeline handler background goroutine, recovering", "err", rec)
			err = fmt.Errorf("panic in pipeline handler: %v", rec)
		}
	}()
	return f()
}
