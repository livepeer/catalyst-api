package pipeline

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
)

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

type RecordingEndPayload struct {
	StreamName                string
	StreamMediaDurationMillis int64
	WrittenBytes              int
}

type PushEndPayload struct {
	StreamName     string
	PushStatus     string
	Last10LogLines string
}

type TranscodeStatusReporter func(clients.TranscodeStatusMessage)

type JobInfo struct {
	mu sync.Mutex
	UploadJobPayload
	StreamName   string
	ReportStatus TranscodeStatusReporter

	handler Handler
}

type Handler interface {
	HandleStartUploadJob(job *JobInfo, p UploadJobPayload) error
	HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) error
	HandlePushEndTrigger(job *JobInfo, p PushEndPayload) error
}

type Coordinator interface {
	StartUploadJob(p UploadJobPayload)
	TriggerRecordingEnd(p RecordingEndPayload)
	TriggerPushEnd(p PushEndPayload)
}

func NewCoordinator(mistClient clients.MistAPIClient, statusClient *clients.PeriodicCallbackClient) Coordinator {
	return &coord{
		mistPipeline: &mist{mistClient},
		statusClient: statusClient,
		Jobs:         cache.New[*JobInfo](),
	}
}

func NewStubCoordinator(statusClient *clients.PeriodicCallbackClient) *coord {
	if statusClient == nil {
		statusClient = clients.NewPeriodicCallbackClient(100 * time.Minute)
	}
	return NewCoordinator(clients.StubMistClient{}, statusClient).(*coord)
}

type coord struct {
	mistPipeline *mist
	statusClient *clients.PeriodicCallbackClient

	Jobs *cache.Cache[*JobInfo]
}

func (c *coord) StartUploadJob(p UploadJobPayload) {
	requestID, streamName := p.RequestID, config.SegmentingStreamName(p.RequestID)
	log.AddContext(requestID, "stream_name", streamName)
	si := &JobInfo{
		UploadJobPayload: p,
		StreamName:       streamName,
		ReportStatus:     c.transcodeStatusReporter(c.statusClient, requestID, streamName),
		handler:          c.mistPipeline,
	}
	si.ReportStatus(clients.NewTranscodeStatusProgress(si.CallbackURL, requestID, clients.TranscodeStatusPreparing, 0))

	c.Jobs.Store(streamName, si)
	log.Log(si.RequestID, "Wrote to jobs cache")

	go callbackWrapped(si, func() error {
		return si.handler.HandleStartUploadJob(si, p)
	})
}

func (c *coord) TriggerRecordingEnd(p RecordingEndPayload) {
	si := c.Jobs.Get(p.StreamName)
	if si == nil {
		log.Log(si.RequestID, "RECORDING_END trigger invoked for unknown stream")
		return
	}
	go callbackWrapped(si, func() error {
		return si.handler.HandleRecordingEndTrigger(si, p)
	})
}

func (c *coord) TriggerPushEnd(p PushEndPayload) {
	si := c.Jobs.Get(p.StreamName)
	if si == nil {
		log.Log(si.RequestID, "PUSH_END trigger invoked for unknown stream", "streamName", p.StreamName)
		return
	}
	go callbackWrapped(si, func() error {
		return si.handler.HandlePushEndTrigger(si, p)
	})
}

func (c *coord) transcodeStatusReporter(statusClient *clients.PeriodicCallbackClient, requestID, streamName string) TranscodeStatusReporter {
	return func(tsm clients.TranscodeStatusMessage) {
		if statusClient != nil {
			statusClient.SendTranscodeStatus(tsm)
		}
		// nolint:errcheck
		go recovered(func() error {
			rawStatus, _ := json.Marshal(tsm)
			log.Log(requestID, "Pipeline coordinator status update",
				"timestamp", tsm.Timestamp, "status", tsm.Status, "completion_ratio", tsm.CompletionRatio,
				"error", tsm.Error, "raw", string(rawStatus))

			// Automatically delete jobs after terminal status updates
			if tsm.IsTerminal() {
				c.Jobs.Remove(streamName)
				log.Log(requestID, "Deleted from Jobs Cache")
			}
			return nil
		})
	}
}

func callbackWrapped(job *JobInfo, f func() error) {
	job.mu.Lock()
	defer job.mu.Unlock()
	err := recovered(f)
	if err != nil {
		job.ReportStatus(clients.NewTranscodeStatusError(job.CallbackURL, job.RequestID, err.Error()))
		metrics.Metrics.UploadVODPipelineFailureCount.Inc()
	}
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
