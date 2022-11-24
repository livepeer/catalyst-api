package pipeline

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
)

type Strategy int

const (
	StrategyCatalystDominance Strategy = iota
	StrategyBackgroundMediaConvert
	StrategyBackgroundMist
	StrategyFallbackMediaConvert
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
	HandleStartUploadJob(job *JobInfo) error
	HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) error
	HandlePushEndTrigger(job *JobInfo, p PushEndPayload) error
}

type Coordinator interface {
	StartUploadJob(p UploadJobPayload)
	TriggerRecordingEnd(p RecordingEndPayload)
	TriggerPushEnd(p PushEndPayload)
}

func NewCoordinator(strategy Strategy, mistClient clients.MistAPIClient, statusClient *clients.PeriodicCallbackClient) Coordinator {
	return &coord{
		strategy:         strategy,
		statusClient:     statusClient,
		pipeMist:         &mist{mistClient},
		pipeMediaConvert: &mediaconvert{},
		Jobs:             cache.New[*JobInfo](),
	}
}

func NewStubCoordinator(statusClient *clients.PeriodicCallbackClient) *coord {
	if statusClient == nil {
		statusClient = clients.NewPeriodicCallbackClient(100 * time.Minute)
	}
	return NewCoordinator(StrategyCatalystDominance, clients.StubMistClient{}, statusClient).(*coord)
}

type coord struct {
	strategy     Strategy
	statusClient *clients.PeriodicCallbackClient

	pipeMist         *mist
	pipeMediaConvert *mediaconvert

	Jobs *cache.Cache[*JobInfo]
}

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

func (c *coord) startOneUploadJob(p UploadJobPayload, handler Handler, foreground bool) <-chan bool {
	requestID, innerStatusClient := p.RequestID, c.statusClient
	if !foreground {
		requestID = fmt.Sprintf("bg_%s", requestID)
		innerStatusClient = nil
	}
	streamName := config.SegmentingStreamName(requestID)
	log.AddContext(requestID, "stream_name", streamName)
	result := make(chan bool, 1)
	si := &JobInfo{
		UploadJobPayload: p,
		StreamName:       streamName,
		ReportStatus:     c.transcodeStatusReporter(innerStatusClient, requestID, streamName, result),
		handler:          handler,
	}
	si.ReportStatus(clients.NewTranscodeStatusProgress(si.CallbackURL, requestID, clients.TranscodeStatusPreparing, 0))

	c.Jobs.Store(streamName, si)
	log.Log(si.RequestID, "Wrote to jobs cache")

	go callbackWrapped(si, func() error {
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

func (c *coord) transcodeStatusReporter(statusClient *clients.PeriodicCallbackClient, requestID, streamName string, result chan<- bool) TranscodeStatusReporter {
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

func callbackWrapped(job *JobInfo, f func() error) {
	job.mu.Lock()
	defer job.mu.Unlock()
	err := recovered(f)
	if err != nil {
		job.ReportStatus(clients.NewTranscodeStatusError(job.CallbackURL, job.RequestID, err.Error()))
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
