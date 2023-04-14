package pipeline

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/video"
)

// Strategy indicates how the pipelines should be coordinated. Mainly changes
// which pipelines to execute, in what order, and which ones go in background.
// Background pipelines are only logged and are not reported back to the client.
type Strategy string

const (
	// Only execute the Catalyst (Mist) pipeline.
	StrategyCatalystDominance Strategy = "catalyst"
	// Only execute the external pipeline.
	StrategyExternalDominance Strategy = "external"
	// Only execute the FFMPEG / Livepeer pipeline
	StrategyCatalystFfmpegDominance Strategy = "catalyst_ffmpeg"
	// Execute the Mist pipeline in foreground and the external transcoder in background.
	StrategyBackgroundExternal Strategy = "background_external"
	// Execute the external transcoder pipeline in foreground and Mist in background.
	StrategyBackgroundMist Strategy = "background_mist"
	// Execute the FFMPEG pipeline first and fallback to the external transcoding
	// provider on errors.
	StrategyFallbackExternal Strategy = "fallback_external"
)

const (
	// Only mp4s of maxMP4OutDuration will have MP4s generated for each rendition
	maxMP4OutDuration = 2 * time.Minute
)

func (s Strategy) IsValid() bool {
	switch s {
	case StrategyCatalystDominance, StrategyExternalDominance, StrategyCatalystFfmpegDominance, StrategyBackgroundExternal, StrategyBackgroundMist, StrategyFallbackExternal:
		return true
	default:
		return false
	}
}

// UploadJobPayload is the required payload to start an upload job.
type UploadJobPayload struct {
	SourceFile            string
	CallbackURL           string
	HlsTargetURL          *url.URL
	Mp4TargetURL          *url.URL
	Mp4OnlyShort          bool
	AccessToken           string
	TranscodeAPIUrl       string
	HardcodedBroadcasters string
	RequestID             string
	ExternalID            string
	Profiles              []video.EncodedProfile
	PipelineStrategy      Strategy
	TargetSegmentSizeSecs int64
	GenerateMP4           bool
	InputFileInfo         video.InputVideo
	SignedSourceURL       string
	InFallbackMode        bool
	MistSupported         bool
}

// UploadJobResult is the object returned by the successful execution of an
// upload job.
type UploadJobResult struct {
	InputVideo video.InputVideo
	Outputs    []video.OutputVideo
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
	// this is only set&used internally in the mist pipeline
	SegmentingTargetURL string
	SourceOutputURL     string

	handler      Handler
	hasFallback  bool
	statusClient clients.TranscodeStatusClient
	startTime    time.Time
	result       chan bool

	sourceBytes        int64
	sourceSegments     int
	sourceDurationMs   int64
	sourceCodecVideo   string
	sourceCodecAudio   string
	sourceWidth        int64
	sourceHeight       int64
	sourceFPS          float64
	sourceBitrateVideo int64
	sourceBitrateAudio int64
	sourceChannels     int
	sourceSampleRate   int
	sourceSampleBits   int

	transcodedSegments    int
	targetSegmentSizeSecs int64
	pipeline              string
	catalystRegion        string
	numProfiles           int
	state                 string
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

	pipeMist, pipeFfmpeg, pipeExternal Handler

	Jobs      *cache.Cache[*JobInfo]
	MetricsDB *sql.DB
	InputCopy clients.InputCopier
}

func NewCoordinator(strategy Strategy, mistClient clients.MistAPIClient,
	sourceOutputURL, extTranscoderURL string, statusClient clients.TranscodeStatusClient, metricsDB *sql.DB) (*Coordinator, error) {

	if !strategy.IsValid() {
		return nil, fmt.Errorf("invalid strategy: %s", strategy)
	}

	var extTranscoder clients.TranscodeProvider
	if extTranscoderURL != "" {
		var err error
		extTranscoder, err = clients.ParseTranscodeProviderURL(extTranscoderURL)
		if err != nil {
			return nil, fmt.Errorf("error creating external transcoder: %v", err)
		}
	}
	if strategy != StrategyCatalystDominance && extTranscoder == nil {
		return nil, fmt.Errorf("external transcoder is required for strategy: %v", strategy)
	}

	return &Coordinator{
		strategy:     strategy,
		statusClient: statusClient,
		pipeFfmpeg:   &ffmpeg{SourceOutputUrl: sourceOutputURL},
		pipeMist:     &mist{MistClient: mistClient, SourceOutputUrl: sourceOutputURL},
		pipeExternal: &external{extTranscoder},
		Jobs:         cache.New[*JobInfo](),
		MetricsDB:    metricsDB,
		InputCopy: &clients.InputCopy{
			Probe:           video.Probe{},
			SourceOutputUrl: sourceOutputURL,
		},
	}, nil
}

func NewStubCoordinator() *Coordinator {
	return NewStubCoordinatorOpts(StrategyCatalystDominance, nil, nil, nil, nil, "")
}

func NewStubCoordinatorOpts(strategy Strategy, statusClient clients.TranscodeStatusClient, pipeMist, pipeFfmpeg, pipeExternal Handler, sourceOutputUrl string) *Coordinator {
	if strategy == "" {
		strategy = StrategyCatalystDominance
	}
	if statusClient == nil {
		statusClient = clients.TranscodeStatusFunc(func(tsm clients.TranscodeStatusMessage) {})
	}
	if pipeMist == nil {
		pipeMist = &mist{MistClient: clients.StubMistClient{}, SourceOutputUrl: sourceOutputUrl}
	}
	if pipeFfmpeg == nil {
		pipeFfmpeg = &ffmpeg{SourceOutputUrl: sourceOutputUrl}
	}
	if pipeExternal == nil {
		pipeExternal = &external{}
	}
	return &Coordinator{
		strategy:     strategy,
		statusClient: statusClient,
		pipeMist:     pipeMist,
		pipeFfmpeg:   pipeFfmpeg,
		pipeExternal: pipeExternal,
		Jobs:         cache.New[*JobInfo](),
		InputCopy: &clients.InputCopy{
			Probe: video.Probe{},
		},
	}
}

// Starts a new upload job.
//
// This has the main logic regarding the pipeline strategy. It starts jobs and
// handles processing the response and triggering a fallback if appropriate.
func (c *Coordinator) StartUploadJob(p UploadJobPayload) {
	// A bit hacky - this is effectively a dummy job object to allow us to reuse the runHandlerAsync and
	// progress reporting logic. The real job objects still get created in startOneUploadJob().
	si := &JobInfo{
		UploadJobPayload: p,
		statusClient:     c.statusClient,
		startTime:        time.Now(),

		numProfiles:    len(p.Profiles),
		state:          "segmenting",
		catalystRegion: os.Getenv("MY_REGION"),
	}
	si.ReportProgress(clients.TranscodeStatusPreparing, 0)

	c.runHandlerAsync(si, func() (*HandlerOutput, error) {
		sourceURL, err := url.Parse(si.SourceFile)
		if err != nil {
			return nil, fmt.Errorf("error parsing source as url: %w", err)
		}

		inputVideoProbe, signedNewSourceURL, newSourceURL, err := c.InputCopy.CopyInputToS3(si.RequestID, sourceURL)
		if err != nil {
			return nil, fmt.Errorf("error copying input to storage: %w", err)
		}
		p.SourceFile = newSourceURL.String()   // OS URL used by mist
		p.SignedSourceURL = signedNewSourceURL // http(s) URL used by mediaconvert
		p.InputFileInfo = inputVideoProbe
		p.GenerateMP4 = func(mp4TargetUrl *url.URL, mp4OnlyShort bool, duration float64) bool {
			if mp4TargetUrl != nil && (!mp4OnlyShort || duration <= maxMP4OutDuration.Seconds()) {
				return true
			}
			return false
		}(p.Mp4TargetURL, p.Mp4OnlyShort, p.InputFileInfo.Duration)

		log.AddContext(si.RequestID, "new_source_url", newSourceURL)
		log.AddContext(si.RequestID, "signed_url", signedNewSourceURL)

		c.startUploadJob(p)
		return nil, nil
	})
}
func (c *Coordinator) startUploadJob(p UploadJobPayload) {
	strategy := c.strategy
	if p.PipelineStrategy.IsValid() {
		strategy = p.PipelineStrategy
	}
	p.MistSupported, strategy = checkMistCompatible(p.RequestID, strategy, p.InputFileInfo)
	log.AddContext(p.RequestID, "strategy", strategy)
	log.Log(p.RequestID, "Starting upload job")

	switch strategy {
	case StrategyCatalystDominance:
		c.startOneUploadJob(p, c.pipeMist, true, false)
	case StrategyExternalDominance:
		c.startOneUploadJob(p, c.pipeExternal, true, false)
	case StrategyCatalystFfmpegDominance:
		c.startOneUploadJob(p, c.pipeFfmpeg, true, false)
	case StrategyBackgroundExternal:
		c.startOneUploadJob(p, c.pipeMist, true, false)
		c.startOneUploadJob(p, c.pipeExternal, false, false)
	case StrategyBackgroundMist:
		c.startOneUploadJob(p, c.pipeExternal, true, false)
		c.startOneUploadJob(p, c.pipeMist, false, false)
	case StrategyFallbackExternal:
		// nolint:errcheck
		go recovered(func() (t bool, e error) {
			success := <-c.startOneUploadJob(p, c.pipeFfmpeg, true, true)
			if !success {
				p.InFallbackMode = true
				log.Log(p.RequestID, "Entering fallback pipeline")
				c.startOneUploadJob(p, c.pipeExternal, true, false)
			}
			return
		})
	}
}

// checkMistCompatible checks if the input codecs are compatible with mist and overrides the pipeline strategy
// to external if they are incompatible
func checkMistCompatible(requestID string, strategy Strategy, iv video.InputVideo) (bool, Strategy) {
	// Mist currently struggles with large files, so we don't send it any files bigger than MAX_MIST_INPUT_SIZE_BYTES
	if iv.SizeBytes > config.MAX_MIST_INPUT_SIZE_BYTES {
		return mistNotSupported(strategy)
	}

	for _, track := range iv.Tracks {
		// if the codecs are not compatible then override to external pipeline to avoid sending to mist
		if (track.Type == video.TrackTypeVideo && strings.ToLower(track.Codec) != "h264") ||
			(track.Type == video.TrackTypeAudio && strings.ToLower(track.Codec) != "aac") {
			log.Log(requestID, "codec not supported by mist", "trackType", track.Type, "codec", track.Codec)
			return mistNotSupported(strategy)
		}
		if track.Type == video.TrackTypeVideo && track.Rotation != 0 {
			log.Log(requestID, "video rotation not supported by mist", "rotation", track.Rotation)
			return mistNotSupported(strategy)
		}
	}
	return true, strategy
}

func mistNotSupported(strategy Strategy) (bool, Strategy) {
	// allow StrategyCatalystDominance to pass through as this is used in tests and we might want to manually force it for debugging
	if strategy == StrategyCatalystDominance {
		return false, strategy
	}
	return false, StrategyExternalDominance
}

// Starts a single upload job with specified pipeline Handler. If the job is
// running in background (foreground=false) then:
//   - the job will have a different requestID
//   - no transcode status updates will be reported to the caller, only logged
//   - the output will go to a different location than the real job
//
// The `hasFallback` argument means the caller has a special logic to handle
// failures (today this means re-running the job in another pipeline). If it's
// set to true, error callbacks from this job will not be sent.
func (c *Coordinator) startOneUploadJob(p UploadJobPayload, handler Handler, foreground, hasFallback bool) <-chan bool {
	if !foreground {
		p.RequestID = fmt.Sprintf("bg_%s", p.RequestID)
		if p.HlsTargetURL != nil {
			p.HlsTargetURL = p.HlsTargetURL.JoinPath("..", handler.Name(), path.Base(p.HlsTargetURL.Path))
		}
		// this will prevent the callbacks for this job from actually being sent
		p.CallbackURL = ""
	}
	if p.InFallbackMode {
		p.RequestID = fmt.Sprintf("fb_%s", p.RequestID)
	}
	streamName := config.SegmentingStreamName(p.RequestID)
	log.AddContext(p.RequestID, "stream_name", streamName)
	log.AddContext(p.RequestID, "handler", handler.Name())

	var pipeline = "mist"
	if handler.Name() == "external" {
		pipeline = "aws-mediaconvert"
	}

	// Codecs are parsed here primarily to write codec stats for each job
	var videoCodec, audioCodec string
	videoTrack, err := p.InputFileInfo.GetTrack(video.TrackTypeVideo)
	if err != nil {
		videoCodec = "n/a"
	} else {
		videoCodec = videoTrack.Codec
	}
	audioTrack, err := p.InputFileInfo.GetTrack(video.TrackTypeAudio)
	if err != nil {
		audioCodec = "n/a"
	} else {
		audioCodec = audioTrack.Codec
	}

	si := &JobInfo{
		UploadJobPayload: p,
		StreamName:       streamName,
		handler:          handler,
		hasFallback:      hasFallback,
		statusClient:     c.statusClient,
		startTime:        time.Now(),
		result:           make(chan bool, 1),

		pipeline:              pipeline,
		numProfiles:           len(p.Profiles),
		state:                 "segmenting",
		transcodedSegments:    0,
		targetSegmentSizeSecs: p.TargetSegmentSizeSecs,
		catalystRegion:        os.Getenv("MY_REGION"),
		sourceCodecVideo:      videoCodec,
		sourceCodecAudio:      audioCodec,
		sourceWidth:           videoTrack.Width,
		sourceHeight:          videoTrack.Height,
		sourceFPS:             videoTrack.FPS,
		sourceBitrateVideo:    videoTrack.Bitrate,
		sourceBitrateAudio:    audioTrack.Bitrate,
		sourceChannels:        audioTrack.Channels,
		sourceSampleRate:      audioTrack.SampleRate,
		sourceSampleBits:      audioTrack.SampleBits,
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
		if err != nil || (out != nil && !out.Continue) {
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
		callbackURL := job.CallbackURL
		if job.hasFallback {
			// an empty url will skip actually sending the callback. we still want the log tho
			callbackURL = ""
		}
		tsm = clients.NewTranscodeStatusError(callbackURL, job.RequestID, err.Error(), errors.IsUnretriable(err))
		job.state = "failed"
	} else {
		tsm = clients.NewTranscodeStatusCompleted(job.CallbackURL, job.RequestID, out.Result.InputVideo, out.Result.Outputs)
		job.state = "completed"
	}
	job.statusClient.SendTranscodeStatus(tsm)

	// Automatically delete jobs after an error or result
	success := err == nil
	c.Jobs.Remove(job.StreamName)

	log.Log(job.RequestID, "Finished job and deleted from job cache", "success", success)

	var labels = []string{
		job.sourceCodecVideo,
		job.sourceCodecAudio,
		job.pipeline,
		job.catalystRegion,
		fmt.Sprint(job.numProfiles),
		job.state,
		config.Version,
		strconv.FormatBool(job.InFallbackMode),
		strconv.FormatBool(job.MistSupported),
	}

	metrics.Metrics.VODPipelineMetrics.Count.
		WithLabelValues(labels...).
		Inc()

	metrics.Metrics.VODPipelineMetrics.Duration.
		WithLabelValues(labels...).
		Observe(time.Since(job.startTime).Seconds())

	metrics.Metrics.VODPipelineMetrics.SourceSegments.
		WithLabelValues(labels...).
		Observe(float64(job.sourceSegments))

	metrics.Metrics.VODPipelineMetrics.SourceBytes.
		WithLabelValues(labels...).
		Observe(float64(job.sourceBytes))

	metrics.Metrics.VODPipelineMetrics.SourceDuration.
		WithLabelValues(labels...).
		Observe(float64(job.sourceDurationMs))

	metrics.Metrics.VODPipelineMetrics.TranscodedSegments.
		WithLabelValues(labels...).
		Add(float64(job.transcodedSegments))

	go c.sendDBMetrics(job, out)

	job.result <- success
}

func getProfileCount(out *HandlerOutput) int {
	if out == nil || out.Result == nil || len(out.Result.Outputs) < 1 {
		return 0
	}
	return len(out.Result.Outputs[0].Videos)
}

func (c *Coordinator) sendDBMetrics(job *JobInfo, out *HandlerOutput) {
	if c.MetricsDB == nil {
		return
	}

	targetURL := ""
	if job.HlsTargetURL != nil {
		targetURL = job.HlsTargetURL.Redacted()
	}
	insertDynStmt := `insert into "vod_completed"(
                            "finished_at",
                            "started_at",
                            "request_id",
                            "external_id",
                            "source_codec_video",
                            "source_codec_audio",
                            "pipeline",
                            "catalyst_region",
                            "state",
                            "profiles_count",
                            "job_duration",
                            "source_segment_count",
                            "transcoded_segment_count",
                            "source_bytes_count",
                            "source_duration",
                            "source_url",
                            "target_url",
                            "in_fallback_mode"
                            ) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`
	_, err := c.MetricsDB.Exec(
		insertDynStmt,
		time.Now().Unix(),
		job.startTime.Unix(),
		job.RequestID,
		job.ExternalID,
		job.sourceCodecVideo,
		job.sourceCodecAudio,
		job.pipeline,
		job.catalystRegion,
		job.state,
		getProfileCount(out),
		time.Since(job.startTime).Milliseconds(),
		job.sourceSegments,
		job.transcodedSegments,
		job.sourceBytes,
		job.sourceDurationMs,
		log.RedactURL(job.SourceFile),
		targetURL,
		job.InFallbackMode,
	)
	if err != nil {
		log.LogError(job.RequestID, "error writing postgres metrics", err)
		return
	}
}

func recovered[T any](f func() (T, error)) (t T, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			log.LogNoRequestID("panic in pipeline handler background goroutine, recovering", "err", err, "trace", debug.Stack())
			err = fmt.Errorf("panic in pipeline handler: %v", rec)
		}
	}()
	return f()
}
