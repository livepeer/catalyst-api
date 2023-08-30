package pipeline

import (
	"crypto/rsa"
	"database/sql"
	"fmt"
	"math"
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
	"github.com/livepeer/catalyst-api/crypto"
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
	// Only execute the external pipeline
	StrategyExternalDominance Strategy = "external"
	// Only execute the FFMPEG / Livepeer pipeline
	StrategyCatalystFfmpegDominance Strategy = "catalyst_ffmpeg"
	// Execute the FFMPEG / Livepeer pipeline in foreground and the external transcoder in background.
	StrategyBackgroundExternal Strategy = "background_external"
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
	case StrategyExternalDominance, StrategyCatalystFfmpegDominance, StrategyBackgroundExternal, StrategyFallbackExternal:
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
	FragMp4TargetURL      *url.URL
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
	Encryption            *EncryptionPayload
	InputFileInfo         video.InputVideo
	SignedSourceURL       string
	InFallbackMode        bool
	LivepeerSupported     bool
	SourceCopy            bool
}

type EncryptionPayload struct {
	EncryptedKey string `json:"encrypted_key"`
}

// UploadJobResult is the object returned by the successful execution of an
// upload job.
type UploadJobResult struct {
	InputVideo video.InputVideo
	Outputs    []video.OutputVideo
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

	SourcePlaybackDone time.Time
	DownloadDone       time.Time
	SegmentingDone     time.Time
	TranscodingDone    time.Time

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
	// Ignore errors, send the progress next time
	_ = j.statusClient.SendTranscodeStatus(tsm)
}

// Coordinator provides the main interface to handle the pipelines. It should be
// called directly from the API handlers and never blocks on execution, but
// rather schedules routines to do the actual work in background.
type Coordinator struct {
	strategy     Strategy
	statusClient clients.TranscodeStatusClient

	pipeFfmpeg, pipeExternal Handler

	Jobs                 *cache.Cache[*JobInfo]
	MetricsDB            *sql.DB
	InputCopy            clients.InputCopier
	VodDecryptPrivateKey *rsa.PrivateKey
	SourceOutputURL      *url.URL
}

func NewCoordinator(strategy Strategy, sourceOutputURL, extTranscoderURL string, statusClient clients.TranscodeStatusClient, metricsDB *sql.DB, VodDecryptPrivateKey *rsa.PrivateKey, broadcasterURL string, sourcePlaybackHosts map[string]string) (*Coordinator, error) {
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
	if strategy != StrategyCatalystFfmpegDominance && extTranscoder == nil {
		return nil, fmt.Errorf("external transcoder is required for strategy: %v", strategy)
	}
	sourceOutput, err := url.Parse(sourceOutputURL)
	if err != nil {
		return nil, fmt.Errorf("cannot create sourceOutputUrl: %w", err)
	}
	broadcaster, err := clients.NewLocalBroadcasterClient(broadcasterURL)
	if err != nil {
		return nil, fmt.Errorf("cannot initalilze local broadcaster: %w", err)
	}

	return &Coordinator{
		strategy:     strategy,
		statusClient: statusClient,
		pipeFfmpeg: &ffmpeg{
			SourceOutputUrl:     sourceOutputURL,
			Broadcaster:         broadcaster,
			probe:               video.Probe{},
			sourcePlaybackHosts: sourcePlaybackHosts,
		},
		pipeExternal:         &external{extTranscoder},
		Jobs:                 cache.New[*JobInfo](),
		MetricsDB:            metricsDB,
		InputCopy:            clients.NewInputCopy(),
		VodDecryptPrivateKey: VodDecryptPrivateKey,
		SourceOutputURL:      sourceOutput,
	}, nil
}

func NewStubCoordinator() *Coordinator {
	return NewStubCoordinatorOpts(StrategyCatalystFfmpegDominance, nil, nil, nil, "")
}

func NewStubCoordinatorOpts(strategy Strategy, statusClient clients.TranscodeStatusClient, pipeFfmpeg, pipeExternal Handler, sourceOutputUrl string) *Coordinator {
	if strategy == "" {
		strategy = StrategyCatalystFfmpegDominance
	}
	if statusClient == nil {
		statusClient = clients.TranscodeStatusFunc(func(tsm clients.TranscodeStatusMessage) error { return nil })
	}
	if pipeFfmpeg == nil {
		pipeFfmpeg = &ffmpeg{SourceOutputUrl: sourceOutputUrl, probe: video.Probe{}}
	}
	if pipeExternal == nil {
		pipeExternal = &external{}
	}
	return &Coordinator{
		strategy:     strategy,
		statusClient: statusClient,
		pipeFfmpeg:   pipeFfmpeg,
		pipeExternal: pipeExternal,
		Jobs:         cache.New[*JobInfo](),
		InputCopy: &clients.InputCopy{
			Probe: video.Probe{},
		},
		SourceOutputURL: &url.URL{},
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
		sourceURL, err := url.Parse(p.SourceFile)
		if err != nil {
			return nil, fmt.Errorf("error parsing source as url: %w", err)
		}

		var decryptor *crypto.DecryptionKeys

		if p.Encryption != nil {
			decryptor = &crypto.DecryptionKeys{
				DecryptKey:   c.VodDecryptPrivateKey,
				EncryptedKey: p.Encryption.EncryptedKey,
			}
		}

		osTransferURL := c.SourceOutputURL.JoinPath(p.RequestID, "transfer", path.Base(sourceURL.Path))
		if clients.IsHLSInput(sourceURL) {
			osTransferURL = sourceURL
		} else if p.SourceCopy {
			log.Log(p.RequestID, "source copy enabled")
			osTransferURL = p.HlsTargetURL.JoinPath("video")
		}

		inputVideoProbe, signedNewSourceURL, err := c.InputCopy.CopyInputToS3(p.RequestID, sourceURL, osTransferURL, decryptor)
		if err != nil {
			return nil, fmt.Errorf("error copying input to storage: %w", err)
		}

		p.SourceFile = osTransferURL.String()  // OS URL used by mist
		p.SignedSourceURL = signedNewSourceURL // http(s) URL used by mediaconvert
		p.InputFileInfo = inputVideoProbe
		p.GenerateMP4 = ShouldGenerateMP4(sourceURL, p.Mp4TargetURL, p.FragMp4TargetURL, p.Mp4OnlyShort, p.InputFileInfo.Duration)

		log.AddContext(p.RequestID, "new_source_url", p.SourceFile)
		log.AddContext(p.RequestID, "signed_url", p.SignedSourceURL)

		c.startUploadJob(p)
		return nil, nil
	})
}

func ShouldGenerateMP4(sourceURL, mp4TargetUrl *url.URL, fragMp4TargetUrl *url.URL, mp4OnlyShort bool, durationSecs float64) bool {
	// We're currently memory-bound for generating MP4s above a certain file size
	// This has been hitting us for long recordings, so do a crude "is it longer than 3 hours?" check and skip the MP4 if it is
	const maxRecordingMP4Duration = 12 * time.Hour
	if clients.IsHLSInput(sourceURL) && durationSecs > maxRecordingMP4Duration.Seconds() {
		return false
	}

	if mp4TargetUrl != nil && (!mp4OnlyShort || durationSecs <= maxMP4OutDuration.Seconds()) {
		return true
	}

	if fragMp4TargetUrl != nil {
		return true
	}

	return false
}

func (c *Coordinator) startUploadJob(p UploadJobPayload) {
	strategy := c.strategy
	if p.PipelineStrategy.IsValid() {
		strategy = p.PipelineStrategy
	}
	p.LivepeerSupported, strategy = checkLivepeerCompatible(p.RequestID, strategy, p.InputFileInfo)
	log.AddContext(p.RequestID, "strategy", strategy)
	log.Log(p.RequestID, "Starting upload job")

	switch strategy {
	case StrategyExternalDominance:
		c.startOneUploadJob(p, c.pipeExternal, true, false)
	case StrategyCatalystFfmpegDominance:
		c.startOneUploadJob(p, c.pipeFfmpeg, true, false)
	case StrategyBackgroundExternal:
		c.startOneUploadJob(p, c.pipeFfmpeg, true, false)
		c.startOneUploadJob(p, c.pipeExternal, false, false)
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

// checkLivepeerCompatible checks if the input codecs are compatible with our Livepeer pipeline and overrides the pipeline strategy
// to external if they are incompatible
func checkLivepeerCompatible(requestID string, strategy Strategy, iv video.InputVideo) (bool, Strategy) {
	for _, track := range iv.Tracks {
		// if the codecs are not compatible then override to external pipeline to avoid sending to Livepeer
		if (track.Type == video.TrackTypeVideo && strings.ToLower(track.Codec) != "h264") ||
			(track.Type == video.TrackTypeAudio && strings.ToLower(track.Codec) != "aac") {
			log.Log(requestID, "codec not supported by Livepeer pipeline", "trackType", track.Type, "codec", track.Codec)
			return livepeerNotSupported(strategy)
		}
		if track.Type == video.TrackTypeVideo && track.Rotation != 0 {
			log.Log(requestID, "video rotation not supported by Livepeer pipeline", "rotation", track.Rotation)
			return livepeerNotSupported(strategy)
		}
		if !checkDisplayAspectRatio(track, requestID) {
			return livepeerNotSupported(strategy)
		}
	}
	return true, strategy
}

func livepeerNotSupported(strategy Strategy) (bool, Strategy) {
	// Allow "dominance" strategies to pass through as these are used in tests and we might want to manually force them for debugging
	if strategy == StrategyCatalystFfmpegDominance {
		return false, strategy
	}
	return false, StrategyExternalDominance
}

func checkDisplayAspectRatio(track video.InputTrack, requestID string) bool {
	if track.Type != video.TrackTypeVideo || track.DisplayAspectRatio == "" {
		return true
	}
	aspectRatioParts := strings.Split(track.DisplayAspectRatio, ":")
	if len(aspectRatioParts) != 2 {
		return true
	}
	w, err := strconv.ParseFloat(aspectRatioParts[0], 64)
	h, err2 := strconv.ParseFloat(aspectRatioParts[1], 64)
	if err != nil || err2 != nil {
		log.Log(requestID, "failed to parse floats when checking display aspect ratio", "err", err, "err2", err2)
		return true
	}
	if h == 0 || track.Height == 0 {
		log.Log(requestID, "height was zero when checking display aspect ratio", "ratioHeight", h, "trackHeight", track.Height)
		return true
	}
	dar := w / h
	resRatio := float64(track.Width) / float64(track.Height)

	// calculate the difference between the aspect ratio and the real ratio of the resolution, allow up to 20%
	diff := math.Abs(dar - resRatio)
	if (diff / dar) < 0.2 {
		return true
	}
	log.Log(requestID, "display aspect ratio doesn't match resolution, not supported by Livepeer pipeline", "display_aspect_ratio", track.DisplayAspectRatio, "width", track.Width, "height", track.Height)
	return false
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
	streamName := config.SegmentingStreamName(p.RequestID)
	log.AddContext(p.RequestID, "stream_name", streamName)
	log.AddContext(p.RequestID, "handler", handler.Name())

	var pipeline = handler.Name()
	if pipeline == "external" {
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
		sourceBytes:           p.InputFileInfo.SizeBytes,
		sourceDurationMs:      int64(math.Round(p.InputFileInfo.Duration) * 1000),
		DownloadDone:          time.Now(),
	}
	si.ReportProgress(clients.TranscodeStatusPreparing, 0)

	c.Jobs.Store(streamName, si)
	log.Log(si.RequestID, "Wrote to jobs cache")

	c.runHandlerAsync(si, func() (*HandlerOutput, error) {
		return si.handler.HandleStartUploadJob(si)
	})
	return si.result
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
	err2 := job.statusClient.SendTranscodeStatus(tsm)
	if err2 != nil {
		log.LogError(tsm.RequestID, "failed sending finalize callback, job state set to 'failed'", err2)
		job.state = "failed"
	}

	// Automatically delete jobs after an error or result
	success := err == nil && err2 == nil
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
		strconv.FormatBool(job.LivepeerSupported),
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

	// If it's a fallback, we want a unique Request ID so that it doesn't clash with the row that's already been created for the first pipeline
	metricsRequestID := job.RequestID
	if job.InFallbackMode {
		metricsRequestID = "fb_" + metricsRequestID
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
                            "in_fallback_mode",
                            "source_playback_at",
                            "download_done_at",
                            "segmenting_done_at",
                            "transcoding_done_at"
                            ) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`
	_, err := c.MetricsDB.Exec(
		insertDynStmt,
		time.Now().Unix(),
		job.startTime.Unix(),
		metricsRequestID,
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
		job.SourcePlaybackDone.Unix(),
		job.DownloadDone.Unix(),
		job.SegmentingDone.Unix(),
		job.TranscodingDone.Unix(),
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
