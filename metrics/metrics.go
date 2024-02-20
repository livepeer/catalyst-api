package metrics

import (
	"github.com/livepeer/catalyst-api/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ClientMetrics struct {
	RetryCount      *prometheus.GaugeVec
	FailureCount    *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
}

type VODPipelineMetrics struct {
	Count              *prometheus.CounterVec
	Duration           *prometheus.SummaryVec
	SourceSegments     *prometheus.SummaryVec
	TranscodedSegments *prometheus.CounterVec
	SourceBytes        *prometheus.SummaryVec
	SourceDuration     *prometheus.SummaryVec
}

type CatalystAPIMetrics struct {
	Version                         *prometheus.CounterVec
	UploadVODRequestCount           prometheus.Counter
	UploadVODRequestDurationSec     *prometheus.SummaryVec
	TranscodeSegmentDurationSec     prometheus.Histogram
	PlaybackRequestDurationSec      *prometheus.SummaryVec
	CDNRedirectCount                *prometheus.CounterVec
	CDNRedirectWebRTC406            *prometheus.CounterVec
	UserEventBufferSize             prometheus.Gauge
	MemberEventBufferSize           prometheus.Gauge
	SerfEventBufferSize             prometheus.Gauge
	AccessControlRequestCount       *prometheus.CounterVec
	AccessControlRequestDurationSec *prometheus.SummaryVec
	ImageAPIDurationSec             *prometheus.SummaryVec
	ImageAPIDownloadDurationSec     *prometheus.SummaryVec
	ImageAPIExtractDurationSec      *prometheus.SummaryVec

	JobsInFlight         prometheus.Gauge
	HTTPRequestsInFlight prometheus.Gauge

	TranscodingStatusUpdate ClientMetrics
	BroadcasterClient       ClientMetrics
	MistClient              ClientMetrics
	ObjectStoreClient       ClientMetrics

	VODPipelineMetrics VODPipelineMetrics
}

var vodLabels = []string{"source_codec_video", "source_codec_audio", "pipeline", "catalyst_region", "num_profiles", "stage", "version", "is_fallback_mode", "is_livepeer_supported", "is_clip", "is_thumbs"}

func NewMetrics() *CatalystAPIMetrics {
	m := &CatalystAPIMetrics{
		// Fired once on startup to let us check which version of this service we're running
		Version: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "version",
			Help: "Current Git SHA / Tag that's running. Incremented once on app startup.",
		}, []string{"app", "version"}),

		// Node-level Capacity Metrics
		JobsInFlight: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "jobs_in_flight",
			Help: "A count of the jobs in flight",
		}),
		HTTPRequestsInFlight: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "A count of the http requests in flight",
		}),
		UserEventBufferSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "user_event_buffer_size",
			Help: "A count of the user events currently held in the buffer",
		}),
		MemberEventBufferSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "member_event_buffer_size",
			Help: "A count of the member events currently held in the buffer",
		}),
		SerfEventBufferSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "serf_event_buffer_size",
			Help: "A count of the serf events currently held in the buffer",
		}),

		// /api/vod request metrics
		UploadVODRequestCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: "upload_vod_request_count",
			Help: "The total number of requests to /api/vod",
		}),
		UploadVODRequestDurationSec: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "upload_vod_request_duration_seconds",
			Help: "The latency of the requests made to /api/vod in seconds broken up by success and status code",
		}, []string{"success", "status_code", "version"}),
		TranscodeSegmentDurationSec: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "transcode_segment_duration_seconds",
			Help:    "Time taken to transcode a segment",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		PlaybackRequestDurationSec: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "catalyst_playback_request_duration_seconds",
			Help: "The latency of the requests made to /asset/hls in seconds broken up by success and status code",
		}, []string{"success", "status_code", "version"}),
		CDNRedirectCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "cdn_redirect_count",
			Help: "Number of requests redirected to CDN",
		}, []string{"playbackID"}),
		CDNRedirectWebRTC406: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "cdn_redirect_webrtc_406",
			Help: "Number of WebRTC requests rejected with HTTP 406 because of playback should be seved from external CDN",
		}, []string{"playbackID"}),
		AccessControlRequestCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "access_control_request_count",
			Help: "The total number of access control requests",
		}, []string{"allowed", "playbackID"}),
		AccessControlRequestDurationSec: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "access_control_request_duration_seconds",
			Help: "The latency of the access control requests",
		}, []string{"allowed", "playbackID"}),
		ImageAPIDurationSec:         durationSummary("image_api_response_duration", "Total time taken to process Image API request", "success", "status_code", "version"),
		ImageAPIDownloadDurationSec: durationSummary("image_api_download_duration", "Time taken to download media from storage while generating an image"),
		ImageAPIExtractDurationSec:  durationSummary("image_api_extract_duration", "Time taken to generate image"),

		// Clients metrics
		TranscodingStatusUpdate: ClientMetrics{
			RetryCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "transcoding_status_update_retry_count",
				Help: "The number of retries of a successful request to Studio",
			}, []string{"host"}),
			FailureCount: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "transcoding_status_update_failure_count",
				Help: "The total number of failed transcoding status updates",
			}, []string{"host", "status_code"}),
			RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "transcoding_status_update_duration",
				Help:    "Time taken to send transcoding status updates",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}, []string{"host"}),
		},

		BroadcasterClient: ClientMetrics{
			RetryCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "broadcaster_client_retry_count",
				Help: "The number of retries of a successful request to Studio",
			}, []string{"host"}),
			FailureCount: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "broadcaster_client_failure_count",
				Help: "The total number of failed transcoding status updates",
			}, []string{"host", "status_code"}),
			RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "broadcaster_client_request_duration",
				Help:    "Time taken to send transcoding status updates",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}, []string{"host"}),
		},

		MistClient: ClientMetrics{
			RetryCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "mist_client_retry_count",
				Help: "The number of retries of a successful request to Studio",
			}, []string{"host"}),
			FailureCount: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "mist_client_failure_count",
				Help: "The total number of failed transcoding status updates",
			}, []string{"host", "status_code"}),
			RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "mist_client_request_duration",
				Help:    "Time taken to send transcoding status updates",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}, []string{"host"}),
		},

		ObjectStoreClient: ClientMetrics{
			RetryCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "object_store_retry_count",
				Help: "The number of retries of a successful request to Studio",
			}, []string{"host", "operation", "bucket"}),
			FailureCount: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "object_store_failure_count",
				Help: "The total number of failed transcoding status updates",
			}, []string{"host", "operation", "bucket"}),
			RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "object_store_request_duration",
				Help:    "Time taken to send transcoding status updates",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}, []string{"host", "operation", "bucket"}),
		},

		VODPipelineMetrics: VODPipelineMetrics{
			Count: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "vod_count",
				Help: "Number of VOD pipeline started",
			}, vodLabels),
			Duration: promauto.NewSummaryVec(prometheus.SummaryOpts{
				Name: "vod_duration",
				Help: "Time taken till the VOD job is completed",
			}, vodLabels),
			SourceSegments: promauto.NewSummaryVec(prometheus.SummaryOpts{
				Name: "vod_source_segments",
				Help: "Number of segments of the source asset",
			}, vodLabels),
			TranscodedSegments: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "vod_transcoded_segments",
				Help: "Number of segments of rendition asset",
			}, vodLabels),
			SourceBytes: promauto.NewSummaryVec(prometheus.SummaryOpts{
				Name: "vod_source_bytes",
				Help: "Size of the source asset",
			}, vodLabels),
			SourceDuration: promauto.NewSummaryVec(prometheus.SummaryOpts{
				Name: "vod_source_duration",
				Help: "Duration of the source asset",
			}, vodLabels),
		},
	}

	// Fire a metric a single time to let us track the version of the app we're using
	m.Version.WithLabelValues("catalyst-api", config.Version).Inc()

	return m
}

func durationSummary(name, help string, labelNames ...string) *prometheus.SummaryVec {
	return promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name:       name,
		Help:       help,
		Objectives: map[float64]float64{0.5: 0.05, 0.8: 0.01, 0.95: 0.01},
	}, labelNames)
}

var Metrics = NewMetrics()
