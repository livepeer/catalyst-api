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
	Version                     *prometheus.CounterVec
	UploadVODRequestCount       prometheus.Counter
	UploadVODRequestDurationSec *prometheus.SummaryVec
	TranscodeSegmentDurationSec prometheus.Histogram
	PlaybackRequestDurationSec  *prometheus.SummaryVec

	TranscodingStatusUpdate ClientMetrics
	BroadcasterClient       ClientMetrics
	MistClient              ClientMetrics
	ObjectStoreClient       ClientMetrics

	VODPipelineMetrics VODPipelineMetrics
}

var vodLabels = []string{"source_codec_video", "source_codec_audio", "pipeline", "catalyst_region", "num_profiles", "stage", "version", "is_fallback_mode", "is_livepeer_supported"}

func NewMetrics() *CatalystAPIMetrics {
	m := &CatalystAPIMetrics{
		// Fired once on startup to let us check which version of this service we're running
		Version: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "version",
			Help: "Current Git SHA / Tag that's running. Incremented once on app startup.",
		}, []string{"app", "version"}),

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
			}, []string{"host", "operation"}),
			FailureCount: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "object_store_failure_count",
				Help: "The total number of failed transcoding status updates",
			}, []string{"host", "operation"}),
			RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "object_store_request_duration",
				Help:    "Time taken to send transcoding status updates",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}, []string{"host", "operation"}),
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

var Metrics = NewMetrics()
