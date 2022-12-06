package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ClientMetrics struct {
	RetryCount      *prometheus.GaugeVec
	FailureCount    *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
}

type CatalystAPIMetrics struct {
	UploadVODRequestCount        prometheus.Counter
	UploadVODRequestDurationSec  *prometheus.SummaryVec
	UploadVODPipelineDurationSec *prometheus.SummaryVec
	TranscodeSegmentDurationSec  prometheus.Histogram

	TranscodingStatusUpdate ClientMetrics
	BroadcasterClient       ClientMetrics
	MistClient              ClientMetrics
	ObjectStoreClient       ClientMetrics
}

func NewMetrics() *CatalystAPIMetrics {
	m := &CatalystAPIMetrics{
		// /api/vod request metrics
		UploadVODRequestCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: "upload_vod_request_count",
			Help: "The total number of requests to /api/vod",
		}),
		UploadVODRequestDurationSec: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "upload_vod_request_duration_seconds",
			Help: "The latency of the requests made to /api/vod in seconds broken up by success and status code",
		}, []string{"success", "status_code"}),
		UploadVODPipelineDurationSec: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "upload_vod_pipeline_duration_seconds",
			Help: "The time that the VOD pipelines take to run, broken up by handler and success",
		}, []string{"handler", "success"}),
		TranscodeSegmentDurationSec: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "transcode_segment_duration_seconds",
			Help:    "Time taken to transcode a segment",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),

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
	}

	return m
}

var Metrics = NewMetrics()
