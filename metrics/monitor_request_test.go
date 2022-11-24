package metrics

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

var m = ClientMetrics{
	RetryCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_retry_count",
	}, []string{"host"}),
	FailureCount: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "test_failures_count",
	}, []string{"host", "status_code"}),
	RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "test_request_duration",
		Buckets: []float64{.5, 1},
	}, []string{"host"}),
}

func TestRetryableClientMonitoring(t *testing.T) {
	var retries int = 0
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if retries < 2 {
			retries++
			w.WriteHeader(http.StatusBadGateway)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		_, _ = w.Write([]byte{})
	}))
	defer svr.Close()
	url, err := url.Parse(svr.URL)
	require.NoError(t, err)

	metricsServer := httptest.NewServer(promhttp.Handler())
	defer metricsServer.Close()

	req, err := http.NewRequest(http.MethodGet, svr.URL, bytes.NewBuffer([]byte{}))
	require.NoError(t, err)

	client := retryablehttp.NewClient()
	client.RetryMax = 3
	client.CheckRetry = HttpRetryHook
	_, _ = MonitorRequest(m, client.StandardClient(), req)

	res, err := http.Get(metricsServer.URL)
	require.NoError(t, err)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	require.Regexp(t, fmt.Sprintf(`\ntest_retry_count{host="%s"} 2\n`, url.Host), string(body))
	require.Regexp(t, fmt.Sprintf(`\ntest_request_duration_bucket{host="%s",le="0.5"} \d+\n`, url.Host), string(body))
	require.Regexp(t, fmt.Sprintf(`\ntest_request_duration_bucket{host="%s",le="1"} \d+\n`, url.Host), string(body))
	require.NotRegexp(t, `test_failures_count`, body)
}

func TestRetryableClientFailingRequestMonitoring(t *testing.T) {
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte{})
	}))
	defer svr.Close()
	url, err := url.Parse(svr.URL)
	require.NoError(t, err)

	metricsServer := httptest.NewServer(promhttp.Handler())
	defer metricsServer.Close()

	req, err := http.NewRequest(http.MethodGet, svr.URL, bytes.NewBuffer([]byte{}))
	require.NoError(t, err)

	client := retryablehttp.NewClient()
	client.RetryMax = 3
	client.CheckRetry = HttpRetryHook
	_, _ = MonitorRequest(m, client.StandardClient(), req)

	res, err := http.Get(metricsServer.URL)
	require.NoError(t, err)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	require.Regexp(t, fmt.Sprintf(`\ntest_failures_count{host="%s",status_code="502"} 1\n`, url.Host), string(body))
	require.NotRegexp(t, `test_retry_count`, body)
	require.NotRegexp(t, `test_request_duration`, body)
}
