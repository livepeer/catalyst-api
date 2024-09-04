package metrics

import (
	"context"
	"fmt"
	"github.com/golang/glog"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

type Retries struct {
	count          int
	lastStatusCode int
}

func MonitorRequest(clientMetrics ClientMetrics, client *http.Client, r *http.Request) (*http.Response, error) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, RetriesKey, &Retries{-1, 0})
	req := r.WithContext(ctx)

	start := time.Now()
	glog.Infof("client.Do(req)")
	res, err := client.Do(req)
	glog.Infof("Done client.Do(req), err=%v", err)
	duration := time.Since(start)

	retries := ctx.Value(RetriesKey).(*Retries)
	if retries.lastStatusCode >= 400 {
		glog.Infof("retries.lastStatusCode >= 400")
		clientMetrics.FailureCount.WithLabelValues(req.URL.Host, fmt.Sprint(retries.lastStatusCode)).Inc()
		return res, err
	}

	clientMetrics.RequestDuration.WithLabelValues(req.URL.Host).Observe(duration.Seconds())
	clientMetrics.RetryCount.WithLabelValues(req.URL.Host).Set(float64(retries.count))

	glog.Infof("return res, err")
	return res, err
}

func HttpRetryHook(ctx context.Context, res *http.Response, err error) (bool, error) {
	glog.Infof("HttpRetryHook")
	retries := ctx.Value(RetriesKey).(*Retries)
	if res == nil {
		// TODO: have a better way to represent closed/refused connections and timeouts
		retries.lastStatusCode = 999
	} else if res.StatusCode >= 400 {
		retries.lastStatusCode = res.StatusCode
	} else {
		retries.lastStatusCode = res.StatusCode
	}
	retries.count++

	glog.Infof("Done HttpRetryHook")
	return retryablehttp.DefaultRetryPolicy(ctx, res, err)
}
