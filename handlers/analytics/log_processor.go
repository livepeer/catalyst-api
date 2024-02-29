package analytics

import (
	"bytes"
	"fmt"
	"github.com/golang/glog"
	"net/http"
	"strings"
	"time"
)

const (
	SendMetricsInterval = 10 * time.Second
	SendMetricsTimeout  = 60 * time.Second
)

type ILogProcessor interface {
	Start(ch chan LogData)
}

type LogProcessor struct {
	logs    map[labelsKey]map[string]metricValue
	promURL string
	host    string
}

type metricValue struct {
	count           int
	sumRebufferRate float64
	sumErrorRatio   float64
}

type labelsKey struct {
	playbackID string
	browser    string
	deviceType string
	country    string
	userID     string
}

type LogData struct {
	SessionID  string
	PlaybackID string
	Browser    string
	DeviceType string
	Country    string
	UserID     string

	PlaytimeMs int
	BufferMs   int
	Errors     int
}

func NewLogProcessor(promURL string, host string) *LogProcessor {
	return &LogProcessor{
		logs:    make(map[labelsKey]map[string]metricValue),
		promURL: promURL,
		host:    host,
	}
}

// Start starts LogProcessor which does the following:
// - on every analytics heartbeat event, process log which means updating the internal structure with the log data
// - every SendMetricsInterval, send metrics to the Prometheus (Victoria Metrics) DB
// Note that it sends the metrics in the plaintext format, this could be changed to sending data in the binary format,
// but plaintext seems to be efficient enough.
func (lp *LogProcessor) Start(ch chan LogData) {
	t := time.NewTicker(SendMetricsInterval)

	go func() {
		for {
			select {
			case d := <-ch:
				lp.processLog(d)
			case <-t.C:
				lp.sendMetrics()
			}
		}
	}()
}

func (p *LogProcessor) processLog(d LogData) {
	var k = labelsKey{
		playbackID: d.PlaybackID,
		browser:    d.Browser,
		deviceType: d.DeviceType,
		country:    d.Country,
		userID:     d.UserID,
	}

	bySessionID, ok := p.logs[k]
	if !ok {
		p.logs[k] = make(map[string]metricValue)
		bySessionID = p.logs[k]
	}

	totalMs := d.PlaytimeMs + d.BufferMs
	var rebufferRate float64
	if totalMs > 0 {
		rebufferRate = float64(d.BufferMs) / float64(totalMs)
	}
	var errorRatio float64
	if d.Errors > 0 {
		errorRatio = 1
	}
	mv := bySessionID[d.SessionID]
	bySessionID[d.SessionID] = metricValue{
		count:           mv.count + 1,
		sumRebufferRate: mv.sumRebufferRate + rebufferRate,
		sumErrorRatio:   mv.sumErrorRatio + errorRatio,
	}
}

func (p *LogProcessor) sendMetrics() {
	if len(p.logs) > 0 {
		glog.Info("sending analytics logs")
	} else {
		glog.V(6).Info("no analytics logs, skip sending")
		return
	}

	// convert values in the Prometheus format
	var metrics strings.Builder
	now := time.Now().UnixMilli()
	for k, v := range p.logs {
		metrics.WriteString(p.toViewCountMetric(k, v, now))
		metrics.WriteString(p.toRebufferRatioMetric(k, v, now))
		metrics.WriteString(p.toErrorRateMetric(k, v, now))
	}

	// send data
	err := p.sendMetricsString(metrics.String())
	if err != nil {
		glog.Errorf("failed to send analytics logs, err=%v", err)
	}

	// clear map
	p.logs = make(map[labelsKey]map[string]metricValue)
}

func (p *LogProcessor) toViewCountMetric(k labelsKey, v map[string]metricValue, nowMs int64) string {
	value := fmt.Sprintf("%d", len(v))
	return p.toMetric(k, "view_count", value, nowMs)
}

func (p *LogProcessor) toRebufferRatioMetric(k labelsKey, v map[string]metricValue, nowMs int64) string {
	var count int
	var sumRebufferRate float64
	for _, mv := range v {
		if mv.count > 0 {
			count += 1
			sumRebufferRate += mv.sumRebufferRate / float64(mv.count)
		}
	}
	var rebufferRate float64
	if count > 0 {
		rebufferRate = sumRebufferRate / float64(count)
	}
	value := fmt.Sprintf("%f", rebufferRate)
	return p.toMetric(k, "rebuffer_ratio", value, nowMs)
}

func (p *LogProcessor) toErrorRateMetric(k labelsKey, v map[string]metricValue, nowMs int64) string {
	var count int
	var sumErrorRatio float64
	for _, mv := range v {
		if mv.count > 0 {
			count += 1
			sumErrorRatio += mv.sumErrorRatio / float64(mv.count)
		}
	}
	var errorRatio float64
	if count > 0 {
		errorRatio = sumErrorRatio / float64(count)
	}
	value := fmt.Sprintf("%f", errorRatio)
	return p.toMetric(k, "error_rate", value, nowMs)
}

func (p *LogProcessor) toMetric(k labelsKey, name string, value string, nowMs int64) string {
	return fmt.Sprintf(`%s{host="%s",user_id="%s",playback_id="%s",device_type="%s",browser="%s",country="%s"} %s %d`+"\n",
		name,
		p.host,
		k.userID,
		k.playbackID,
		k.deviceType,
		k.browser,
		k.country,
		value,
		nowMs,
	)
}

func (p *LogProcessor) sendMetricsString(metrics string) error {
	client := &http.Client{Timeout: SendMetricsTimeout}
	req, err := http.NewRequest("POST", p.promURL, bytes.NewBuffer([]byte(metrics)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("non-OK status code: %d", resp.StatusCode)
	}
	return nil
}
