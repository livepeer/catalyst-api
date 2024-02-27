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
	Start(ch chan AnalyticsData)
}

type LogProcessor struct {
	logs    map[labelsKey]map[string]metricValue
	promURL string
	host    string
}

type metricValue struct {
}

type labelsKey struct {
	playbackID string
	browser    string
	deviceType string
	country    string
	userID     string
}

type AnalyticsData struct {
	SessionID  string
	PlaybackID string
	Browser    string
	DeviceType string
	Country    string
	UserID     string
}

func NewLogProcessor(promURL string, host string) LogProcessor {
	return LogProcessor{
		logs:    make(map[labelsKey]map[string]metricValue),
		promURL: promURL,
		host:    host,
	}
}

func (lp *LogProcessor) Start(ch chan AnalyticsData) {
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

func (p *LogProcessor) processLog(d AnalyticsData) {
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
	bySessionID[d.SessionID] = metricValue{}
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
		metrics.WriteString(p.toMetric(k, v, now))
	}

	// send data
	err := p.sendMetricsString(metrics.String())
	if err != nil {
		glog.Errorf("failed to send analytics logs, err=%w", err)
	}

	// clear map
	p.logs = make(map[labelsKey]map[string]metricValue)
}

func (p *LogProcessor) toMetric(k labelsKey, v map[string]metricValue, nowMs int64) string {
	return fmt.Sprintln(fmt.Sprintf(`viewcount{host="%s",user_id="%s",playback_id="%s",device_type="%s",browser="%s",country="%s"} %d %d`,
		p.host,
		k.userID,
		k.playbackID,
		k.deviceType,
		k.browser,
		k.country,
		len(v),
		nowMs,
	))
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
