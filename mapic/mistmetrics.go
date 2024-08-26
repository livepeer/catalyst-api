package mistapiconnector

import (
	"fmt"
	"github.com/golang/glog"
	"io"
	"net/http"
	"strings"
)

func (mc *mac) MistMetricsHandler() http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, req *http.Request) {
			mistMetrics, err := mc.queryMistMetrics()
			if err != nil {
				http.Error(w, fmt.Sprintf("error fetching Mist prometheus metrics: %s", err), http.StatusInternalServerError)
				return
			}

			enrichedMistMetrics := mc.enrichMistMetrics(mistMetrics)
			_, err = w.Write([]byte(enrichedMistMetrics))
			if err != nil {
				http.Error(w, fmt.Sprintf("error writing enriched metrics: %s", err), http.StatusInternalServerError)
				return
			}
		})
}

func (mc *mac) queryMistMetrics() (string, error) {
	mistMetricsURL := fmt.Sprintf("http://%s:%d/%s", mc.config.MistHost, mc.config.MistPort, mc.config.MistPrometheus)
	resp, err := http.Get(mistMetricsURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// enrichMistMetrics adds additional labels to Mist metrics
func (mc *mac) enrichMistMetrics(metrics string) string {
	res := strings.Builder{}
	lines := strings.Split(metrics, "\n")
	for i, line := range lines {
		playbackID, ok := mc.parsePlaybackID(line)
		if ok {
			// Enrich labels for the lines that contains playbackID
			oldStr := mc.streamLabel(playbackID)
			newStr := mc.enrichLabels(playbackID)
			res.WriteString(strings.Replace(line, oldStr, newStr, -1))
		} else {
			// Do not enrich labels for lines that do not contain playbackID
			res.WriteString(line)
		}

		// Skip last end of line to preserve the same number of lines as Mist
		if i < len(lines)-1 {
			res.WriteString("\n")
		}
	}
	return res.String()
}

func (mc *mac) parsePlaybackID(line string) (string, bool) {
	match := mc.streamMetricsRe.FindStringSubmatch(line)
	if len(match) > 1 {
		return match[1], true
	}
	return "", false
}

func (mc *mac) enrichLabels(playbackID string) string {
	res := mc.streamLabel(playbackID)
	res += fmt.Sprintf(",catalyst=true")
	si, err := mc.getStreamInfo(playbackID)
	if err != nil {
		glog.Warning("could not enrich Mist metrics for stream=%s err=%v", playbackID, err)
	} else if si == nil || si.stream == nil {
		glog.Warning("could not enrich Mist metrics for stream=%s, cannot fetch stream info", playbackID)
	} else {
		res += fmt.Sprintf(`,user_id="%s"`, si.stream.UserID)
	}

	return res
}

func (mc *mac) streamLabel(playbackID string) string {
	return fmt.Sprintf(`stream="%s+%s"`, mc.baseStreamName, playbackID)
}
