package mistapiconnector

import (
	"context"
	"fmt"
	"github.com/golang/glog"
	"io"
	"net/http"
	"strings"
	"time"
)

const mistMetricsCallTimeeout = 10 * time.Second

func (mc *mac) MistMetricsHandler() http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, req *http.Request) {
			ctx, cancel := context.WithTimeout(req.Context(), mistMetricsCallTimeeout)
			defer cancel()
			mistMetrics, err := mc.queryMistMetrics(ctx)
			if err != nil {
				glog.Warningf("error fetching Mist prometheus metrics: %s", err)
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

func (mc *mac) queryMistMetrics(ctx context.Context) (string, error) {
	mistMetricsURL := fmt.Sprintf("http://%s:%d/%s", mc.config.MistHost, mc.config.MistPort, mc.config.MistPrometheus)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mistMetricsURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
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
		res.WriteString(mc.enrichLine(line))

		// Skip last end of line to preserve the same number of lines as Mist
		if i < len(lines)-1 {
			res.WriteString("\n")
		}
	}
	return res.String()
}

func (mc *mac) enrichLine(line string) string {
	res := mc.enrichPlaybackSpecificLabels(line)
	return mc.enrichConstLabels(res)
	return res
}

func (mc *mac) enrichPlaybackSpecificLabels(line string) string {
	playbackID, ok := mc.parsePlaybackID(line)
	if ok {
		// Enrich labels for the lines that contains playbackID
		oldStr := mc.streamLabel(playbackID)
		newStr := mc.enrichLabels(playbackID)
		return strings.Replace(line, oldStr, newStr, 1)
	}
	return line
}

func (mc *mac) enrichConstLabels(line string) string {
	constLabels := fmt.Sprintf(`catalyst="true",catalyst_node="%s"`, mc.nodeID)
	if len(line) == 0 || strings.HasPrefix(line, "#") {
		// empty lines or comments
		return line
	}
	if strings.Contains(line, "}") {
		// metrics with labels
		return strings.Replace(line, "}", fmt.Sprintf(",%s}", constLabels), 1)
	}
	// metrics without labels
	lineSplit := strings.Split(line, " ")
	if len(lineSplit) < 2 {
		// invalid metric, do not enrich
		return line
	}
	metricName := lineSplit[0]
	return strings.Replace(line, metricName, fmt.Sprintf("%s{%s}", metricName, constLabels), 1)
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
