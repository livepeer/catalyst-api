package mistapiconnector

import (
	"fmt"
	"github.com/golang/glog"
	"strings"
)

func (mc *mac) enrichMistMetrics(metrics string) string {
	res := strings.Builder{}
	lines := strings.Split(metrics, "\n")
	for i, line := range lines {
		playbackID, ok := mc.parsePlaybackID(line)
		if ok {
			oldStr := mc.streamLabel(playbackID)
			newStr := mc.enrichLabels(playbackID)
			res.WriteString(strings.Replace(line, oldStr, newStr, -1))
		} else {
			res.WriteString(line)
		}
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
	si, err := mc.getStreamInfo(playbackID)
	if err != nil {
		glog.Warning("could not enrich Mist metrics for strm=%s err=%v", playbackID, err)
	} else if si == nil || si.stream == nil {
		glog.Warning("could not enrich Mist metrics for strm=%s, cannot fetch stream info", playbackID)
	} else {
		res += fmt.Sprintf(`,userId="%s"`, si.stream.UserID)
	}

	return res
}

func (mc *mac) streamLabel(playbackID string) string {
	return fmt.Sprintf(`stream="%s+%s"`, mc.baseStreamName, playbackID)
}
