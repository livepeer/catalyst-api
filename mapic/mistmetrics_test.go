package mistapiconnector

import (
	"github.com/livepeer/go-api-client"
	"github.com/stretchr/testify/require"
	"regexp"
	"strings"
	"testing"
)

const mistMetrics = `
# HELP version Current software version as a tag, always 1
# TYPE version gauge
version{app="MistServer",version="729ddd4b42980d0124c72a46f13d8e0697293e94",release="Generic_x86_64"} 1

# HELP mist_viewcount Count of unique viewer sessions since stream start, per stream.
# TYPE mist_viewcount counter
mist_sessions{stream="video+077bh6xx5bx5tdua",sessType="viewers"}1
mist_latency{stream="video+077bh6xx5bx5tdua",source="sin-prod-catalyst-3.lp-playback.studio"}1795
mist_sessions{stream="video+51b13mqy7sgw520w",sessType="viewers"}5
mist_latency{stream="video+51b13mqy7sgw520w",source="prg-prod-catalyst-0.lp-playback.studio"}1156
`

func TestEnrichMistMetrics(t *testing.T) {
	// given
	mc := mac{
		baseStreamName: "video",
		streamInfo: map[string]*streamInfo{
			"077bh6xx5bx5tdua": {stream: &api.Stream{UserID: "abcdefgh-123456789"}},
			"51b13mqy7sgw520w": {stream: &api.Stream{UserID: "hgfedcba-987654321"}},
		},
		streamMetricsRe: regexp.MustCompile(`stream="video\+(.*?)"`),
	}

	// when
	res := mc.enrichMistMetrics(mistMetrics)

	// then
	inLines := strings.Split(mistMetrics, "\n")
	resLines := strings.Split(res, "\n")
	require.Equal(t, len(inLines), len(resLines))

	expLines := []string{
		`version{app="MistServer",version="729ddd4b42980d0124c72a46f13d8e0697293e94",release="Generic_x86_64"} 1`,
		`mist_sessions{stream="video+077bh6xx5bx5tdua",userId="abcdefgh-123456789",sessType="viewers"}1`,
		`mist_latency{stream="video+077bh6xx5bx5tdua",userId="abcdefgh-123456789",source="sin-prod-catalyst-3.lp-playback.studio"}1795`,
		`mist_sessions{stream="video+51b13mqy7sgw520w",userId="hgfedcba-987654321",sessType="viewers"}5`,
		`mist_latency{stream="video+51b13mqy7sgw520w",userId="hgfedcba-987654321",source="prg-prod-catalyst-0.lp-playback.studio"}1156`,
	}
	for _, exp := range expLines {
		require.Contains(t, resLines, exp)
	}
}
