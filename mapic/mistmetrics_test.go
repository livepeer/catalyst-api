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
mist_sessions_count{sessType="viewers"}93
mist_logs 14860632
`

func TestEnrichMistMetrics(t *testing.T) {
	// given
	mc := mac{
		baseStreamName: "video",
		nodeID:         "fra-staging-staging-catalyst-0.livepeer.monster",
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
		`version{app="MistServer",version="729ddd4b42980d0124c72a46f13d8e0697293e94",release="Generic_x86_64",catalyst="true",catalyst_node="fra-staging-staging-catalyst-0.livepeer.monster"} 1`,
		`mist_sessions{stream="video+077bh6xx5bx5tdua",user_id="abcdefgh-123456789",sessType="viewers",catalyst="true",catalyst_node="fra-staging-staging-catalyst-0.livepeer.monster"}1`,
		`mist_latency{stream="video+077bh6xx5bx5tdua",user_id="abcdefgh-123456789",source="sin-prod-catalyst-3.lp-playback.studio",catalyst="true",catalyst_node="fra-staging-staging-catalyst-0.livepeer.monster"}1795`,
		`mist_sessions{stream="video+51b13mqy7sgw520w",user_id="hgfedcba-987654321",sessType="viewers",catalyst="true",catalyst_node="fra-staging-staging-catalyst-0.livepeer.monster"}5`,
		`mist_latency{stream="video+51b13mqy7sgw520w",user_id="hgfedcba-987654321",source="prg-prod-catalyst-0.lp-playback.studio",catalyst="true",catalyst_node="fra-staging-staging-catalyst-0.livepeer.monster"}1156`,
		`mist_sessions_count{sessType="viewers",catalyst="true",catalyst_node="fra-staging-staging-catalyst-0.livepeer.monster"}93`,
		`mist_logs{catalyst="true",catalyst_node="fra-staging-staging-catalyst-0.livepeer.monster"} 14860632`,
	}
	for _, exp := range expLines {
		require.Contains(t, resLines, exp)
	}
}
