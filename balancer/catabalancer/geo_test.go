package catabalancer

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

var NodeGeos = []ScoredNode{
	{Node: Node{Name: "lax-1"}, NodeMetrics: NodeMetrics{GeoLatitude: 33.94, GeoLongitude: -118.41}},
	{Node: Node{Name: "lax-2"}, NodeMetrics: NodeMetrics{GeoLatitude: 33.94, GeoLongitude: -118.41}},
	{Node: Node{Name: "lax-3"}, NodeMetrics: NodeMetrics{GeoLatitude: 33.94, GeoLongitude: -118.41}},
	{Node: Node{Name: "mdw-1"}, NodeMetrics: NodeMetrics{GeoLatitude: 41.88, GeoLongitude: -87.63}},
	{Node: Node{Name: "mdw-2"}, NodeMetrics: NodeMetrics{GeoLatitude: 41.88, GeoLongitude: -87.63}},
	{Node: Node{Name: "mdw-3"}, NodeMetrics: NodeMetrics{GeoLatitude: 41.88, GeoLongitude: -87.63}},
	{Node: Node{Name: "mdw-4"}, NodeMetrics: NodeMetrics{GeoLatitude: 41.88, GeoLongitude: -87.63}},
	{Node: Node{Name: "mdw-5"}, NodeMetrics: NodeMetrics{GeoLatitude: 41.88, GeoLongitude: -87.63}},
	{Node: Node{Name: "nyc-1"}, NodeMetrics: NodeMetrics{GeoLatitude: 40.71, GeoLongitude: -74.01}},
	{Node: Node{Name: "nyc-2"}, NodeMetrics: NodeMetrics{GeoLatitude: 40.71, GeoLongitude: -74.01}},
	{Node: Node{Name: "lon-1"}, NodeMetrics: NodeMetrics{GeoLatitude: 51.51, GeoLongitude: 0.13}},
	{Node: Node{Name: "lon-2"}, NodeMetrics: NodeMetrics{GeoLatitude: 51.51, GeoLongitude: 0.13}},
	{Node: Node{Name: "lon-3"}, NodeMetrics: NodeMetrics{GeoLatitude: 51.51, GeoLongitude: 0.13}},
	{Node: Node{Name: "lon-4"}, NodeMetrics: NodeMetrics{GeoLatitude: 51.51, GeoLongitude: 0.13}},
	{Node: Node{Name: "sao-1"}, NodeMetrics: NodeMetrics{GeoLatitude: -23.55, GeoLongitude: -46.63}},
	{Node: Node{Name: "sao-2"}, NodeMetrics: NodeMetrics{GeoLatitude: -23.55, GeoLongitude: -46.63}},
	{Node: Node{Name: "fra-1"}, NodeMetrics: NodeMetrics{GeoLatitude: 50.11, GeoLongitude: 8.68}},
	{Node: Node{Name: "fra-2"}, NodeMetrics: NodeMetrics{GeoLatitude: 50.11, GeoLongitude: 8.68}},
	{Node: Node{Name: "fra-3"}, NodeMetrics: NodeMetrics{GeoLatitude: 50.11, GeoLongitude: 8.68}},
	{Node: Node{Name: "fra-4"}, NodeMetrics: NodeMetrics{GeoLatitude: 50.11, GeoLongitude: 8.68}},
	{Node: Node{Name: "fra-5"}, NodeMetrics: NodeMetrics{GeoLatitude: 50.11, GeoLongitude: 8.68}},
	{Node: Node{Name: "prg-1"}, NodeMetrics: NodeMetrics{GeoLatitude: 50.08, GeoLongitude: 14.44}},
	{Node: Node{Name: "prg-2"}, NodeMetrics: NodeMetrics{GeoLatitude: 50.08, GeoLongitude: 14.44}},
	{Node: Node{Name: "sin-1"}, NodeMetrics: NodeMetrics{GeoLatitude: 1.35, GeoLongitude: 103.82}},
	{Node: Node{Name: "sin-2"}, NodeMetrics: NodeMetrics{GeoLatitude: 1.35, GeoLongitude: 103.82}},
	// add syd
}

func TestGeoScores(t *testing.T) {
	// Oxford - Europe should be good, West Coast USA should be okay
	good, okay, bad := getGeoScores(51.7520, 1.2577)
	require.Equal(t, []string{"fra-1", "fra-2", "fra-3", "fra-4", "fra-5", "lon-1", "lon-2", "lon-3", "lon-4", "prg-1", "prg-2"}, good)
	require.Equal(t, []string{"mdw-1", "mdw-2", "mdw-3", "mdw-4", "mdw-5", "nyc-1", "nyc-2"}, okay)
	require.Equal(t, []string{"lax-1", "lax-2", "lax-3", "sao-1", "sao-2", "sin-1", "sin-2"}, bad)

	// New York - Central / East USA should be good, Europe, West Coast should be okay
	good, okay, bad = getGeoScores(40.7128, -74.0060)
	require.Equal(t, []string{"mdw-1", "mdw-2", "mdw-3", "mdw-4", "mdw-5", "nyc-1", "nyc-2"}, good)
	require.Equal(t, []string{"fra-1", "fra-2", "fra-3", "fra-4", "fra-5", "lax-1", "lax-2", "lax-3", "lon-1", "lon-2", "lon-3", "lon-4", "prg-1", "prg-2"}, okay)
	require.Equal(t, []string{"sao-1", "sao-2", "sin-1", "sin-2"}, bad)

	// Lima - SAO should be good, USA and Western Europe should be okay
	good, okay, bad = getGeoScores(-12.046374, -77.042793)
	require.Equal(t, []string{"sao-1", "sao-2"}, good)
	require.Equal(t, []string{"fra-1", "fra-2", "fra-3", "fra-4", "fra-5", "lax-1", "lax-2", "lax-3", "lon-1", "lon-2", "lon-3", "lon-4", "mdw-1", "mdw-2", "mdw-3", "mdw-4", "mdw-5", "nyc-1", "nyc-2"}, okay)
	require.Equal(t, []string{"prg-1", "prg-2", "sin-1", "sin-2"}, bad)

	// Delhi - SIN good, Europe okay
	good, okay, bad = getGeoScores(28.644800, 77.216721)
	require.Equal(t, []string{"sin-1", "sin-2"}, good)
	require.Equal(t, []string{"fra-1", "fra-2", "fra-3", "fra-4", "fra-5", "lon-1", "lon-2", "lon-3", "lon-4", "prg-1", "prg-2"}, okay)
	require.Equal(t, []string{"lax-1", "lax-2", "lax-3", "mdw-1", "mdw-2", "mdw-3", "mdw-4", "mdw-5", "nyc-1", "nyc-2", "sao-1", "sao-2"}, bad)

	// Southern tip of Argentina - SAO good, USA okay
	good, okay, bad = getGeoScores(-54.93355, -67.60963)
	require.Equal(t, []string{"sao-1", "sao-2"}, good)
	require.Equal(t, []string{"lax-1", "lax-2", "lax-3", "mdw-1", "mdw-2", "mdw-3", "mdw-4", "mdw-5", "nyc-1", "nyc-2"}, okay)
	require.Equal(t, []string{"fra-1", "fra-2", "fra-3", "fra-4", "fra-5", "lon-1", "lon-2", "lon-3", "lon-4", "prg-1", "prg-2", "sin-1", "sin-2"}, bad)

	// Auckland (New Zealand) - Singapore good, USA + SAO okay
	good, okay, bad = getGeoScores(-36.848461, 174.763336)
	require.Equal(t, []string{"sin-1", "sin-2"}, good)
	require.Equal(t, []string{"lax-1", "lax-2", "lax-3", "mdw-1", "mdw-2", "mdw-3", "mdw-4", "mdw-5", "nyc-1", "nyc-2", "sao-1", "sao-2"}, okay)
	require.Equal(t, []string{"fra-1", "fra-2", "fra-3", "fra-4", "fra-5", "lon-1", "lon-2", "lon-3", "lon-4", "prg-1", "prg-2"}, bad)
}

func getGeoScores(requestLatitude, requestLongitude float64) (good, okay, bad []string) {
	scoredNodes := geoScores(NodeGeos, requestLatitude, requestLongitude)
	for _, scoredNode := range scoredNodes {
		if scoredNode.GeoScore == 2 {
			good = append(good, scoredNode.Name)
		}
		if scoredNode.GeoScore == 1 {
			okay = append(okay, scoredNode.Name)
		}
		if scoredNode.GeoScore == 0 {
			bad = append(bad, scoredNode.Name)
		}
	}

	slices.Sort(good)
	slices.Sort(okay)
	slices.Sort(bad)
	return
}
