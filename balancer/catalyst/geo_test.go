package catalyst

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

var NodeGeos = []ScoredNode{
	{Node: Node{ID: "lax-1", GeoLatitude: 33.94, GeoLongitude: -118.41}},
	{Node: Node{ID: "lax-2", GeoLatitude: 33.94, GeoLongitude: -118.41}},
	{Node: Node{ID: "lax-3", GeoLatitude: 33.94, GeoLongitude: -118.41}},
	{Node: Node{ID: "mdw-1", GeoLatitude: 41.88, GeoLongitude: -87.63}},
	{Node: Node{ID: "mdw-2", GeoLatitude: 41.88, GeoLongitude: -87.63}},
	{Node: Node{ID: "mdw-3", GeoLatitude: 41.88, GeoLongitude: -87.63}},
	{Node: Node{ID: "mdw-4", GeoLatitude: 41.88, GeoLongitude: -87.63}},
	{Node: Node{ID: "mdw-5", GeoLatitude: 41.88, GeoLongitude: -87.63}},
	{Node: Node{ID: "nyc-1", GeoLatitude: 40.71, GeoLongitude: -74.01}},
	{Node: Node{ID: "nyc-2", GeoLatitude: 40.71, GeoLongitude: -74.01}},
	{Node: Node{ID: "lon-1", GeoLatitude: 51.51, GeoLongitude: 0.13}},
	{Node: Node{ID: "lon-2", GeoLatitude: 51.51, GeoLongitude: 0.13}},
	{Node: Node{ID: "lon-3", GeoLatitude: 51.51, GeoLongitude: 0.13}},
	{Node: Node{ID: "lon-4", GeoLatitude: 51.51, GeoLongitude: 0.13}},
	{Node: Node{ID: "sao-1", GeoLatitude: -23.55, GeoLongitude: -46.63}},
	{Node: Node{ID: "sao-2", GeoLatitude: -23.55, GeoLongitude: -46.63}},
	{Node: Node{ID: "fra-1", GeoLatitude: 50.11, GeoLongitude: 8.68}},
	{Node: Node{ID: "fra-2", GeoLatitude: 50.11, GeoLongitude: 8.68}},
	{Node: Node{ID: "fra-3", GeoLatitude: 50.11, GeoLongitude: 8.68}},
	{Node: Node{ID: "fra-4", GeoLatitude: 50.11, GeoLongitude: 8.68}},
	{Node: Node{ID: "fra-5", GeoLatitude: 50.11, GeoLongitude: 8.68}},
	{Node: Node{ID: "prg-1", GeoLatitude: 50.08, GeoLongitude: 14.44}},
	{Node: Node{ID: "prg-2", GeoLatitude: 50.08, GeoLongitude: 14.44}},
	{Node: Node{ID: "sin-1", GeoLatitude: 1.35, GeoLongitude: 103.82}},
	{Node: Node{ID: "sin-2", GeoLatitude: 1.35, GeoLongitude: 103.82}},
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
}

func getGeoScores(requestLatitude, requestLongitude float64) (good, okay, bad []string) {
	scoredNodes := geoScores(NodeGeos, requestLatitude, requestLongitude)
	for _, scoredNode := range scoredNodes {
		if scoredNode.GeoScore == 2 {
			good = append(good, scoredNode.ID)
		}
		if scoredNode.GeoScore == 1 {
			okay = append(okay, scoredNode.ID)
		}
		if scoredNode.GeoScore == 0 {
			bad = append(bad, scoredNode.ID)
		}
	}

	slices.Sort(good)
	slices.Sort(okay)
	slices.Sort(bad)
	return
}
