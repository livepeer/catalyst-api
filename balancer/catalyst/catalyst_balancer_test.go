package catalyst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var CPUOverloadedNode = Node{
	ID:                       "cpu_overload",
	CPUUsagePercentage:       100,
	RAMUsagePercentage:       0,
	BandwidthUsagePercentage: 0,
	Streams:                  map[string]Stream{},
}

var RAMOverloadedNode = Node{
	ID:                       "mem_overload",
	CPUUsagePercentage:       0,
	RAMUsagePercentage:       100,
	BandwidthUsagePercentage: 0,
	Streams:                  map[string]Stream{},
}

var BandwidthOverloadedNode = Node{
	ID:                       "bw_overload",
	CPUUsagePercentage:       0,
	RAMUsagePercentage:       0,
	BandwidthUsagePercentage: 100,
	Streams:                  map[string]Stream{},
}

func TestItReturnsItselfWhenNoOtherNodesPresent(t *testing.T) {
	// Make the node handling the request unfavourable in terms of stats, to make sure
	// it'll still pick itself if it's the only option
	_, err := SelectNode([]Node{}, "some-stream-id", 0, 0)
	require.EqualError(t, err, "no nodes to select from")
}

func TestItDoesntChooseOverloadedNodes(t *testing.T) {
	expectedNode := Node{
		CPUUsagePercentage:       10,
		RAMUsagePercentage:       10,
		BandwidthUsagePercentage: 10,
	}
	selectionNodes := []Node{
		CPUOverloadedNode,
		RAMOverloadedNode,
		expectedNode,
		BandwidthOverloadedNode,
	}

	n, err := SelectNode(selectionNodes, "some-stream-id", 0, 0)
	require.NoError(t, err)
	require.Equal(t, expectedNode, n)
}

func TestItChoosesRandomlyFromTheBestNodes(t *testing.T) {
	selectionNodes := []Node{
		{ID: "good-node-1"},
		{ID: "good-node-2"},
		{ID: "good-node-3"},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{ID: "good-node-4"},
		{ID: "good-node-5"},
	}

	foundNodes := map[string]bool{}
	for i := 0; i < 1000; i++ {
		n, err := SelectNode(selectionNodes, "some-stream-id", 0, 0)
		require.NoError(t, err)
		foundNodes[n.ID] = true
	}
	require.Equal(
		t,
		map[string]bool{
			"good-node-1": true,
			"good-node-2": true,
			"good-node-3": true,
			"good-node-4": true,
			"good-node-5": true,
		},
		foundNodes,
	)
}

// If we have geographically local servers that aren't overloaded _and_ already have the stream replicated to them
// then that's the ideal situation and we should prefer that over everything else
func TestItPrefersLocalUnloadedServersWithStreamAlready(t *testing.T) {
	selectionNodes := []Node{
		{ID: "good-node-1", Streams: map[string]Stream{"stream-name-we-want": Stream{ID: "stream-name-we-want"}}},
		{ID: "good-node-2", Streams: map[string]Stream{"stream-name-we-want": Stream{ID: "stream-name-we-want"}}},
		{ID: "good-node-3", Streams: map[string]Stream{"other-steam-name": Stream{ID: "other-steam-name"}}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{ID: "good-node-4", Streams: map[string]Stream{"other-steam-name": Stream{ID: "other-steam-name"}}},
		{ID: "good-node-5", Streams: map[string]Stream{"stream-name-we-want": Stream{ID: "stream-name-we-want"}}},
	}

	foundNodes := map[string]bool{}
	for i := 0; i < 1000; i++ {
		n, err := SelectNode(selectionNodes, "stream-name-we-want", 0, 0)
		require.NoError(t, err)
		foundNodes[n.ID] = true
	}
	require.Equal(
		t,
		map[string]bool{
			"good-node-1": true,
			"good-node-2": true,
			"good-node-5": true,
		},
		foundNodes,
	)
}

// If the geographically local servers that have the stream already are overloaded, prefer local ones
// that aren't overloaded, even if they haven't had the stream replicated to them yet
func TestItPrefersLocalUnloadedServersWithoutStream(t *testing.T) {
	selectionNodes := []Node{
		{ID: "local-with-stream-high-cpu", CPUUsagePercentage: 90, Streams: map[string]Stream{"stream-name-we-want": Stream{ID: "stream-name-we-want"}}},
		{ID: "local-without-stream", Streams: map[string]Stream{"other-steam-name": Stream{ID: "other-steam-name"}}},
		{ID: "local-without-stream-2", Streams: map[string]Stream{"other-steam-name": Stream{ID: "other-steam-name"}}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{ID: "far-with-stream", GeoLatitude: 100, GeoLongitude: 100, Streams: map[string]Stream{"stream-name-we-want": Stream{ID: "stream-name-we-want"}}},
		{ID: "far-without-stream", GeoLatitude: 100, GeoLongitude: 100, Streams: map[string]Stream{"stream-name-we-want": Stream{ID: "stream-name-we-want"}}},
	}

	foundNodes := map[string]bool{}
	for i := 0; i < 1000; i++ {
		n, err := SelectNode(selectionNodes, "stream-name-we-want", 0, 0)
		require.NoError(t, err)
		foundNodes[n.ID] = true
	}
	require.Equal(
		t,
		map[string]bool{
			"local-without-stream":   true,
			"local-without-stream-2": true,
		},
		foundNodes,
	)
}
