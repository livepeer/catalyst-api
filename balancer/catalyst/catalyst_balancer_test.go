package catalyst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var CPUOverloadedNode = Node{
	ID: "cpu_overload",
	NodeMetrics: NodeMetrics{
		CPUUsagePercentage:       100,
		RAMUsagePercentage:       0,
		BandwidthUsagePercentage: 0,
	},
	Streams: map[string]Stream{},
}

var RAMOverloadedNode = Node{
	ID: "mem_overload",
	NodeMetrics: NodeMetrics{
		CPUUsagePercentage:       0,
		RAMUsagePercentage:       100,
		BandwidthUsagePercentage: 0,
	},
	Streams: map[string]Stream{},
}

var BandwidthOverloadedNode = Node{
	ID: "bw_overload",
	NodeMetrics: NodeMetrics{
		CPUUsagePercentage:       0,
		RAMUsagePercentage:       0,
		BandwidthUsagePercentage: 100,
	},
	Streams: map[string]Stream{},
}

func TestItReturnsItselfWhenNoOtherNodesPresent(t *testing.T) {
	// Make the node handling the request unfavourable in terms of stats, to make sure
	// it'll still pick itself if it's the only option
	_, err := SelectNode([]Node{}, "some-stream-id", 0, 0)
	require.EqualError(t, err, "no nodes to select from")
}

func TestItDoesntChooseOverloadedNodes(t *testing.T) {
	expectedNode := Node{
		NodeMetrics: NodeMetrics{
			CPUUsagePercentage:       10,
			RAMUsagePercentage:       10,
			BandwidthUsagePercentage: 10,
		},
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
		{ID: "good-node-1", Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
		{ID: "good-node-2", Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
		{ID: "good-node-3", Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name"}}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{ID: "good-node-4", Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name"}}},
		{ID: "good-node-5", Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
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
		{ID: "local-with-stream-high-cpu", NodeMetrics: NodeMetrics{CPUUsagePercentage: 90}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
		{ID: "local-without-stream", Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name"}}},
		{ID: "local-without-stream-2", Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name"}}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{ID: "far-with-stream", GeoLatitude: 100, GeoLongitude: 100, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
		{ID: "far-without-stream", GeoLatitude: 100, GeoLongitude: 100, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
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

func TestItChoosesLeastBad(t *testing.T) {
	requestLatitude, requestLongitude := 51.7520, 1.2577 // Oxford
	highCPULocal := Node{ID: "local-high-cpu", NodeMetrics: NodeMetrics{CPUUsagePercentage: 90}, GeoLatitude: requestLatitude, GeoLongitude: requestLongitude}
	highCPUWithStream := Node{ID: "far-high-cpu", NodeMetrics: NodeMetrics{CPUUsagePercentage: 90}, GeoLatitude: 1.35, GeoLongitude: 103.82,
		Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}} // Sin
	highCPU := Node{ID: "far-medium-cpu", NodeMetrics: NodeMetrics{CPUUsagePercentage: 90}, GeoLatitude: 1.35, GeoLongitude: 103.82}    // Sin
	mediumCPU := Node{ID: "far-medium-cpu", NodeMetrics: NodeMetrics{CPUUsagePercentage: 80}, GeoLatitude: 1.35, GeoLongitude: 103.82}  // Sin
	lowCPU := Node{ID: "far-low-cpu", NodeMetrics: NodeMetrics{CPUUsagePercentage: 40}, GeoLatitude: 1.35, GeoLongitude: 103.82}        // Sin
	lowCPUOkDistance := Node{ID: "low-cpu", NodeMetrics: NodeMetrics{CPUUsagePercentage: 40}, GeoLatitude: 41.88, GeoLongitude: -87.63} // Mdw
	mediumMem := Node{ID: "far-medium-mem", NodeMetrics: NodeMetrics{RAMUsagePercentage: 80}, GeoLatitude: 1.35, GeoLongitude: 103.82}  // Sin
	mediumMemWithStream := Node{ID: "far-medium-mem-with-stream", NodeMetrics: NodeMetrics{RAMUsagePercentage: 80}, GeoLatitude: 1.35, GeoLongitude: 103.82,
		Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}} // Sin

	tests := []struct {
		name  string
		nodes []Node
		want  []ScoredNode
	}{
		{
			name:  "it sorts on resource utilisation",
			nodes: []Node{highCPULocal, mediumCPU, lowCPU, highCPU},
			want: []ScoredNode{
				{Score: 2, GeoScore: 2, GeoDistance: 0, Node: highCPULocal},
				{Score: 2, GeoScore: 0, GeoDistance: 10749, Node: lowCPU},
				{Score: 1, GeoScore: 0, GeoDistance: 10749, Node: mediumCPU},
			},
		},
		{
			name:  "it sorts on mixed resource utilisation",
			nodes: []Node{highCPULocal, lowCPU, mediumMem},
			want: []ScoredNode{
				{Score: 2, GeoScore: 2, GeoDistance: 0, Node: highCPULocal},
				{Score: 2, GeoScore: 0, GeoDistance: 10749, Node: lowCPU},
				{Score: 1, GeoScore: 0, GeoDistance: 10749, Node: mediumMem},
			},
		},
		{
			name:  "high CPU node picked last",
			nodes: []Node{highCPULocal, lowCPU, highCPU},
			want: []ScoredNode{
				{Score: 2, GeoScore: 2, GeoDistance: 0, Node: highCPULocal},
				{Score: 2, GeoScore: 0, GeoDistance: 10749, Node: lowCPU},
				{Score: 0, GeoScore: 0, GeoDistance: 10749, Node: highCPU},
			},
		},
		{
			name:  "okay distance unloaded node",
			nodes: []Node{highCPULocal, mediumCPU, lowCPUOkDistance},
			want: []ScoredNode{
				{Score: 3, GeoScore: 1, GeoDistance: 6424, Node: lowCPUOkDistance},
				{Score: 2, GeoScore: 2, GeoDistance: 0, Node: highCPULocal},
				{Score: 1, GeoScore: 0, GeoDistance: 10749, Node: mediumCPU},
			},
		},
		{
			name:  "medium loaded node with the stream",
			nodes: []Node{highCPULocal, lowCPU, mediumMem, mediumMemWithStream},
			want: []ScoredNode{
				{Score: 3, GeoScore: 0, GeoDistance: 10749, Node: mediumMemWithStream},
				{Score: 2, GeoScore: 2, GeoDistance: 0, Node: highCPULocal},
				{Score: 2, GeoScore: 0, GeoDistance: 10749, Node: lowCPU},
			},
		},
		{
			name:  "high loaded node with the stream",
			nodes: []Node{highCPULocal, lowCPU, highCPUWithStream},
			want: []ScoredNode{
				{Score: 2, GeoScore: 2, GeoDistance: 0, Node: highCPULocal},
				{Score: 2, GeoScore: 0, GeoDistance: 10749, Node: lowCPU},
				{Score: 2, GeoScore: 0, GeoDistance: 10749, Node: highCPUWithStream},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectTopNodes(tt.nodes, "stream-name-we-want", requestLatitude, requestLongitude, 3)
			require.Equal(t, tt.want, got)
		})
	}
}
