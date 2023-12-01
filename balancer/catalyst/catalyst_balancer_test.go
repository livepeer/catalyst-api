package catalyst

import (
	"context"
	"github.com/livepeer/catalyst-api/cluster"
	"golang.org/x/sync/errgroup"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var CPUOverloadedNode = ScoredNode{
	Node: Node{Name: "cpu_overload"},
	NodeMetrics: NodeMetrics{
		CPUUsagePercentage:       100,
		RAMUsagePercentage:       0,
		BandwidthUsagePercentage: 0,
	},
}

var RAMOverloadedNode = ScoredNode{
	Node: Node{Name: "mem_overload"},
	NodeMetrics: NodeMetrics{
		CPUUsagePercentage:       0,
		RAMUsagePercentage:       100,
		BandwidthUsagePercentage: 0,
	},
}

var BandwidthOverloadedNode = ScoredNode{
	Node: Node{Name: "bw_overload"},
	NodeMetrics: NodeMetrics{
		CPUUsagePercentage:       0,
		RAMUsagePercentage:       0,
		BandwidthUsagePercentage: 100,
	},
}

func TestItReturnsItselfWhenNoOtherNodesPresent(t *testing.T) {
	// Make the node handling the request unfavourable in terms of stats, to make sure
	// it'll still pick itself if it's the only option
	_, err := SelectNode([]ScoredNode{}, "some-stream-id", 0, 0)
	require.EqualError(t, err, "no nodes to select from")
}

func TestItReturnsBadNodeIfOnlyAvailable(t *testing.T) {
	selectionNodes := []ScoredNode{
		CPUOverloadedNode,
	}

	n, err := SelectNode(selectionNodes, "some-stream-id", 0, 0)
	require.NoError(t, err)
	require.Equal(t, CPUOverloadedNode.Node, n)
}

func TestItDoesntChooseOverloadedNodes(t *testing.T) {
	expectedNode := ScoredNode{
		Node: Node{Name: "expected"},
		NodeMetrics: NodeMetrics{
			CPUUsagePercentage:       10,
			RAMUsagePercentage:       10,
			BandwidthUsagePercentage: 10,
		},
	}
	selectionNodes := []ScoredNode{
		CPUOverloadedNode,
		RAMOverloadedNode,
		expectedNode,
		BandwidthOverloadedNode,
	}

	n, err := SelectNode(selectionNodes, "some-stream-id", 0, 0)
	require.NoError(t, err)
	require.Equal(t, expectedNode.Node, n)
}

func TestItChoosesRandomlyFromTheBestNodes(t *testing.T) {
	selectionNodes := []ScoredNode{
		{Node: Node{Name: "good-node-1"}},
		{Node: Node{Name: "good-node-2"}},
		{Node: Node{Name: "good-node-3"}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{Node: Node{Name: "good-node-4"}},
		{Node: Node{Name: "good-node-5"}},
	}

	foundNodes := map[string]bool{}
	for i := 0; i < 1000; i++ {
		n, err := SelectNode(selectionNodes, "some-stream-id", 0, 0)
		require.NoError(t, err)
		foundNodes[n.Name] = true
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
	selectionNodes := []ScoredNode{
		{Node: Node{Name: "good-node-1"}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", LastSeen: time.Now()}}},
		{Node: Node{Name: "good-node-2"}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", LastSeen: time.Now()}}},
		{Node: Node{Name: "good-node-3"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", LastSeen: time.Now()}}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{Node: Node{Name: "good-node-4"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", LastSeen: time.Now()}}},
		{Node: Node{Name: "good-node-5"}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", LastSeen: time.Now()}}},
	}

	foundNodes := map[string]bool{}
	for i := 0; i < 1000; i++ {
		n, err := SelectNode(selectionNodes, "stream-name-we-want", 0, 0)
		require.NoError(t, err)
		foundNodes[n.Name] = true
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
	selectionNodes := []ScoredNode{
		{Node: Node{Name: "local-with-stream-high-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 90}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
		{Node: Node{Name: "local-without-stream"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", LastSeen: time.Now()}}},
		{Node: Node{Name: "local-without-stream-2"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", LastSeen: time.Now()}}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{Node: Node{Name: "far-with-stream"}, NodeMetrics: NodeMetrics{GeoLatitude: 100, GeoLongitude: 100}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
		{Node: Node{Name: "far-without-stream"}, NodeMetrics: NodeMetrics{GeoLatitude: 100, GeoLongitude: 100}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
	}

	foundNodes := map[string]bool{}
	for i := 0; i < 1000; i++ {
		n, err := SelectNode(selectionNodes, "stream-name-we-want", 0, 0)
		require.NoError(t, err)
		foundNodes[n.Name] = true
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
	highCPULocal := ScoredNode{Node: Node{Name: "local-high-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 90, GeoLatitude: requestLatitude, GeoLongitude: requestLongitude}}
	highCPUWithStream := ScoredNode{Node: Node{Name: "far-high-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 90, GeoLatitude: 1.35, GeoLongitude: 103.82},
		Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", LastSeen: time.Now()}}} // Sin
	highCPU := ScoredNode{Node: Node{Name: "far-medium-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 90, GeoLatitude: 1.35, GeoLongitude: 103.82}}    // Sin
	mediumCPU := ScoredNode{Node: Node{Name: "far-medium-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 80, GeoLatitude: 1.35, GeoLongitude: 103.82}}  // Sin
	lowCPU := ScoredNode{Node: Node{Name: "far-low-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 40, GeoLatitude: 1.35, GeoLongitude: 103.82}}        // Sin
	lowCPUOkDistance := ScoredNode{Node: Node{Name: "low-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 40, GeoLatitude: 41.88, GeoLongitude: -87.63}} // Mdw
	mediumMem := ScoredNode{Node: Node{Name: "far-medium-mem"}, NodeMetrics: NodeMetrics{RAMUsagePercentage: 80, GeoLatitude: 1.35, GeoLongitude: 103.82}}  // Sin
	mediumMemWithStream := ScoredNode{Node: Node{Name: "far-medium-mem-with-stream"}, NodeMetrics: NodeMetrics{RAMUsagePercentage: 80, GeoLatitude: 1.35, GeoLongitude: 103.82},
		Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", LastSeen: time.Now()}}} // Sin

	tests := []struct {
		name  string
		nodes []ScoredNode
		want  []ScoredNode
	}{
		{
			name:  "it sorts on resource utilisation",
			nodes: []ScoredNode{highCPULocal, mediumCPU, lowCPU, highCPU},
			want: []ScoredNode{
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(lowCPU, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
				scores(mediumCPU, ScoredNode{Score: 1, GeoScore: 0, GeoDistance: 10749}),
			},
		},
		{
			name:  "it sorts on mixed resource utilisation",
			nodes: []ScoredNode{highCPULocal, lowCPU, mediumMem},
			want: []ScoredNode{
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(lowCPU, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
				scores(mediumMem, ScoredNode{Score: 1, GeoScore: 0, GeoDistance: 10749}),
			},
		},
		{
			name:  "high CPU node picked last",
			nodes: []ScoredNode{highCPULocal, lowCPU, highCPU},
			want: []ScoredNode{
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(lowCPU, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
				scores(highCPU, ScoredNode{Score: 0, GeoScore: 0, GeoDistance: 10749}),
			},
		},
		{
			name:  "okay distance unloaded node",
			nodes: []ScoredNode{highCPULocal, mediumCPU, lowCPUOkDistance},
			want: []ScoredNode{
				scores(lowCPUOkDistance, ScoredNode{Score: 3, GeoScore: 1, GeoDistance: 6424}),
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(mediumCPU, ScoredNode{Score: 1, GeoScore: 0, GeoDistance: 10749}),
			},
		},
		{
			name:  "medium loaded node with the stream",
			nodes: []ScoredNode{highCPULocal, lowCPU, mediumMem, mediumMemWithStream},
			want: []ScoredNode{
				scores(mediumMemWithStream, ScoredNode{Score: 3, GeoScore: 0, GeoDistance: 10749}),
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(lowCPU, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
			},
		},
		{
			name:  "high loaded node with the stream",
			nodes: []ScoredNode{highCPULocal, lowCPU, highCPUWithStream},
			want: []ScoredNode{
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(lowCPU, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
				scores(highCPUWithStream, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
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

func scores(node1 ScoredNode, node2 ScoredNode) ScoredNode {
	node1.Score = node2.Score
	node1.GeoScore = node2.GeoScore
	node1.GeoDistance = node2.GeoDistance
	return node1
}

func TestNoIngestStream(t *testing.T) {
	c := NewBalancer()
	// first test no nodes available
	c.UpdateNodes("id", NodeMetrics{})
	c.UpdateStreams("id", "stream", false)
	source, err := c.MistUtilLoadSource(context.Background(), "stream", "", "")
	require.EqualError(t, err, "no node found for ingest stream: stream")
	require.Empty(t, source)

	// test node present but ingest stream not available
	err = c.UpdateMembers(context.Background(), []cluster.Member{{
		Name: "node",
		Tags: map[string]string{
			"dtsc": "dtsc://nodedtsc",
		},
	}})
	require.NoError(t, err)
	source, err = c.MistUtilLoadSource(context.Background(), "stream", "", "")
	require.EqualError(t, err, "no node found for ingest stream: stream")
	require.Empty(t, source)
}

func TestMistUtilLoadSource(t *testing.T) {
	c := NewBalancer()
	err := c.UpdateMembers(context.Background(), []cluster.Member{{
		Name: "node",
		Tags: map[string]string{
			"dtsc": "dtsc://nodedtsc",
		},
	}})
	require.NoError(t, err)

	metrics := NodeMetrics{
		CPUUsagePercentage:       10,
		RAMUsagePercentage:       10,
		BandwidthUsagePercentage: 10,
		GeoLatitude:              10,
		GeoLongitude:             10,
	}
	c.UpdateNodes("node", metrics)
	require.Equal(t, map[string]NodeMetrics{
		"node": metrics,
	}, c.NodeMetrics)

	c.UpdateStreams("node", "stream", false)
	c.UpdateStreams("node", "ingest", true)
	require.Len(t, c.Streams, 1)
	require.Equal(t, "stream", c.Streams["node"]["stream"].ID)

	source, err := c.MistUtilLoadSource(context.Background(), "ingest", "", "")
	require.NoError(t, err)
	require.Equal(t, "dtsc://nodedtsc", source)

	err = c.UpdateMembers(context.Background(), []cluster.Member{})
	require.NoError(t, err)
	require.Empty(t, c.IngestStreams)
	source, err = c.MistUtilLoadSource(context.Background(), "ingest", "", "")
	require.EqualError(t, err, "no node found for ingest stream: ingest")
	require.Empty(t, source)
}

func TestStreamTimeout(t *testing.T) {
	c := NewBalancer()
	err := c.UpdateMembers(context.Background(), []cluster.Member{{
		Name: "node",
		Tags: map[string]string{
			"dtsc": "dtsc://nodedtsc",
		},
	}})
	require.NoError(t, err)

	streamTimeout = 5 * time.Second
	c.UpdateStreams("node", "stream", false)
	c.UpdateStreams("node", "ingest", true)

	require.Equal(t, "stream", c.Streams["node"]["stream"].ID)
	require.Equal(t, "ingest", c.IngestStreams["node"]["ingest"].ID)

	streamTimeout = -5 * time.Second
	// MistUtilLoadSource should detect the expiry and not return the stream
	source, err := c.MistUtilLoadSource(context.Background(), "ingest", "", "")
	require.EqualError(t, err, "no node found for ingest stream: ingest")
	require.Empty(t, source)
	c.UpdateStreams("node", "ingest", true)
	require.Empty(t, c.Streams["node"])
	c.UpdateStreams("node", "stream", false)
	require.Empty(t, c.IngestStreams["node"])
}

// needs to be run with go test -race
func TestConcurrentUpdates(t *testing.T) {
	c := NewBalancer()

	err := c.UpdateMembers(context.Background(), []cluster.Member{{Name: "node"}})
	require.NoError(t, err)

	errGroup := errgroup.Group{}
	for i := 0; i < 100; i++ {
		i := i
		errGroup.Go(func() error {
			c.UpdateNodes("node", NodeMetrics{CPUUsagePercentage: float64(i)})
			return nil
		})
		errGroup.Go(func() error {
			c.UpdateStreams("node", strconv.Itoa(i), false)
			return nil
		})
	}
	// simulate some member updates at the same time
	for i := 0; i < 100; i++ {
		err = c.UpdateMembers(context.Background(), []cluster.Member{{Name: "node"}})
		require.NoError(t, err)
	}
	require.NoError(t, errGroup.Wait())
}
