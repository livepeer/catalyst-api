package catabalancer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/patrickmn/go-cache"
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

var mediaTags = map[string]string{"node": "media", "dtsc": "dtsc://nodedtsc"}

func mockDB(t *testing.T) *sql.DB {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		mock.ExpectQuery("SELECT stats FROM node_stats").
			WillReturnRows(sqlmock.NewRows([]string{"stats"}).AddRow("{}"))

	}
	return db
}

func setNodeMetrics(t *testing.T, mock sqlmock.Sqlmock, nodeStats []NodeUpdateEvent) {
	rows := sqlmock.NewRows([]string{"stats"})
	for _, s := range nodeStats {
		payload, err := json.Marshal(s)
		require.NoError(t, err)
		rows.AddRow(payload)
	}
	mock.ExpectQuery("SELECT stats FROM node_stats").
		WillReturnRows(rows)
}

func TestItReturnsItselfWhenNoOtherNodesPresent(t *testing.T) {
	c := NewBalancer("me", time.Second, time.Second, mockDB(t))
	nodeName, prefix, err := c.GetBestNode(context.Background(), nil, "playbackID", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, "me", nodeName)
	require.Equal(t, "video+playbackID", prefix)
}

func TestStaleNodes(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	c := NewBalancer("me", time.Second, time.Second, db)
	c.nodeStatsCache = cache.New(1*time.Millisecond, time.Minute)
	err = c.UpdateMembers(context.Background(), []cluster.Member{{Name: "node1", Tags: mediaTags}})
	require.NoError(t, err)

	// node is stale, old timestamp
	setNodeMetrics(t, mock, []NodeUpdateEvent{{NodeID: "node1", NodeMetrics: NodeMetrics{}}})
	c.metricTimeout = -5 * time.Second
	nodeName, prefix, err := c.GetBestNode(context.Background(), nil, "playbackID", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, "me", nodeName) // we expect node1 to be ignored
	require.Equal(t, "video+playbackID", prefix)

	// node is fresh, recent timestamp
	time.Sleep(2 * time.Millisecond)
	setNodeMetrics(t, mock, []NodeUpdateEvent{{NodeID: "node1", NodeMetrics: NodeMetrics{Timestamp: time.Now()}}})
	c.metricTimeout = 5 * time.Second
	nodeName, prefix, err = c.GetBestNode(context.Background(), nil, "playbackID", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, "node1", nodeName) // we expect node1 this time
	require.Equal(t, "video+playbackID", prefix)
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
		{Node: Node{Name: "good-node-1"}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", Timestamp: time.Now()}}},
		{Node: Node{Name: "good-node-2"}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", Timestamp: time.Now()}}},
		{Node: Node{Name: "good-node-3"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", Timestamp: time.Now()}}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{Node: Node{Name: "good-node-4"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", Timestamp: time.Now()}}},
		{Node: Node{Name: "good-node-5"}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", Timestamp: time.Now()}}},
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
		{Node: Node{Name: "local-without-stream"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", Timestamp: time.Now()}}},
		{Node: Node{Name: "local-without-stream-2"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", Timestamp: time.Now()}}},
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
		Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", Timestamp: time.Now()}}} // Sin
	highCPU := ScoredNode{Node: Node{Name: "far-medium-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 90, GeoLatitude: 1.35, GeoLongitude: 103.82}}    // Sin
	mediumCPU := ScoredNode{Node: Node{Name: "far-medium-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 80, GeoLatitude: 1.35, GeoLongitude: 103.82}}  // Sin
	lowCPU := ScoredNode{Node: Node{Name: "far-low-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 40, GeoLatitude: 1.35, GeoLongitude: 103.82}}        // Sin
	lowCPUOkDistance := ScoredNode{Node: Node{Name: "low-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 40, GeoLatitude: 41.88, GeoLongitude: -87.63}} // Mdw
	mediumMem := ScoredNode{Node: Node{Name: "far-medium-mem"}, NodeMetrics: NodeMetrics{RAMUsagePercentage: 80, GeoLatitude: 1.35, GeoLongitude: 103.82}}  // Sin
	mediumMemWithStream := ScoredNode{Node: Node{Name: "far-medium-mem-with-stream"}, NodeMetrics: NodeMetrics{RAMUsagePercentage: 80, GeoLatitude: 1.35, GeoLongitude: 103.82},
		Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", Timestamp: time.Now()}}} // Sin

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
				scores(mediumMemWithStream, ScoredNode{Score: 3, GeoScore: 0, GeoDistance: 10749, StreamScore: 2}),
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
				scores(highCPUWithStream, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749, StreamScore: 2}),
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
	node1.StreamScore = node2.StreamScore
	return node1
}

func TestSetMetrics(t *testing.T) {
	// simple check that node metrics make it through to the load balancing algo
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	c := NewBalancer("", time.Second, time.Second, db)
	err = c.UpdateMembers(context.Background(), []cluster.Member{{Name: "node1", Tags: mediaTags}, {Name: "node2", Tags: mediaTags}})
	require.NoError(t, err)

	setNodeMetrics(t, mock, []NodeUpdateEvent{
		{NodeID: "node1", NodeMetrics: NodeMetrics{CPUUsagePercentage: 90, Timestamp: time.Now()}},
		{NodeID: "node2", NodeMetrics: NodeMetrics{CPUUsagePercentage: 0, Timestamp: time.Now()}},
	})

	node, fullPlaybackID, err := c.GetBestNode(context.Background(), nil, "1234", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, "node2", node)
	require.Equal(t, "video+1234", fullPlaybackID)
}

func TestNoIngestStream(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	c := NewBalancer("", time.Second, time.Second, db)
	// first test no nodes available
	nodeStats := NodeUpdateEvent{NodeID: "id", NodeMetrics: NodeMetrics{Timestamp: time.Now()}}
	nodeStats.SetStreams([]string{"stream"}, nil)
	setNodeMetrics(t, mock, []NodeUpdateEvent{nodeStats})
	source, err := c.MistUtilLoadSource(context.Background(), "stream", "", "")
	require.EqualError(t, err, "catabalancer no node found for ingest stream: stream stale: false")
	require.Empty(t, source)

	// test node present but ingest stream not available
	err = c.UpdateMembers(context.Background(), []cluster.Member{{
		Name: "node",
		Tags: mediaTags,
	}})
	require.NoError(t, err)
	setNodeMetrics(t, mock, []NodeUpdateEvent{nodeStats})
	source, err = c.MistUtilLoadSource(context.Background(), "stream", "", "")
	require.EqualError(t, err, "catabalancer no node found for ingest stream: stream stale: false")
	require.Empty(t, source)
}

func TestMistUtilLoadSource(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	c := NewBalancer("", time.Second, time.Second, db)
	c.nodeStatsCache = cache.New(1*time.Millisecond, time.Minute)
	err = c.UpdateMembers(context.Background(), []cluster.Member{{
		Name: "node",
		Tags: mediaTags,
	}})
	require.NoError(t, err)

	nodeStats := NodeUpdateEvent{NodeID: "node", NodeMetrics: NodeMetrics{Timestamp: time.Now()}}
	nodeStats.SetStreams([]string{"stream"}, []string{"ingest"})
	setNodeMetrics(t, mock, []NodeUpdateEvent{nodeStats})

	source, err := c.MistUtilLoadSource(context.Background(), "ingest", "", "")
	require.NoError(t, err)
	require.Equal(t, "dtsc://node", source)

	time.Sleep(2 * time.Millisecond)
	err = c.UpdateMembers(context.Background(), []cluster.Member{})
	require.NoError(t, err)
	setNodeMetrics(t, mock, []NodeUpdateEvent{})
	source, err = c.MistUtilLoadSource(context.Background(), "ingest", "", "")
	require.EqualError(t, err, "catabalancer no node found for ingest stream: ingest stale: false")
	require.Empty(t, source)
}

func TestStreamTimeout(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	c := NewBalancer("", time.Second, time.Second, db)
	err = c.UpdateMembers(context.Background(), []cluster.Member{{
		Name: "node",
		Tags: mediaTags,
	}})
	require.NoError(t, err)

	c.metricTimeout = 5 * time.Second
	c.ingestStreamTimeout = 5 * time.Second
	nodeStats := NodeUpdateEvent{NodeID: "node", NodeMetrics: NodeMetrics{Timestamp: time.Now()}}
	nodeStats.SetStreams([]string{"video+stream"}, []string{"video+ingest"})
	setNodeMetrics(t, mock, []NodeUpdateEvent{nodeStats})
	s, err := c.refreshNodes()
	require.NoError(t, err)
	setNodeMetrics(t, mock, []NodeUpdateEvent{nodeStats})

	// Source load balance call should work
	source, err := c.MistUtilLoadSource(context.Background(), "video+ingest", "", "")
	require.NoError(t, err)
	require.Equal(t, "dtsc://node", source)
	// Playback load balance calls should work
	nodes := selectTopNodes(c.createScoredNodes(s), "stream", 0, 0, 1)
	require.Equal(t, int64(2), nodes[0].StreamScore)
	nodes = selectTopNodes(c.createScoredNodes(s), "ingest", 0, 0, 1)
	require.Equal(t, int64(2), nodes[0].StreamScore)

	setNodeMetrics(t, mock, []NodeUpdateEvent{nodeStats})
	c.ingestStreamTimeout = -5 * time.Second
	// Re-run the same load balance calls as above, now no results should be returned due to expiry
	source, err = c.MistUtilLoadSource(context.Background(), "video+ingest", "", "")
	require.EqualError(t, err, "catabalancer no node found for ingest stream: video+ingest stale: true")
	require.Empty(t, source)

	c.metricTimeout = -5 * time.Second
	nodes = selectTopNodes(c.createScoredNodes(s), "stream", 0, 0, 1)
	require.Empty(t, nodes)
	nodes = selectTopNodes(c.createScoredNodes(s), "ingest", 0, 0, 1)
	require.Empty(t, nodes)
}

func TestSimulate(t *testing.T) {
	// update these values to test the lock contention with higher numbers of nodes etc
	nodeCount := 1
	streamsPerNode := 2
	expectedResponseTime := 100 * time.Millisecond
	loadBalanceCallCount := 5

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	c := NewBalancer("node0", time.Second, time.Second, db)
	var nodes []cluster.Member
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, cluster.Member{Name: fmt.Sprintf("node%d", i)})
	}
	err = c.UpdateMembers(context.Background(), nodes)
	require.NoError(t, err)

	var s []NodeUpdateEvent
	for i := 0; i < nodeCount; i++ {
		nodeStats := NodeUpdateEvent{NodeID: fmt.Sprintf("node%d", i), NodeMetrics: NodeMetrics{Timestamp: time.Now()}}
		var streams []string
		for k := 0; k < streamsPerNode; k++ {
			streams = append(streams, fmt.Sprintf("stream%d", k))
		}
		nodeStats.SetStreams(streams, nil)
		s = append(s, nodeStats)
	}

	for j := 0; j < loadBalanceCallCount; j++ {
		setNodeMetrics(t, mock, s)
		start := time.Now()
		_, _, err = c.GetBestNode(context.Background(), nil, "playbackID", "0", "0", "", false)
		require.NoError(t, err)
		require.LessOrEqual(t, time.Since(start), expectedResponseTime)
		time.Sleep(10 * time.Millisecond)
	}
}
