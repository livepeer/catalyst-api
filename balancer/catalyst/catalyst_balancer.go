package catalyst

import (
	"context"
	"fmt"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/log"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)

type CataBalancer struct {
	Nodes         map[string]*Node
	metricsLock   sync.Mutex
	streamsLock   sync.Mutex
	Streams       map[string]Streams     // Node name -> Streams
	IngestStreams map[string]Streams     // Node name -> Streams
	NodeMetrics   map[string]NodeMetrics // Node name -> NodeMetrics
}

type Streams map[string]Stream // Stream ID -> Stream

type Node struct {
	Name string
	DTSC string
}

type Stream struct {
	ID       string
	LastSeen time.Time // last time we saw a stream message for this stream, old streams can be removed on a timeout
}

type NodeMetrics struct {
	CPUUsagePercentage       float64
	RAMUsagePercentage       float64
	BandwidthUsagePercentage float64
	LoadAvg                  float64
	GeoLatitude              float64
	GeoLongitude             float64
}

// All of the scores are in the range 0-2, where:
// 2 = Good
// 1 = Okay
// 0 = Bad
type ScoredNode struct {
	Score       int64
	GeoScore    int64
	GeoDistance float64
	Node
	Streams       Streams
	IngestStreams Streams
	NodeMetrics
}

func NewBalancer() *CataBalancer {
	return &CataBalancer{
		Nodes:         make(map[string]*Node),
		Streams:       make(map[string]Streams),
		IngestStreams: make(map[string]Streams),
		NodeMetrics:   make(map[string]NodeMetrics),
	}
}

func (c *CataBalancer) Start(ctx context.Context) error {
	return nil
}

func (c *CataBalancer) UpdateMembers(ctx context.Context, members []cluster.Member) error {
	log.LogNoRequestID("catabalancer UpdateMembers", "members", fmt.Sprintf("%+v", members))
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	c.streamsLock.Lock()
	defer c.streamsLock.Unlock()

	latestNodes := make(map[string]*Node)
	for _, member := range members {
		latestNodes[member.Name] = &Node{
			Name: member.Name,
			DTSC: member.Tags["dtsc"],
		}
	}

	// remove stream data for nodes no longer present
	for nodeName := range c.Streams {
		if _, ok := latestNodes[nodeName]; !ok {
			delete(c.Streams, nodeName)
		}
	}
	for nodeName := range c.IngestStreams {
		if _, ok := latestNodes[nodeName]; !ok {
			delete(c.IngestStreams, nodeName)
		}
	}

	// remove metric data for nodes no longer present
	for nodeName := range c.NodeMetrics {
		if _, ok := latestNodes[nodeName]; !ok {
			delete(c.NodeMetrics, nodeName)
		}
	}

	c.Nodes = latestNodes
	return nil
}

func (c *CataBalancer) GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string) (string, string, error) {
	var err error
	latf := 0.0
	if lat != "" {
		latf, err = strconv.ParseFloat(lat, 64)
		if err != nil {
			return "", "", err
		}
	}
	lonf := 0.0
	if lon != "" {
		lonf, err = strconv.ParseFloat(lon, 64)
		if err != nil {
			return "", "", err
		}
	}
	var nodesList []ScoredNode
	for nodeName, node := range c.Nodes {
		nodesList = append(nodesList, ScoredNode{
			Node:        *node,
			Streams:     c.Streams[nodeName],
			NodeMetrics: c.NodeMetrics[nodeName],
		})
	}
	node, err := SelectNode(nodesList, playbackID, latf, lonf)
	if err != nil {
		return "", "", err
	}
	// TODO video+ is hard coded
	return node.Name, "video+" + playbackID, nil
}

func (n ScoredNode) HasStream(streamID string) bool {
	s, ok := n.Streams[streamID]
	found := ok && !isStreamExpired(s)
	if found {
		log.LogNoRequestID("catabalancer found stream on node", "node", n.Name, "streamid", streamID)
	}
	return found
}

func (n ScoredNode) GetLoadScore() int {
	if n.CPUUsagePercentage > 85 || n.BandwidthUsagePercentage > 85 || n.RAMUsagePercentage > 85 {
		return 0
	}
	if n.CPUUsagePercentage > 50 || n.BandwidthUsagePercentage > 50 || n.RAMUsagePercentage > 50 {
		return 1
	}
	return 2
}

func SelectNode(nodes []ScoredNode, streamID string, requestLatitude, requestLongitude float64) (Node, error) {
	if len(nodes) == 0 {
		return Node{}, fmt.Errorf("no nodes to select from")
	}

	topNodes := selectTopNodes(nodes, streamID, requestLatitude, requestLongitude, 3)
	// TODO figure out what logging we need
	log.LogNoRequestID("catabalancer select nodes", "topNodes", topNodes)
	// TODO return error if none found?
	return topNodes[rand.Intn(len(topNodes))].Node, nil
}

func selectTopNodes(scoredNodes []ScoredNode, streamID string, requestLatitude, requestLongitude float64, numNodes int) []ScoredNode {
	scoredNodes = geoScores(scoredNodes, requestLatitude, requestLongitude)

	// 1. Has Stream and Is Local and Isn't Overloaded
	localHasStreamNotOverloaded := []ScoredNode{}
	for _, node := range scoredNodes {
		if node.GeoScore == 2 && node.HasStream(streamID) && node.GetLoadScore() == 2 {
			localHasStreamNotOverloaded = append(localHasStreamNotOverloaded, node)
		}
	}
	if len(localHasStreamNotOverloaded) > 0 { // TODO: Should this be > 1 or > 2 so that we can ensure there's always some randomness?
		return localHasStreamNotOverloaded
	}

	// 2. Is Local and Isn't Overloaded
	localNotOverloaded := []ScoredNode{}
	for _, node := range scoredNodes {
		if node.GeoScore == 2 && node.GetLoadScore() == 2 {
			localNotOverloaded = append(localNotOverloaded, node)
		}
	}
	if len(localNotOverloaded) > 0 { // TODO: Should this be > 1 or > 2 so that we can ensure there's always some randomness?
		return localNotOverloaded
	}

	// 3. Weighted least-bad option
	for i, node := range scoredNodes {
		node.Score += node.GeoScore
		node.Score += int64(node.GetLoadScore())
		if node.HasStream(streamID) {
			node.Score += 2
		}
		scoredNodes[i] = node
	}

	sort.Slice(scoredNodes, func(i, j int) bool {
		return scoredNodes[i].Score > scoredNodes[j].Score
	})
	if len(scoredNodes) < numNodes {
		return scoredNodes
	}

	return scoredNodes[:numNodes]
}

func (c *CataBalancer) MistUtilLoadSource(ctx context.Context, stream, lat, lon string) (string, error) {
	for _, node := range c.Nodes {
		if s, ok := c.IngestStreams[node.Name][stream]; ok && !isStreamExpired(s) {
			return node.DTSC, nil
		}
	}
	return "", fmt.Errorf("no node found for ingest stream: %s", stream)
}

func (c *CataBalancer) UpdateNodes(id string, nodeMetrics NodeMetrics) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()
	log.LogNoRequestID("catabalancer updatenodes", "id", id, "ram", nodeMetrics.RAMUsagePercentage, "cpu", nodeMetrics.CPUUsagePercentage)
	if _, ok := c.Nodes[id]; !ok {
		log.LogNoRequestID("catabalancer updatenodes node not found", "id", id)
		return
	}
	c.NodeMetrics[id] = nodeMetrics
}

var streamTimeout = 5 * time.Second // should match how often we send the update messages

func (c *CataBalancer) UpdateStreams(id string, streamID string, isIngest bool) {
	c.streamsLock.Lock()
	defer c.streamsLock.Unlock()
	if _, ok := c.Nodes[id]; !ok {
		log.LogNoRequestID("catabalancer updatestreams node not found", "id", id)
		return
	}
	// remove old streams
	removeOldStreams(c.Streams[id])
	removeOldStreams(c.IngestStreams[id])

	if isIngest {
		if c.IngestStreams[id] == nil {
			c.IngestStreams[id] = make(Streams)
		}
		c.IngestStreams[id][streamID] = Stream{ID: streamID, LastSeen: time.Now()}
		return
	}
	if c.Streams[id] == nil {
		c.Streams[id] = make(Streams)
	}
	c.Streams[id][streamID] = Stream{ID: streamID, LastSeen: time.Now()}
}

func isStreamExpired(stream Stream) bool {
	return time.Since(stream.LastSeen) >= streamTimeout
}

func removeOldStreams(streams Streams) {
	for s, stream := range streams {
		if isStreamExpired(stream) {
			delete(streams, s)
		}
	}
}
