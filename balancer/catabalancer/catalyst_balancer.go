package catabalancer

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
	NodeName      string // Node name of this instance
	Nodes         map[string]*Node
	nodesLock     sync.Mutex
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
	StreamScore int64
	GeoDistance float64
	Node
	Streams       Streams
	IngestStreams Streams
	NodeMetrics
}

func (s ScoredNode) String() string {
	return fmt.Sprintf("(Name:%s Score:%d GeoScore:%d StreamScore:%d GeoDistance:%.2f Lat:%.2f Lon:%.2f CPU:%.2f RAM:%.2f BW:%.2f)",
		s.Name,
		s.Score,
		s.GeoScore,
		s.StreamScore,
		s.GeoDistance,
		s.GeoLatitude,
		s.GeoLongitude,
		s.CPUUsagePercentage,
		s.RAMUsagePercentage,
		s.BandwidthUsagePercentage,
	)
}

func NewBalancer(nodeName string) *CataBalancer {
	return &CataBalancer{
		NodeName:      nodeName,
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
	c.nodesLock.Lock()
	defer c.nodesLock.Unlock()

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

	// default to ourself if there are no other nodes
	nodeName := c.NodeName

	if len(c.Nodes) > 0 {
		node, err := SelectNode(c.createScoredNodes(), playbackID, latf, lonf)
		if err != nil {
			return "", "", err
		}
		nodeName = node.Name
	} else {
		log.LogNoRequestID("catabalancer no nodes found, choosing myself", "chosenNode", nodeName, "streamID", playbackID, "reqLat", lat, "reqLon", lon)
	}

	return nodeName, "video+" + playbackID, nil
}

func (c *CataBalancer) createScoredNodes() []ScoredNode {
	c.nodesLock.Lock()
	defer c.nodesLock.Unlock()
	var nodesList []ScoredNode
	for nodeName, node := range c.Nodes {
		if _, ok := c.NodeMetrics[nodeName]; !ok {
			continue
		}
		nodesList = append(nodesList, ScoredNode{
			Node:        *node,
			Streams:     c.Streams[nodeName],
			NodeMetrics: c.NodeMetrics[nodeName],
		})
	}
	return nodesList
}

func (n ScoredNode) HasStream(streamID string) bool {
	s, ok := n.Streams[streamID]
	found := ok && !isStreamExpired(s)
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

	if len(topNodes) == 0 {
		return Node{}, fmt.Errorf("selectTopNodes returned no nodes")
	}
	chosen := topNodes[rand.Intn(len(topNodes))].Node
	log.LogNoRequestID("catabalancer select nodes", "chosenNode", chosen.Name, "topNodes", topNodes, "streamID", streamID, "reqLat", requestLatitude, "reqLon", requestLongitude)
	return chosen, nil
}

func selectTopNodes(scoredNodes []ScoredNode, streamID string, requestLatitude, requestLongitude float64, numNodes int) []ScoredNode {
	scoredNodes = geoScores(scoredNodes, requestLatitude, requestLongitude)

	// 1. Has Stream and Is Local and Isn't Overloaded
	localHasStreamNotOverloaded := []ScoredNode{}
	for _, node := range scoredNodes {
		if node.GeoScore == 2 && node.HasStream(streamID) && node.GetLoadScore() == 2 {
			node.StreamScore = 2
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
			node.StreamScore = 2
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
	c.nodesLock.Lock()
	defer c.nodesLock.Unlock()
	for _, node := range c.Nodes {
		if s, ok := c.IngestStreams[node.Name][stream]; ok && !isStreamExpired(s) {
			return node.DTSC, nil
		}
	}
	return "", fmt.Errorf("no node found for ingest stream: %s", stream)
}

func (c *CataBalancer) UpdateNodes(id string, nodeMetrics NodeMetrics) {
	c.nodesLock.Lock()
	defer c.nodesLock.Unlock()
	log.LogNoRequestID("catabalancer updatenodes", "id", id, "ram", nodeMetrics.RAMUsagePercentage, "cpu", nodeMetrics.CPUUsagePercentage)
	if _, ok := c.Nodes[id]; !ok {
		log.LogNoRequestID("catabalancer updatenodes node not found", "id", id)
		return
	}
	c.NodeMetrics[id] = nodeMetrics
}

var streamTimeout = 5 * time.Second // should match how often we send the update messages

func (c *CataBalancer) UpdateStreams(nodeName string, streamID string, isIngest bool) {
	c.nodesLock.Lock()
	defer c.nodesLock.Unlock()
	if _, ok := c.Nodes[nodeName]; !ok {
		log.LogNoRequestID("catabalancer UpdateStreams node not found", "nodeName", nodeName)
		return
	}
	// remove old streams
	removeOldStreams(c.Streams[nodeName])
	removeOldStreams(c.IngestStreams[nodeName])

	if isIngest {
		if c.IngestStreams[nodeName] == nil {
			c.IngestStreams[nodeName] = make(Streams)
		}
		c.IngestStreams[nodeName][streamID] = Stream{ID: streamID, LastSeen: time.Now()}
		return
	}
	if c.Streams[nodeName] == nil {
		c.Streams[nodeName] = make(Streams)
	}
	c.Streams[nodeName][streamID] = Stream{ID: streamID, LastSeen: time.Now()}
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
