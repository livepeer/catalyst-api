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
)

type CataBalancer struct {
	Cluster       cluster.Cluster
	Nodes         map[string]*Node
	nodesLock     sync.Mutex
	Streams       map[string]Streams     // Node name -> Streams
	IngestStreams map[string]Streams     // Node name -> Streams
	NodeMetrics   map[string]NodeMetrics // Node name -> NodeMetrics
}

type Streams map[string]Stream // Stream ID -> Stream

// TODO: This is temporary until we have the real struct definition
type Node struct {
	Name    string
	DTSCTag string
}

type Stream struct {
	ID string
}

type NodeMetrics struct {
	CPUUsagePercentage       float64
	RAMUsagePercentage       float64
	BandwidthUsagePercentage float64
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

func NewBalancer(c cluster.Cluster) *CataBalancer {
	return &CataBalancer{
		Cluster: c,
		Nodes:   make(map[string]*Node),
	}
}

func (c *CataBalancer) Start(ctx context.Context) error {
	return nil
}

func (c *CataBalancer) UpdateMembers(ctx context.Context, members []cluster.Member) error {
	if len(c.Nodes) > 0 {
		return nil
	}

	// I'm assuming UpdateMembers can be called concurrently, so we need to lock while updating the nodes map
	c.nodesLock.Lock()
	defer c.nodesLock.Unlock()

	latestNodes := make(map[string]*Node)
	for _, member := range members {
		if _, ok := c.Nodes[member.Name]; !ok {
			latestNodes[member.Name] = &Node{
				Name:    member.Name,
				DTSCTag: member.Tags["dtsc"],
			}
		}
	}

	// remove stream data for nodes no longer present

	// remove metric data for nodes no longer present

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
	_, ok := n.Streams[streamID]
	if ok {
		log.LogNoRequestID("catabalancer found stream on node", "node", n.Name, "streamid", streamID)
	}
	return ok
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
		if c.IngestStreams[node.Name] != nil {
			if _, ok := c.IngestStreams[node.Name][stream]; ok {
				return "dtsc://" + node.DTSCTag, nil
			}
		}
	}
	return "", fmt.Errorf("no node found for ingest stream: %s", stream)
}

func (c *CataBalancer) UpdateNodes(id string, nodeMetrics NodeMetrics) {
	log.LogNoRequestID("catabalancer updatenodes", "id", id, "ram", nodeMetrics.RAMUsagePercentage, "cpu", nodeMetrics.CPUUsagePercentage)
	if _, ok := c.Nodes[id]; !ok {
		log.LogNoRequestID("catabalancer updatenodes node not found", "id", id)
		return
	}
	c.NodeMetrics[id] = nodeMetrics
}

func (c *CataBalancer) UpdateStreams(id string, stream string, isIngest bool) {
	if _, ok := c.Nodes[id]; !ok {
		log.LogNoRequestID("catabalancer updatestreams node not found", "id", id)
		return
	}
	if isIngest {
		if c.IngestStreams[id] == nil {
			c.IngestStreams[id] = make(Streams)
		}
		c.IngestStreams[id][stream] = Stream{ID: stream}
		return
	}
	if c.Streams[id] == nil {
		c.Streams[id] = make(Streams)
	}
	c.Streams[id][stream] = Stream{ID: stream}
}
