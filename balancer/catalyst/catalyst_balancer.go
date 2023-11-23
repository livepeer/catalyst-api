package catalyst

import (
	"context"
	"fmt"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/log"
	"math/rand"
	"sort"
	"strconv"
)

type CataBalancer struct {
	Cluster cluster.Cluster
	Nodes   []Node
}

func (c *CataBalancer) Start(ctx context.Context) error {
	return nil
}

func (c *CataBalancer) UpdateMembers(ctx context.Context, members []cluster.Member) error {
	fmt.Println("catabalancer update members ", members)
	if len(c.Nodes) > 0 {
		return nil
	}
	// TODO rather than only run once let this update nodes list, removing and adding new ones
	for _, member := range members {
		c.Nodes = append(c.Nodes, Node{
			ID: member.Name,
		})
	}
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
	node, err := SelectNode(c.Nodes, playbackID, latf, lonf)
	if err != nil {
		return "", "", err
	}
	return node.ID, "video+" + playbackID, nil
}

func (c *CataBalancer) MistUtilLoadBalance(ctx context.Context, stream, lat, lon string) (string, error) {
	return "", nil
}

func (c *CataBalancer) MistUtilLoadSource(ctx context.Context, stream, lat, lon string) (string, error) {
	return "", nil
}

func (c *CataBalancer) MistUtilLoadStreamStats(ctx context.Context, stream string) error {
	return nil
}

func NewBalancer(c cluster.Cluster) *CataBalancer {

	return &CataBalancer{
		Cluster: c,
	}
}

func (c *CataBalancer) UpdateNodes(id string, nodeMetrics NodeMetrics) {
	// TODO change c.Nodes to a map so that we don't have to worry about the size of the slice changing?
	log.LogNoRequestID("catabalancer updatenodes", "id", id, "ram", nodeMetrics.RAMUsagePercentage, "cpu", nodeMetrics.CPUUsagePercentage)
	for i := range c.Nodes {
		if c.Nodes[i].ID == id {
			c.Nodes[i].NodeMetrics = nodeMetrics
			break
		}
	}
}

func (c *CataBalancer) UpdateStreams(id string, streams map[string]Stream) {
	for i := range c.Nodes {
		if c.Nodes[i].ID == id {
			c.Nodes[i].Streams = streams
			break
		}
	}
}

// TODO: This is temporary until we have the real struct definition
type Node struct {
	ID      string
	Streams map[string]Stream // Stream ID -> Stream
	NodeMetrics
	GeoLatitude  float64
	GeoLongitude float64
}

type Stream struct {
	ID string
}

type NodeMetrics struct {
	CPUUsagePercentage       int64
	RAMUsagePercentage       int64
	BandwidthUsagePercentage int64
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
}

func (n Node) HasStream(streamID string) bool {
	_, ok := n.Streams[streamID]
	return ok
}

func (n Node) GetLoadScore() int {
	if n.CPUUsagePercentage > 85 || n.BandwidthUsagePercentage > 85 || n.RAMUsagePercentage > 85 {
		return 0
	}
	if n.CPUUsagePercentage > 50 || n.BandwidthUsagePercentage > 50 || n.RAMUsagePercentage > 50 {
		return 1
	}
	return 2
}

func SelectNode(nodes []Node, streamID string, requestLatitude, requestLongitude float64) (Node, error) {
	if len(nodes) == 0 {
		return Node{}, fmt.Errorf("no nodes to select from")
	}

	topNodes := selectTopNodes(nodes, streamID, requestLatitude, requestLongitude, 3)
	log.LogNoRequestID("catabalancer select nodes", "topNodes", topNodes)
	return topNodes[rand.Intn(len(topNodes))].Node, nil
}

func selectTopNodes(nodes []Node, streamID string, requestLatitude, requestLongitude float64, numNodes int) []ScoredNode {
	var scoredNodes []ScoredNode
	for _, node := range nodes {
		scoredNodes = append(scoredNodes, ScoredNode{Node: node, Score: 0})
	}

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
