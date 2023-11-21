package catalyst

import (
	"fmt"
	"math/rand"
	"sort"
)

// TODO: This is temporary until we have the real struct definition
type Node struct {
	ID                       string
	Streams                  map[string]Stream // Stream ID -> Stream
	CPUUsagePercentage       int64
	RAMUsagePercentage       int64
	BandwidthUsagePercentage int64
	GeoLatitude              float64
	GeoLongitude             float64
}

type Stream struct {
	ID string
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
