package catalyst

import (
	"fmt"
	"math"
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

// Earth radius in kilometers
const earthRadius = 6371

// ToRadians converts degrees to radians
func toRadians(deg float64) float64 {
	return deg * (math.Pi / 180)
}

type Stream struct {
	ID string
}

type ScoredNode struct {
	Score       int64
	GeoScore    int64
	GeoDistance float64
	Node
}

func SelectNode(nodes []Node, requestLatitude, requestLongitude float64) (Node, error) {
	if len(nodes) == 0 {
		return Node{}, fmt.Errorf("no nodes to select from")
	}

	return Node{}, fmt.Errorf("not yet implemented")
}

func selectTopNodes(nodes []Node, requestLatitude, requestLongitude float64, numNodes int) []ScoredNode {
	var scoredNodes []ScoredNode
	for _, node := range nodes {
		scoredNodes = append(scoredNodes, ScoredNode{Node: node, Score: 0})
	}

	// geoScores(scoredNodes, requestLatitude, requestLongitude)

	// Is Local?
	// Order all nodes by closest geo distance
	// Take distance of closest one and mark any within X distance of it as "Good"
	// Take distance of next one and mark any within X distance of it as "Okay"
	// Mark any remaining as "Bad"

	// Good / Okay / Bad
	// Has Stream and Is Local and Isn't Overloaded
	// Is Local and Isn't Overloaded
	// Has

	// Has Stream?
	// CPU Okay?
	// RAM Okay?
	// Bandwidth Okay?
	// Geo Good?

	sort.Slice(scoredNodes, func(i, j int) bool {
		return scoredNodes[i].Score < scoredNodes[j].Score
	})

	return scoredNodes[:numNodes]
}
