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
