package catalyst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItReturnsItselfWhenNoOtherNodesPresent(t *testing.T) {
	// Make the node handling the request unfavourable in terms of stats, to make sure
	// it'll still pick itself if it's the only option
	currentNode := Node{
		CPUUsagePercentage:       100,
		RAMUsagePercentage:       100,
		BandwidthUsagePercentage: 100,
		Streams:                  map[string]Stream{},
	}
	n, err := currentNode.SelectNode([]Node{})
	require.NoError(t, err)
	require.Equal(t, currentNode, n)

}
