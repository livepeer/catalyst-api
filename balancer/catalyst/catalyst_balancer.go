package catalyst

import "fmt"

// TODO: This is temporary until we have the real struct definition
type Node struct {
	Streams                  map[string]Stream // Stream ID -> Stream
	CPUUsagePercentage       int64
	RAMUsagePercentage       int64
	BandwidthUsagePercentage int64
}
type Stream struct {
	ID string
}

func (n Node) SelectNode(nodes []Node) (Node, error) {
	if len(nodes) == 0 {
		return n, nil
	}
	return Node{}, fmt.Errorf("not yet implemented")
}
