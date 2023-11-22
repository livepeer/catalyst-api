package events

import (
	"encoding/json"
	"fmt"

	"github.com/livepeer/catalyst-api/balancer/catalyst"
)

const streamEventResource = "stream"
const nukeEventResource = "nuke"
const nodeStatsEventResource = "nodeStats"
const nodeStreamsEventResource = "nodeStreams"

type Event interface{}

type GenericEvent struct {
	Resource string `json:"resource"`
}

type StreamEvent struct {
	Resource   string `json:"resource"`
	PlaybackID string `json:"playback_id"`
}

type NukeEvent struct {
	Resource   string `json:"resource"`
	PlaybackID string `json:"playback_id"`
}

type NodeStatsEvent struct {
	Resource    string               `json:"resource"`
	NodeID      string               `json:"node_id"`
	NodeMetrics catalyst.NodeMetrics `json:"node_metrics"`
}

type NodeStreamsEvent struct {
	Resource string                     `json:"resource"`
	NodeID   string                     `json:"node_id"`
	Streams  map[string]catalyst.Stream `json:"streams"`
}

func Unmarshal(payload []byte) (Event, error) {
	var generic GenericEvent
	err := json.Unmarshal(payload, &generic)
	if err != nil {
		return nil, err
	}
	switch generic.Resource {
	case streamEventResource:
		event := &StreamEvent{}
		err := json.Unmarshal(payload, event)
		if err != nil {
			return nil, err
		}
		return event, nil
	case nukeEventResource:
		event := &NukeEvent{}
		err := json.Unmarshal(payload, event)
		if err != nil {
			return nil, err
		}
		return event, nil
	case nodeStatsEventResource:
		event := &NodeStatsEvent{}
		err := json.Unmarshal(payload, event)
		if err != nil {
			return nil, err
		}
		return event, nil
	case nodeStreamsEventResource:
		event := &NodeStreamsEvent{}
		err := json.Unmarshal(payload, event)
		if err != nil {
			return nil, err
		}
		return event, nil
	}
	return nil, fmt.Errorf("unable to unmarshal event, unknown resource '%s'", generic.Resource)
}
