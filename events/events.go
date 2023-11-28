package events

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/serf/serf"
	"github.com/livepeer/catalyst-api/balancer/catalyst"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/log"
	"time"
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
	Resource      string               `json:"resource"`
	NodeID        string               `json:"node_id"`
	NodeMetrics   catalyst.NodeMetrics `json:"node_metrics"`
	NodeLatitude  float64              `json:"node_latitude"`
	NodeLongitude float64              `json:"node_longitude"`
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

func StartMetricSending(nodeName string, latitude float64, longitude float64, c cluster.Cluster) {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			log.LogNoRequestID("catabalancer sending node stats")
			sysinfo, err := catalyst.GetSystemInfo()
			if err != nil {
				log.LogNoRequestID("catabalancer failed to get sys info", "err", err)
				break
			}

			event := NodeStatsEvent{
				Resource: "nodeStats",
				NodeID:   nodeName,
				NodeMetrics: catalyst.NodeMetrics{
					CPUUsagePercentage:       sysinfo.CPUUsagePercentage,
					RAMUsagePercentage:       sysinfo.RAMUsagePercentage,
					BandwidthUsagePercentage: sysinfo.BandwidthUsagePercentage,
				},
				NodeLatitude:  latitude,
				NodeLongitude: longitude,
			}
			payload, err := json.Marshal(event)
			if err != nil {
				log.LogNoRequestID("catabalancer failed to marhsal node stats", "err", err)
				break
			}

			err = c.BroadcastEvent(serf.UserEvent{
				Name:    "node-stats",
				Payload: payload,
			})
			if err != nil {
				log.LogNoRequestID("catabalancer failed to send sys info", "err", err)
				break
			}
		}
	}()
}
