package events

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/serf/serf"
	"github.com/livepeer/catalyst-api/balancer/catalyst"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/log"
	"strings"
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

// TODO move this somewhere more appropriate
func StartMetricSending(nodeName string, latitude float64, longitude float64, c cluster.Cluster, mist clients.MistAPIClient) {
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
				Resource: nodeStatsEventResource,
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

			if mist == nil {
				continue
			}

			// send streams event
			mistState, err := mist.GetState()
			if err != nil {
				log.LogNoRequestID("catabalancer failed to get mist state", "err", err)
			}
			streams := make(map[string]catalyst.Stream)
			for playbackID := range mistState.ActiveStreams {
				parts := strings.Split(playbackID, "+")
				if len(parts) == 2 {
					playbackID = parts[1] // take the playbackID after the prefix e.g. 'video+'
				}
				streams[playbackID] = catalyst.Stream{
					ID: playbackID,
				}
			}

			streamsEvent := NodeStreamsEvent{
				Resource: nodeStreamsEventResource,
				NodeID:   nodeName,
				Streams:  streams,
			}
			payload, err = json.Marshal(streamsEvent)
			if err != nil {
				log.LogNoRequestID("catabalancer failed to marhsal node stats", "err", err)
				break
			}

			// TODO may need one message per stream
			// TODO on stream_buffer mist trigger also fire these
			// TODO we can add user count per stream
			err = c.BroadcastEvent(serf.UserEvent{
				Name:    "node-streams",
				Payload: payload,
			})
			if err != nil {
				log.LogNoRequestID("catabalancer failed to send streams info", "err", err)
				break
			}
		}
	}()
}
