package events

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/serf/serf"
	"github.com/livepeer/catalyst-api/balancer/catabalancer"
	"github.com/livepeer/catalyst-api/clients"
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
	Resource    string                   `json:"resource"`
	NodeID      string                   `json:"node_id"`
	NodeMetrics catabalancer.NodeMetrics `json:"node_metrics"`
}

type NodeStreamsEvent struct {
	Resource string `json:"resource"`
	NodeID   string `json:"node_id"`
	Stream   string `json:"stream"`
	IsIngest bool   `json:"is_ingest"`
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

func StartMetricSending(nodeName string, latitude float64, longitude float64, c cluster.Cluster, mist clients.MistAPIClient) {
	ticker := time.NewTicker(catabalancer.UpdateEvery)
	go func() {
		for range ticker.C {
			sysusage, err := catabalancer.GetSystemUsage()
			if err != nil {
				log.LogNoRequestID("catabalancer failed to get sys usage", "err", err)
				continue
			}

			event := NodeStatsEvent{
				Resource: nodeStatsEventResource,
				NodeID:   nodeName,
				NodeMetrics: catabalancer.NodeMetrics{
					CPUUsagePercentage:       sysusage.CPUUsagePercentage,
					RAMUsagePercentage:       sysusage.RAMUsagePercentage,
					BandwidthUsagePercentage: sysusage.BWUsagePercentage,
					LoadAvg:                  sysusage.LoadAvg.Load5Min,
					GeoLatitude:              latitude,
					GeoLongitude:             longitude,
				},
			}
			payload, err := json.Marshal(event)
			if err != nil {
				log.LogNoRequestID("catabalancer failed to marhsal node stats", "err", err)
				continue
			}

			err = c.BroadcastEvent(serf.UserEvent{
				Name:    "node-stats",
				Payload: payload,
			})
			if err != nil {
				log.LogNoRequestID("catabalancer failed to send sys info", "err", err)
				continue
			}
		}
	}()
	streamTicker := time.NewTicker(catabalancer.UpdateEvery)
	go func() {
		for range streamTicker.C {
			if mist == nil {
				continue
			}

			// send streams event
			mistState, err := mist.GetState()
			if err != nil {
				log.LogNoRequestID("catabalancer failed to get mist state", "err", err)
				continue
			}
			for stream := range mistState.ActiveStreams {
				streamsEvent := NodeStreamsEvent{
					Resource: nodeStreamsEventResource,
					NodeID:   nodeName,
					Stream:   stream,
					IsIngest: mistState.IsIngestStream(stream),
				}
				payload, err := json.Marshal(streamsEvent)
				if err != nil {
					log.LogNoRequestID("catabalancer failed to marhsal node stats", "err", err)
					continue
				}

				err = c.BroadcastEvent(serf.UserEvent{
					Name:    "node-streams",
					Payload: payload,
				})
				if err != nil {
					log.LogNoRequestID("catabalancer failed to send streams info", "err", err)
					continue
				}
			}
		}
	}()
}
