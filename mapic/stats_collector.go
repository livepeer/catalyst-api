package mistapiconnector

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/clients"
	census "github.com/livepeer/catalyst-api/mapic/metrics"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	"golang.org/x/sync/errgroup"
)

const lastSeenBumpPeriod = 30 * time.Second

type infoProvider interface {
	getStreamInfo(mistID string) (*streamInfo, error)
	wildcardPlaybackID(stream *api.Stream) string
}

type metricsCollector struct {
	nodeID, ownRegion string
	mist              clients.MistAPIClient
	lapi              *api.Client
	producer          event.AMQPProducer
	amqpExchange      string
	infoProvider
}

func createMetricsCollector(nodeID, ownRegion string, mapi clients.MistAPIClient, lapi *api.Client, producer event.AMQPProducer, amqpExchange string, infop infoProvider) *metricsCollector {
	mc := &metricsCollector{nodeID, ownRegion, mapi, lapi, producer, amqpExchange, infop}
	return mc
}

func (c *metricsCollector) collectMetricsLogged(ctx context.Context, timeout time.Duration, mistState clients.MistState) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.collectMetrics(ctx, mistState); err != nil {
		glog.Errorf("Error collecting mist metrics. err=%v", err)
	}
}

func (c *metricsCollector) collectMetrics(ctx context.Context, mistState clients.MistState) error {
	defer func() {
		if rec := recover(); rec != nil {
			glog.Errorf("Panic in metrics collector. value=%v", rec)
		}
	}()

	streamsMetrics := compileStreamMetrics(&mistState)

	eg := errgroup.Group{}
	eg.SetLimit(5)

	for streamID, metrics := range streamsMetrics {
		if err := ctx.Err(); err != nil {
			return err
		}
		if streamID == "" {
			continue
		}
		streamID, metrics := streamID, metrics

		info, err := c.getStreamInfo(streamID)
		if err != nil {
			glog.Errorf("Error getting stream info for streamId=%s err=%q", streamID, err)
			continue
		}

		stream := c.infoProvider.wildcardPlaybackID(info.stream)
		isIngest := isIngestStream(stream, info, mistState)

		if !isIngest {
			glog.V(8).Infof("Skipping non-ingest stream. streamId=%q", streamID)
			continue
		}

		eg.Go(recovered(func() {
			info.mu.Lock()
			timeSinceBumped := time.Since(info.lastSeenBumpedAt)
			info.mu.Unlock()
			if timeSinceBumped <= lastSeenBumpPeriod {
				return
			}

			if err := c.lapi.Heartbeat(info.stream.ID); err != nil {
				glog.Errorf("Error updating stream last seen. err=%q streamId=%q", err, info.stream.ID)
				return
			}

			info.mu.Lock()
			info.lastSeenBumpedAt = time.Now()
			info.mu.Unlock()
		}))

		eg.Go(recovered(func() {
			mseEvent := createMetricsEvent(c.nodeID, c.ownRegion, info, metrics)
			err = c.producer.Publish(ctx, event.AMQPMessage{
				Exchange: c.amqpExchange,
				Key:      fmt.Sprintf("stream.metrics.%s", info.stream.ID),
				Body:     mseEvent,
			})
			if err != nil {
				glog.Errorf("Error sending mist stream metrics event. err=%q streamId=%q event=%+v", err, info.stream.ID, mseEvent)
			}
		}))
	}

	return eg.Wait()
}

func createMetricsEvent(nodeID, region string, info *streamInfo, metrics *streamMetrics) *data.MediaServerMetricsEvent {
	info.mu.Lock()
	defer info.mu.Unlock()
	multistream := []*data.MultistreamTargetMetrics{}
	for _, push := range metrics.pushes {
		pushInfo := info.pushStatus[push.OriginalURL]
		if pushInfo == nil {
			glog.Infof("Mist exported metrics from unknown push. streamId=%q pushURL=%q", info.id, push.OriginalURL)
			continue
		}
		var metrics *data.MultistreamMetrics
		if push.Stats != nil {
			metrics = &data.MultistreamMetrics{
				ActiveSec:   push.Stats.ActiveSeconds,
				Bytes:       push.Stats.Bytes,
				MediaTimeMs: push.Stats.MediaTime,
			}
			if last := pushInfo.metrics; last != nil {
				if metrics.Bytes > last.Bytes {
					census.IncMultistreamBytes(metrics.Bytes-last.Bytes, info.stream.PlaybackID) // manifestID === playbackID
				}
				if metrics.MediaTimeMs > last.MediaTimeMs {
					diff := time.Duration(metrics.MediaTimeMs-last.MediaTimeMs) * time.Millisecond
					census.IncMultistreamTime(diff, info.stream.PlaybackID)
				}
			}
			pushInfo.metrics = metrics
		}
		multistream = append(multistream, &data.MultistreamTargetMetrics{
			Target:  pushToMultistreamTargetInfo(pushInfo),
			Metrics: metrics,
		})
	}
	var stream *data.StreamMetrics
	if ss := metrics.stream; ss != nil {
		stream = &data.StreamMetrics{}
		// mediatime comes as -1 when not available
		if ss.MediaTimeMs >= 0 {
			stream.MediaTimeMs = &ss.MediaTimeMs
		}
	}
	return data.NewMediaServerMetricsEvent(nodeID, region, info.stream.ID, stream, multistream)
}

// streamMetrics aggregates all the data collected from Mist about a specific
// stream. Mist calls these values stats, but we use them as a single entry and
// will create analytics across multiple observations. So they are more like
// metrics for our infrastrucutre and that's what we call them from here on.
type streamMetrics struct {
	stream *clients.MistStreamStats
	pushes []*clients.MistPush
}

func compileStreamMetrics(mistStats *clients.MistState) map[string]*streamMetrics {
	streamsMetrics := map[string]*streamMetrics{}
	getOrCreate := func(stream string) *streamMetrics {
		if metrics, ok := streamsMetrics[stream]; ok {
			return metrics
		}
		metrics := &streamMetrics{}
		streamsMetrics[stream] = metrics
		return metrics
	}

	for stream, stats := range mistStats.StreamsStats {
		getOrCreate(stream).stream = stats
	}
	for _, push := range mistStats.PushList {
		info := getOrCreate(push.Stream)
		info.pushes = append(info.pushes, push)
	}
	return streamsMetrics
}

func recovered(f func()) func() error {
	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				glog.Errorf("Panic in metrics collector. value=%v stack=%s", r, debug.Stack())
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		f()
		return nil
	}
}
