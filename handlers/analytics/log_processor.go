package analytics

import (
	"encoding/json"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaBatchInterval  = 1 * time.Second
	kafkaRequestTimeout = 60 * time.Second
)

type ILogProcessor interface {
	Start(ch chan LogData)
}

type LogProcessor struct {
	logs   []LogData
	writer *kafka.Writer
	topic  string
}

type LogDataEvent struct {
	// Heartbeat event
	ID                  *string `json:"id,omitempty"`
	Errors              *int    `json:"errors,omitempty"`
	AutoplayStatus      *string `json:"autoplay_status,omitempty"`
	StalledCount        *int    `json:"stalled_count,omitempty"`
	WaitingCount        *int    `json:"waiting_count,omitempty"`
	TimeWarningMS       *int    `json:"time_warning_ms,omitempty"`
	TimeErroredMS       *int    `json:"time_errored_ms,omitempty"`
	TimeStalledMS       *int    `json:"time_stalled_ms,omitempty"`
	TimePlayingMS       *int    `json:"time_playing_ms,omitempty"`
	TimeWaitingMS       *int    `json:"time_waiting_ms,omitempty"`
	MountToPlayMS       *int    `json:"mount_to_play_ms,omitempty"`
	MountToFirstFrameMS *int    `json:"mount_to_first_frame_ms,omitempty"`
	PlayToFirstFrameMS  *int    `json:"play_to_first_frame_ms,omitempty"`
	DurationMS          *int    `json:"duration_ms,omitempty"`
	OffsetMS            *int    `json:"offset_ms,omitempty"`
	PlayerHeightPX      *int    `json:"player_height_px,omitempty"`
	PlayerWidthPX       *int    `json:"player_width_px,omitempty"`
	VideoHeightPX       *int    `json:"video_height_px,omitempty"`
	VideoWidthPX        *int    `json:"video_width_px,omitempty"`
	WindowHeightPX      *int    `json:"window_height_px,omitempty"`
	WindowWidthPX       *int    `json:"window_width_px,omitempty"`

	// Error event
	Message  *string `json:"message,omitempty"`
	Category *string `json:"category,omitempty"`
}

type LogData struct {
	SessionID             string       `json:"session_id"`
	ServerTimestamp       int64        `json:"server_timestamp"`
	PlaybackID            string       `json:"playback_id"`
	ViewerHash            string       `json:"viewer_hash"`
	Protocol              string       `json:"protocol"`
	Domain                string       `json:"domain"`
	Path                  string       `json:"path"`
	Params                string       `json:"params"`
	Hash                  string       `json:"hash"`
	SourceURL             string       `json:"source_url"`
	Player                string       `json:"player"`
	Version               string       `json:"version"`
	UserID                string       `json:"user_id"`
	ProjectID             string       `json:"project_id"`
	DStorageURL           string       `json:"d_storage_url"`
	Source                string       `json:"source"`
	CreatorID             string       `json:"creator_id"`
	DeviceType            string       `json:"device_type"`
	DeviceModel           string       `json:"device_model"`
	DeviceBrand           string       `json:"device_brand"`
	Browser               string       `json:"browser"`
	OS                    string       `json:"os"`
	PlaybackGeoHash       string       `json:"playback_geo_hash"`
	PlaybackContinentName string       `json:"playback_continent_name"`
	PlaybackCountryCode   string       `json:"playback_country_code"`
	PlaybackCountryName   string       `json:"playback_country_name"`
	PlaybackSubdivision   string       `json:"playback_subdivision_name"`
	PlaybackTimezone      string       `json:"playback_timezone"`
	EventType             string       `json:"event_type"`
	EventTimestamp        int64        `json:"event_timestamp"`
	EventData             LogDataEvent `json:"event_data"`
}

type KafkaKey struct {
	SessionID string `json:"session_id"`
	EventType string `json:"event_type"`
}

func NewLogProcessor(bootstrapServers, user, password, topic string) *LogProcessor {
	writer := newWriter(bootstrapServers, user, password, topic)
	return &LogProcessor{
		logs:   []LogData{},
		writer: writer,
		topic:  topic,
	}
}

// Start starts LogProcessor which sends events to Kafka in batches.
func (lp *LogProcessor) Start(ch chan LogData) {
	t := time.NewTicker(kafkaBatchInterval)
	go func() {
		for {
			select {
			case d := <-ch:
				lp.processLog(d)
			case <-t.C:
				lp.sendEvents()
			}
		}
	}()
}

func (p *LogProcessor) processLog(d LogData) {
	updateMetrics(d)
	p.logs = append(p.logs, d)
}

func updateMetrics(d LogData) {
	if d.EventType != "heartbeat" {
		return
	}
	metrics.Metrics.AnalyticsMetrics.AnalyticsLogsPlaytimeMs.
		WithLabelValues(d.PlaybackID, d.UserID, d.ProjectID, d.PlaybackContinentName).
		Observe(float64(*d.EventData.TimePlayingMS))
	metrics.Metrics.AnalyticsMetrics.AnalyticsLogsBufferTimeMs.
		WithLabelValues(d.PlaybackID, d.UserID, d.ProjectID, d.PlaybackContinentName).
		Observe(float64(*d.EventData.TimeStalledMS + *d.EventData.TimeWaitingMS))
}

func (p *LogProcessor) sendEvents() {
	defer logWriteMetrics(p.writer)

	if len(p.logs) > 0 {
		glog.Infof("sending analytics logs, count=%d", len(p.logs))
	} else {
		glog.V(6).Info("no analytics logs, skip sending")
		return
	}

	var msgs []kafka.Message
	for _, d := range p.logs {
		key, err := json.Marshal(KafkaKey{SessionID: d.SessionID, EventType: d.EventType})
		if err != nil {
			glog.Errorf("invalid analytics log event, cannot create Kafka key, sessionID=%s, err=%v", d.SessionID, err)
			continue
		}
		value, err := json.Marshal(d)
		if err != nil {
			glog.Errorf("invalid analytics log event, cannot sent to Kafka, sessionID=%s, err=%v", d.SessionID, err)
			continue
		}
		msgs = append(msgs, kafka.Message{
			Key:   key,
			Value: value,
		})
	}
	p.logs = []LogData{}

	sendWithRetries(p.writer, msgs)
}
