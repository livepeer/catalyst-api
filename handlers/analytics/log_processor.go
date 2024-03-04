package analytics

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/golang/glog"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"time"
)

type ILogProcessor interface {
	Start(ch chan LogData)
}

type LogProcessor struct {
	writer *kafka.Writer
	topic  string
}

type LogDataEvent struct {
	EventType      string `json:"event_type"`
	EventTimestamp int64  `json:"event_timestamp"`

	// Heartbeat Event
	Errors         int    `json:"errors,omitempty"`
	PlaytimeMS     int    `json:"playtime_ms,omitempty"`
	TTFFMS         int    `json:"ttff_ms,omitempty"`
	PreloadTimeMS  int    `json:"preload_time_ms,omitempty"`
	AutoplayStatus string `json:"autoplay_status,omitempty"`
	BufferMS       int    `json:"buffer_ms,omitempty"`

	// Error Event
	ErrorMessage string `json:"error_message,omitempty"`
}

type LogData struct {
	SessionID             string       `json:"session_id"`
	ServerTimestamp       int64        `json:"server_timestamp"`
	PlaybackID            string       `json:"playback_id"`
	ViewerHash            string       `json:"viewer_hash"`
	Protocol              string       `json:"protocol"`
	PageURL               string       `json:"page_url"`
	SourceURL             string       `json:"source_url"`
	Player                string       `json:"player"`
	UserID                string       `json:"user_id"`
	DStorageURL           string       `json:"d_storage_url"`
	Source                string       `json:"source"`
	CreatorID             string       `json:"creator_id"`
	DeviceType            string       `json:"device_type"`
	DeviceModel           string       `json:"device_model"`
	DeviceBrand           string       `json:"device_brand"`
	Browser               string       `json:"browser"`
	OS                    string       `json:"os"`
	CPU                   string       `json:"cpu"`
	PlaybackGeoHash       string       `json:"playback_geo_hash"`
	PlaybackContinentName string       `json:"playback_continent_name"`
	PlaybackCountryCode   string       `json:"playback_country_code"`
	PlaybackCountryName   string       `json:"playback_country_name"`
	PlaybackSubdivision   string       `json:"playback_subdivision_name"`
	PlaybackTimezone      string       `json:"playback_timezone"`
	Data                  LogDataEvent `json:"data"`
}

func NewLogProcessor(bootstrapServers, user, password, topic string) (*LogProcessor, error) {
	dialer := &kafka.Dialer{
		Timeout: 10 * time.Second,
		SASLMechanism: plain.Mechanism{
			Username: user,
			Password: password,
		},
		DualStack: true,
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	// Create a new Kafka writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{bootstrapServers},
		Topic:    topic,
		Balancer: kafka.CRC32Balancer{},
		Dialer:   dialer,
	})

	return &LogProcessor{
		writer: writer,
		topic:  topic,
	}, nil
}

// Start starts LogProcessor which sends events to Kafka.
func (lp *LogProcessor) Start(ch chan LogData) {
	go func() {
		for {
			select {
			case d := <-ch:
				lp.processLog(d)
			}
		}
	}()
}

func (p *LogProcessor) processLog(d LogData) {
	key := []byte(d.SessionID)
	value, err := json.Marshal(d)
	if err != nil {
		glog.Errorf("invalid analytics log event, cannot sent to Kafka, err=%v", err)
		return
	}

	err = p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   key,
		Value: value,
	})

	if err != nil {
		glog.Errorf("error while sending analytics log to Kafka, err=%v", err)
	}
}
