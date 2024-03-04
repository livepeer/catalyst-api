package analytics

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/glog"
)

type ILogProcessor interface {
	Start(ch chan LogData)
}

type LogProcessor struct {
	kafkaProducer *kafka.Producer
	topic         string
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
	cfg := &kafka.ConfigMap{
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"bootstrap.servers": bootstrapServers,
		"sasl.username":     user,
		"sasl.password":     password,
	}
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}

	// Log errors in delivery to Kafka
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					glog.Errorf("Error in sending analytics logs to Kafka: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()

	return &LogProcessor{
		kafkaProducer: producer,
		topic:         topic,
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

	err = p.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}, nil)

	if err != nil {
		glog.Errorf("error while sending analytics log to Kafka, err=%v", err)
	}
}
