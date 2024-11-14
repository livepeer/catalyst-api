package analytics

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/livepeer/catalyst-api/config"
	"github.com/segmentio/kafka-go"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
)

const (
	userEndTableName  = "user_end_trigger"
	channelBufferSize = 200000
	sendInterval      = 1 * time.Second
)

type AnalyticsHandler struct {
	db     *sql.DB
	dataCh chan userEndData
	events []userEndData
	writer *kafka.Writer
}

type userEndData struct {
	UUID            string `json:"uuid"`
	TimestampMs     int64  `json:"timestamp_ms"`
	ConnectionToken string `json:"connection_token"`
	DownloadedBytes string `json:"downloaded_bytes"`
	UploadedBytes   string `json:"uploaded_bytes"`
	SessionDuration string `json:"session_duration_s"`
	StreamID        string `json:"stream_id"`
	StreamIDCount   int    `json:"stream_id_count"`
	Protocol        string `json:"protocol"`
	ProtocolCount   int    `json:"protocol_count"`
	IPAddress       string `json:"ip_address"`
	IPAddressCount  int    `json:"ip_address_count"`
	Tags            string `json:"tags"`
}

func NewAnalyticsHandler(cli config.Cli, db *sql.DB) AnalyticsHandler {
	var writer *kafka.Writer
	if cli.KafkaBootstrapServers == "" || cli.KafkaUser == "" || cli.KafkaPassword == "" || cli.UserEndKafkaTopic == "" {
		glog.Warning("Invalid Kafka configuration for USER_END events, not using Kafka")
	} else {
		writer = newWriter(cli.KafkaBootstrapServers, cli.KafkaUser, cli.KafkaPassword, cli.UserEndKafkaTopic)
	}

	a := AnalyticsHandler{
		// Deprecated, we'll remove it when the Kafka setup is all in place
		db: db,

		// User to send USER_END events to Kafka
		dataCh: make(chan userEndData, channelBufferSize),
		writer: writer,
	}

	a.startLoop()
	return a

}

func (a *AnalyticsHandler) HandleUserEnd(ctx context.Context, payload *misttriggers.UserEndPayload) error {
	if a.writer != nil {
		// Using Kafka
		select {
		case a.dataCh <- toUserEndData(payload):
			// process data async
		default:
			glog.Warningf("error processing USER_END trigger event, too many triggers in the buffer")
		}
	}

	if a.db != nil {
		// Using Postgres DB

		// No need to block our response to Mist; everything else in a goroutine
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					glog.Errorf("panic writing to analytics database err=%s payload=%v", rec, payload)
				}
			}()
			insertDynStmt := `insert into "` + userEndTableName + `"(
			"uuid",
			"timestamp_ms",
			"connection_token",
			"downloaded_bytes",
			"uploaded_bytes",
			"session_duration_s",
			"stream_id",
			"stream_id_count",
			"protocol",
			"protocol_count",
			"ip_address",
			"ip_address_count",
			"tags"
			) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
			_, err := a.db.Exec(
				insertDynStmt,
				payload.TriggerID,                               // uuid
				time.Now().UnixMilli(),                          // timestamp_ms
				payload.ConnectionToken,                         // connection_token
				payload.DownloadedBytes,                         // downloaded_bytes
				payload.UploadedBytes,                           // uploaded_bytes
				payload.TimeActiveSecs,                          // session_duration_s
				payload.StreamNames[len(payload.StreamNames)-1], // stream_id
				len(payload.StreamNames),                        // stream_id_count
				payload.Protocols[len(payload.Protocols)-1],     // protocol
				len(payload.Protocols),                          // protocol_count
				payload.IPs[len(payload.IPs)-1],                 // ip_address
				len(payload.IPs),                                // ip_address_count
				strings.Join(payload.Tags, ","),                 // tags
			)
			if err != nil {
				glog.Errorf("error writing to analytics database err=%s payload=%v", err, payload)
			}
		}()
	}

	return nil
}

func (a *AnalyticsHandler) startLoop() {
	if a.writer == nil {
		// Not using Kafka
		return
	}

	t := time.NewTicker(sendInterval)
	go func() {
		for {
			select {
			case d := <-a.dataCh:
				a.events = append(a.events, d)
			case <-t.C:
				a.sendEvents()
			}
		}
	}()
}

func (a *AnalyticsHandler) sendEvents() {
	defer logWriteMetrics(a.writer)

	if len(a.events) > 0 {
		glog.Infof("sending USER_END events, count=%d", len(a.events))
	} else {
		glog.V(6).Info("no USER_END events, skip sending")
		return
	}

	var msgs []kafka.Message
	for _, d := range a.events {
		key, err := json.Marshal(KafkaKey{SessionID: d.UUID})
		if err != nil {
			glog.Errorf("invalid USER_END event, cannot create Kafka key, UUID=%s, err=%v", d.UUID, err)
			continue
		}
		value, err := json.Marshal(d)
		if err != nil {
			glog.Errorf("invalid USER_END event, cannot create Kafka value, UUID=%s, err=%v", d.UUID, err)
			continue
		}
		msgs = append(msgs, kafka.Message{Key: key, Value: value})
	}
	a.events = []userEndData{}

	sendWithRetries(a.writer, msgs)
}

func toUserEndData(payload *misttriggers.UserEndPayload) userEndData {
	return userEndData{
		UUID:            payload.TriggerID,
		TimestampMs:     time.Now().UnixMilli(),
		ConnectionToken: payload.ConnectionToken,
		DownloadedBytes: payload.DownloadedBytes,
		UploadedBytes:   payload.UploadedBytes,
		SessionDuration: payload.TimeActiveSecs,
		StreamID:        payload.StreamNames[len(payload.StreamNames)-1],
		StreamIDCount:   len(payload.StreamNames),
		Protocol:        payload.Protocols[len(payload.Protocols)-1],
		ProtocolCount:   len(payload.Protocols),
		IPAddress:       payload.IPs[len(payload.IPs)-1],
		IPAddressCount:  len(payload.IPs),
		Tags:            strings.Join(payload.Tags, ","),
	}
}
