package analytics

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/handlers/misttriggers"
)

const USER_END_TABLE_NAME = "user_end_trigger"

type AnalyticsHandler struct {
	db *sql.DB
}

func NewAnalyticsHandler(db *sql.DB) AnalyticsHandler {
	return AnalyticsHandler{db: db}
}

func (a *AnalyticsHandler) HandleUserEnd(ctx context.Context, payload *misttriggers.UserEndPayload) error {
	// If there's nowhere to write to, this handler is a no-op
	if a.db == nil {
		return nil
	}

	insertDynStmt := `insert into "` + USER_END_TABLE_NAME + `"(
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
		return err
	}

	return nil
}
