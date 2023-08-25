package analytics

import (
	"context"
	"database/sql"
	"time"

	"github.com/lib/pq"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
)

const USER_END_TABLE_NAME = "user_end"

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
		"timestamp_ms",
		"connection_token",
		"downloaded_bytes",
		"uploaded_bytes",
		"session_duration_s",
		"stream_id",
		"streams_viewed",
		"stream_id_duration_s",
		"protocol",
		"protocol_duration_s",
		"ip_address",
		"ip_address_duration_s",
		"tags"
		) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
	_, err := a.db.Exec(
		insertDynStmt,
		time.Now().UnixMilli(),                          // timestamp_ms
		payload.ConnectionToken,                         // connection_token
		payload.DownloadedBytes,                         // delivered_bytes
		payload.UploadedBytes,                           // uploaded_bytes
		payload.TimeActiveSecs,                          // session_duration_s
		payload.StreamNames[len(payload.StreamNames)-1], // stream_id
		pq.Array(payload.StreamNames),                   // streams_viewed
		pq.Array(payload.PerStreamSecs),                 // stream_id_duration_s
		pq.Array(payload.Protocols),                     // protocol
		pq.Array(payload.PerProtocolSecs),               // protocol_duration_s
		pq.Array(payload.IPs),                           // ip_address
		pq.Array(payload.PerIPSecs),                     // ip_address_duration_s
		pq.Array(payload.Tags),                          // tags

	)
	if err != nil {
		return err
	}

	return nil
}
