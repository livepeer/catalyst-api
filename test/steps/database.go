package steps

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cucumber/godog"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

const DB_CONNECTION_STRING = "host=127.0.0.1 port=5432 sslmode=disable user=postgres password=postgres dbname=postgres"

func (s *StepContext) StartDatabase() error {
	s.Database = embeddedpostgres.NewDatabase()
	if err := s.Database.Start(); err != nil {
		return err
	}

	metricsDB, err := sql.Open("postgres", DB_CONNECTION_STRING)
	if err != nil {
		return err
	}

	// Create the table we'll be writing to
	_, err = metricsDB.Exec(`
		CREATE table user_end_trigger (
			uuid varchar,
			timestamp_ms bigint,
			connection_token varchar,
			downloaded_bytes bigint,
			uploaded_bytes bigint,
			session_duration_s bigint,
			stream_id varchar,
			stream_id_count int,
			protocol varchar,
			protocol_count int,
			ip_address varchar,
			ip_address_count int,
			tags varchar
		)
	`)
	if err != nil {
		return err
	}

	// Create vod table
	_, err = metricsDB.Exec(`
		CREATE TABLE vod_completed (
			timestamp                bigint,
			request_id               text,
			job_duration             double precision,
			source_segment_count     integer,
			transcoded_segment_count integer,
			source_bytes_count       integer,
			source_duration          double precision,
			source_codec_video       text,
			source_codec_audio       text,
			pipeline                 text,
			catalyst_region          text,
			state                    text,
			profiles_count           integer,
			started_at               bigint,
			finished_at              bigint,
			source_url               text,
			target_url               text,
			in_fallback_mode         boolean,
			external_id              text,
			source_playback_at       bigint,
			download_done_at         bigint,
			segmenting_done_at       bigint,
			transcoding_done_at      bigint,
			is_clip                  boolean,
			is_thumbs                boolean
		);
	`)
	if err != nil {
		return err
	}
	return nil
}

func (s *StepContext) CheckDatabase(table string, keyValues *godog.Table) error {
	metricsDB, err := sql.Open("postgres", DB_CONNECTION_STRING)
	if err != nil {
		return err
	}
	queryRows, err := metricsDB.Query(fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		return err
	}

	cols, err := queryRows.Columns()
	if err != nil {
		return err
	}

	if !queryRows.Next() {
		return fmt.Errorf("no rows found in the database")
	}

	// Create a slice of interface{}'s to represent each column,
	// and a second slice to contain pointers to each item in the columns slice.
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}

	// Scan the result into the column pointers...
	if err := queryRows.Scan(columnPointers...); err != nil {
		return err
	}

	// Create our map, and retrieve the value for each column from the pointers slice,
	// storing it in the map with the name of the column as the key.
	queryMap := make(map[string]interface{})
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		queryMap[colName] = *val
	}

	for i, row := range keyValues.Rows {
		if i == 0 {
			continue // Skip the header row
		}

		key := row.Cells[0].Value
		value := row.Cells[1].Value
		actualValue := queryMap[key]

		var actualValueString string
		if actualValueU, ok := actualValue.([]uint8); ok {
			// This is a Postgres array and will look like {val1 val2} but we only ever deal with single values
			// so can be a bit hacky with turning it into a string
			actualValueString = string(actualValueU)
			actualValueString = strings.TrimLeft(actualValueString, "{")
			actualValueString = strings.TrimRight(actualValueString, "}")
		} else if actualValueS, ok := actualValue.(string); ok {
			actualValueString = actualValueS
		} else if actualValueI, ok := actualValue.(int64); ok {
			actualValueString = strconv.Itoa(int(actualValueI))
		} else if actualValueB, ok := actualValue.(bool); ok {
			actualValueString = strconv.FormatBool(actualValueB)
		} else if actualValueF, ok := actualValue.(float64); ok {
			actualValueString = strconv.FormatFloat(actualValueF, 'f', -1, 64)
		} else if actualValue == nil {
			return fmt.Errorf("field %s not found in database", key)
		} else {
			return fmt.Errorf("unsupported type returned from DB for field %s: %s", key, reflect.TypeOf(actualValue))
		}

		if actualValueString != value {
			return fmt.Errorf("expected %s to equal %s but got %s", key, value, actualValueString)
		}
	}
	return nil
}
