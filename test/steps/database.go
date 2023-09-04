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
			downloaded_bytes int,
			uploaded_bytes int,
			session_duration_s int,
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
	return nil
}

func (s *StepContext) CheckDatabase(keyValues *godog.Table) error {
	metricsDB, err := sql.Open("postgres", DB_CONNECTION_STRING)
	if err != nil {
		return err
	}
	queryRows, err := metricsDB.Query("SELECT * FROM user_end_trigger")
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
