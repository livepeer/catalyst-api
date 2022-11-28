package misttriggers

import (
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/pipeline"
)

// This trigger is run whenever an output to file finishes writing, either through the pushing system (with a file target) or when ran manually.
// It’s purpose is for handling re-encodes or logging of stored files, etcetera.
// This trigger is stream-specific and non-blocking.
//
// The payload for this trigger is multiple lines, each separated by a single newline character (without an ending newline), containing data as such:
// stream name
// path to file that just finished writing
// output protocol name
// number of bytes written to file
// amount of seconds that writing took (NOT duration of stream media data!)
// time of connection start (unix-time)
// time of connection end (unix-time)
// duration of stream media data (milliseconds)
// first media timestamp (milliseconds)
// last media timestamp (milliseconds)
func (d *MistCallbackHandlersCollection) TriggerRecordingEnd(w http.ResponseWriter, req *http.Request, payload []byte) {
	p, err := ParseRecordingEndPayload(string(payload))
	if err != nil {
		log.LogNoRequestID("Error parsing RECORDING_END payload", "error", err, "payload", string(payload))
		errors.WriteHTTPBadRequest(w, "Error parsing RECORDING_END payload", err)
		return
	}

	switch streamNameToPipeline(p.StreamName) {
	case Transcoding:
		// TODO
	case Segmenting:
		d.VODEngine.TriggerRecordingEnd(pipeline.RecordingEndPayload{
			StreamName:                p.StreamName,
			StreamMediaDurationMillis: p.StreamMediaDurationMillis,
			WrittenBytes:              p.WrittenBytes,
		})
	default:
		// Not related to API logic
		log.LogNoRequestID("Received RECORDING_END trigger for non-VOD stream", "stream_name", p.StreamName)
	}
}

type RecordingEndPayload struct {
	StreamName                string
	WrittenFilepath           string
	OutputProtocol            string
	WrittenBytes              int
	WritingDurationSecs       int
	ConnectionStartTimeUnix   int
	ConnectionEndTimeUnix     int
	StreamMediaDurationMillis int64
	FirstMediaTimestampMillis big.Int
	LastMediaTimestampMillis  big.Int
}

func ParseRecordingEndPayload(payload string) (RecordingEndPayload, error) {
	lines := strings.Split(strings.TrimSuffix(payload, "\n"), "\n")
	if len(lines) != 10 {
		return RecordingEndPayload{}, fmt.Errorf("expected 10 lines in RECORDING_END payload but got %d. Payload: %s", len(lines), payload)
	}

	WrittenBytes, err := strconv.Atoi(lines[3])
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 3, lines[3], err)
	}

	WritingDurationSecs, err := strconv.Atoi(lines[4])
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 4, lines[4], err)
	}

	ConnectionStartTimeUnix, err := strconv.Atoi(lines[5])
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 5, lines[5], err)
	}

	ConnectionEndTimeUnix, err := strconv.Atoi(lines[6])
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 6, lines[6], err)
	}

	StreamMediaDurationMillis, err := strconv.ParseInt(lines[7], 10, 64)
	if err != nil {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 7, lines[7], err)
	}

	var FirstMediaTimestampMillis big.Int
	if _, ok := FirstMediaTimestampMillis.SetString(lines[8], 10); !ok {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 8, lines[8], err)
	}

	var LastMediaTimestampMillis big.Int
	if _, ok := LastMediaTimestampMillis.SetString(lines[9], 10); !ok {
		return RecordingEndPayload{}, fmt.Errorf("error parsing line %d of RECORDING_END payload as an int. Line contents: %s. Error: %s", 9, lines[9], err)
	}

	return RecordingEndPayload{
		StreamName:                lines[0],
		WrittenFilepath:           lines[1],
		OutputProtocol:            lines[2],
		WrittenBytes:              WrittenBytes,
		WritingDurationSecs:       WritingDurationSecs,
		ConnectionStartTimeUnix:   ConnectionStartTimeUnix,
		ConnectionEndTimeUnix:     ConnectionEndTimeUnix,
		StreamMediaDurationMillis: StreamMediaDurationMillis,
		FirstMediaTimestampMillis: FirstMediaTimestampMillis,
		LastMediaTimestampMillis:  LastMediaTimestampMillis,
	}, nil
}
