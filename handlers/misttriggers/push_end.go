package misttriggers

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/livepeer/catalyst-api/errors"
)

type PushEndPayload struct {
	StreamName        string
	Destination       string
	ActualDestination string
	Last10LogLines    string
	PushStatus        string
}

func ParsePushEndPayload(payload string) (PushEndPayload, error) {
	lines := strings.Split(strings.TrimSuffix(payload, "\n"), "\n")
	if len(lines) != 6 {
		return PushEndPayload{}, fmt.Errorf("expected 6 lines in PUSH_END payload but got %d. Payload: %s", len(lines), payload)
	}

	return PushEndPayload{
		StreamName:        lines[1],
		Destination:       lines[2],
		ActualDestination: lines[3],
		Last10LogLines:    lines[4],
		PushStatus:        lines[5],
	}, nil
}

// TriggerPushEnd responds to PUSH_END trigger
// This trigger is run whenever an outgoing push stops, for any reason.
// This trigger is stream-specific and non-blocking. The payload for this trigger is multiple lines,
// each separated by a single newline character (without an ending newline), containing data:
//
//	push ID (integer)
//	stream name (string)
//	target URI, before variables/triggers affected it (string)
//	target URI, afterwards, as actually used (string)
//	last 10 log messages (JSON array string)
//	most recent push status (JSON object string)
func (d *MistCallbackHandlersCollection) TriggerPushEnd(ctx context.Context, w http.ResponseWriter, req *http.Request, payload []byte) {
	_, err := ParsePushEndPayload(string(payload))
	if err != nil {
		errors.WriteHTTPBadRequest(w, "Error parsing PUSH_END payload", err)
		return
	}
}
