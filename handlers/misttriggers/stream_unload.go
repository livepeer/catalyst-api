package misttriggers

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/pipeline"
)

// This trigger is run whenever a stream is unloaded from Mist.
// This trigger is stream-specific and non-blocking.
//
// The payload for this trigger is multiple lines, each separated by a single newline character (without an ending newline), containing data as such:
// stream name
// input type
func (d *MistCallbackHandlersCollection) TriggerStreamUnload(w http.ResponseWriter, req *http.Request, payload []byte) {

	p, err := ParseStreamUnloadPayload(string(payload))
	if err != nil {
		log.LogNoRequestID("Error parsing STREAM_UNLOAD payload", "error", err, "payload", string(payload))
		errors.WriteHTTPBadRequest(w, "Error parsing STREAM_UNLOAD payload", err)
		return
	}
	d.VODEngine.PipeMist.HandleStreamUnloadTrigger(pipeline.StreamUnloadPayload{StreamName: p.StreamName})
}

type StreamUnloadPayload struct {
	StreamName string
	InputType  string
}

func ParseStreamUnloadPayload(payload string) (StreamUnloadPayload, error) {
	lines := strings.Split(strings.TrimSuffix(payload, "\n"), "\n")
	if len(lines) != 2 {
		return StreamUnloadPayload{}, fmt.Errorf("expected 2 lines in STREAM_UNLOAD payload but got %d. Payload: %s", len(lines), payload)
	}
	return StreamUnloadPayload{
		StreamName: lines[0],
		InputType:  lines[1],
	}, nil
}
