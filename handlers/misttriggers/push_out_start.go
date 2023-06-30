package misttriggers

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/livepeer/catalyst-api/errors"
)

// TriggerPushOutStart responds to PUSH_OUT_START trigger
// This trigger is run right before an outgoing push is started. This trigger is stream-specific and must be blocking.
// The payload for this trigger is multiple lines, each separated by a single newline character (without an ending newline), containing data:
//
// stream name (string)
// push target URI (string)
func (d *MistCallbackHandlersCollection) TriggerPushOutStart(ctx context.Context, w http.ResponseWriter, req *http.Request, payload MistTriggerBody) {
	lines := payload.Lines()
	if len(lines) != 2 {
		errors.WriteHTTPBadRequest(w, "Bad request payload", fmt.Errorf("unknown payload '%s'", string(payload)))
		return
	}
	// streamName := lines[0]
	destination := lines[1]
	var destinationToReturn = destination
	if _, err := w.Write([]byte(destinationToReturn)); err != nil {
		log.Printf("TriggerPushOutStart failed to send rewritten url: %v", err)
	}
}
