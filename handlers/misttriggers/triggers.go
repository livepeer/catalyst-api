package misttriggers

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
)

const (
	TRIGGER_PUSH_END       = "PUSH_END"
	TRIGGER_PUSH_OUT_START = "PUSH_OUT_START"
	TRIGGER_STREAM_BUFFER  = "STREAM_BUFFER"
)

type MistCallbackHandlersCollection struct {
	cli    *config.Cli
	broker TriggerBroker
}

type TriggerPayload interface {
	StreamBufferPayload | PushEndPayload
}

func NewMistCallbackHandlersCollection(cli config.Cli, b TriggerBroker) *MistCallbackHandlersCollection {
	return &MistCallbackHandlersCollection{cli: &cli, broker: b}
}

// Trigger dispatches request to mapped method according to trigger name
// Only single trigger callback is allowed on Mist.
// All created streams and our handlers (segmenting, transcoding, et.) must share this endpoint.
// If handler logic grows more complicated we may consider adding dispatch mechanism here.
func (d *MistCallbackHandlersCollection) Trigger(ctx context.Context) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Cannot read trigger payload", err)
			return
		}

		triggerName := req.Header.Get("X-Trigger")
		log.LogNoRequestID(
			"Received Mist Trigger",
			"trigger_name", triggerName,
			"payload", log.RedactLogs(string(payload), "\n"),
		)

		switch triggerName {
		case TRIGGER_PUSH_OUT_START:
			d.TriggerPushOutStart(ctx, w, req, payload)
		case TRIGGER_PUSH_END:
			d.TriggerPushEnd(ctx, w, req, payload)
		case TRIGGER_STREAM_BUFFER:
			d.TriggerStreamBuffer(ctx, w, req, payload)
		default:
			errors.WriteHTTPBadRequest(w, "Unsupported X-Trigger", fmt.Errorf("unknown trigger '%s'", triggerName))
			return
		}
	}
}
