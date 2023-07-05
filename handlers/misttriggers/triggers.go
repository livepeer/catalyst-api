package misttriggers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
)

const (
	TRIGGER_PUSH_END        = "PUSH_END"
	TRIGGER_PUSH_OUT_START  = "PUSH_OUT_START"
	TRIGGER_PUSH_REWRITE    = "PUSH_REWRITE"
	TRIGGER_STREAM_BUFFER   = "STREAM_BUFFER"
	TRIGGER_LIVE_TRACK_LIST = "LIVE_TRACK_LIST"
	TRIGGER_USER_NEW        = "USER_NEW"
)

type MistCallbackHandlersCollection struct {
	cli    *config.Cli
	broker TriggerBroker
}

func NewMistCallbackHandlersCollection(cli config.Cli, b TriggerBroker) *MistCallbackHandlersCollection {
	return &MistCallbackHandlersCollection{cli: &cli, broker: b}
}

// Trigger dispatches request to mapped method according to trigger name
// Only single trigger callback is allowed on Mist.
// All created streams and our handlers (segmenting, transcoding, et.) must share this endpoint.
// If handler logic grows more complicated we may consider adding dispatch mechanism here.
func (d *MistCallbackHandlersCollection) Trigger() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Cannot read trigger payload", err)
			return
		}

		triggerName := req.Header.Get("X-Trigger")
		mistVersion := req.Header.Get("X-Version")
		if mistVersion == "" {
			mistVersion = req.UserAgent()
		}
		log.LogNoRequestID(
			"Received Mist Trigger",
			"trigger_name", triggerName,
			"mist_version", mistVersion,
			"payload", log.RedactLogs(string(payload), "\n"),
		)

		ctx := context.Background()
		body := MistTriggerBody(payload)

		switch triggerName {
		case TRIGGER_PUSH_OUT_START:
			d.TriggerPushOutStart(ctx, w, req, body)
		case TRIGGER_PUSH_END:
			d.TriggerPushEnd(ctx, w, req, body)
		case TRIGGER_STREAM_BUFFER:
			d.TriggerStreamBuffer(ctx, w, req, body)
		case TRIGGER_PUSH_REWRITE:
			d.TriggerPushRewrite(ctx, w, req, body)
		case TRIGGER_LIVE_TRACK_LIST:
			d.TriggerLiveTrackList(ctx, w, req, body)
		case TRIGGER_USER_NEW:
			d.TriggerUserNew(ctx, w, req, body)
		default:
			errors.WriteHTTPBadRequest(w, "Unsupported X-Trigger", fmt.Errorf("unknown trigger '%s'", triggerName))
			return
		}
	}
}

type MistTriggerBody string

func (b MistTriggerBody) Lines() []string {
	trimmed := strings.TrimSpace(string(b))
	lines := strings.Split(trimmed, "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(lines[i])
	}
	return lines
}
