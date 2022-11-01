package misttriggers

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
)

const (
	TRIGGER_PUSH_END        = "PUSH_END"
	TRIGGER_PUSH_OUT_START  = "PUSH_OUT_START"
	TRIGGER_LIVE_TRACK_LIST = "LIVE_TRACK_LIST"
	TRIGGER_RECORDING_END   = "RECORDING_END"
)

type MistCallbackHandlersCollection struct {
	MistClient clients.MistAPIClient
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
		log.LogNoRequestID(
			"msg", "Received Mist Trigger",
			"trigger_name", triggerName,
			"payload", string(payload),
		)

		switch triggerName {
		case TRIGGER_PUSH_OUT_START:
			d.TriggerPushOutStart(w, req, payload)
		case TRIGGER_PUSH_END:
			d.TriggerPushEnd(w, req, payload)
		case TRIGGER_LIVE_TRACK_LIST:
			d.TriggerLiveTrackList(w, req, payload)
		case TRIGGER_RECORDING_END:
			d.TriggerRecordingEnd(w, req, payload)
		default:
			errors.WriteHTTPBadRequest(w, "Unsupported X-Trigger", fmt.Errorf("unknown trigger '%s'", triggerName))
			return
		}
	}
}

// streamNameToPipeline returns pipeline that given stream belongs to. We use different stream name prefixes for each pipeline.
func streamNameToPipeline(name string) PipelineId {
	if strings.HasPrefix(name, config.RENDITION_PREFIX) {
		// config.SOURCE_PREFIX also belongs to Transcoding. So far no triggers installed for source streams.
		return Transcoding
	} else if strings.HasPrefix(name, config.SEGMENTING_PREFIX) {
		return Segmenting
	} else if strings.HasPrefix(name, config.RECORDING_PREFIX) {
		return Recording
	}
	return Unrelated
}

type PipelineId = int

const (
	Unrelated PipelineId = iota
	Segmenting
	Transcoding
	Recording
)
