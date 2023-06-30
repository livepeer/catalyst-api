package misttriggers

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/errors"
)

type PushRewritePayload struct {
	FullURL    string
	URL        *url.URL
	Hostname   string
	StreamName string
}

func ParsePushRewritePayload(payload MistTriggerBody) (PushRewritePayload, error) {
	lines := payload.Lines()
	if len(lines) != 3 {
		return PushRewritePayload{}, fmt.Errorf("expected 3 lines in PUSH_REWRITE payload but got %d. Payload: %s", len(lines), payload)
	}

	u, err := url.Parse(lines[0])
	if err != nil {
		return PushRewritePayload{}, fmt.Errorf("unparsable URL in PUSH_REWRITE payload err=%s payload=%s", err, payload)
	}

	return PushRewritePayload{
		FullURL:    lines[0],
		URL:        u,
		Hostname:   lines[1],
		StreamName: lines[2],
	}, nil
}

func (d *MistCallbackHandlersCollection) TriggerPushRewrite(ctx context.Context, w http.ResponseWriter, req *http.Request, payload MistTriggerBody) {
	body, err := ParsePushRewritePayload(payload)
	if err != nil {
		glog.Infof("Error parsing PUSH_REWRITE payload error=%q payload=%q", err, string(payload))
		errors.WriteHTTPBadRequest(w, "Error parsing PUSH_REWRITE payload", err)
		return
	}
	resp, err := d.broker.TriggerPushRewrite(ctx, &body)
	if err != nil {
		glog.Infof("Error handling PUSH_REWRITE payload error=%q payload=%q", err, string(payload))
		errors.WriteHTTPInternalServerError(w, "Error handling PUSH_REWRITE payload", err)
		return
	}
	// Flushing necessary here for Mist to handle an empty response body
	flusher := w.(http.Flusher)
	flusher.Flush()
	w.Write([]byte(resp)) // nolint:errcheck
}
