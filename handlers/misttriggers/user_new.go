package misttriggers

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/golang/glog"
)

type UserNewPayload struct {
	StreamName   string
	Hostname     string
	ConnectionID string
	Protocol     string
	URL          *url.URL
	FullURL      string
	SessionID    string
}

func ParseUserNewPayload(payload MistTriggerBody) (UserNewPayload, error) {
	lines := payload.Lines()
	if len(lines) != 6 {
		return UserNewPayload{}, fmt.Errorf("expected 6 lines in USER_NEW payload but got lines=%d payload=%s", len(lines), payload)
	}

	u, err := url.Parse(lines[4])
	if err != nil {
		return UserNewPayload{}, fmt.Errorf("unparsable URL in USER_NEW payload err=%s payload=%s", err, payload)
	}

	return UserNewPayload{
		StreamName:   lines[0],
		Hostname:     lines[1],
		ConnectionID: lines[2],
		Protocol:     lines[3],
		URL:          u,
		FullURL:      lines[4],
		SessionID:    lines[5],
	}, nil
}

func (d *MistCallbackHandlersCollection) TriggerUserNew(ctx context.Context, w http.ResponseWriter, req *http.Request, body MistTriggerBody) {
	payload, err := ParseUserNewPayload(body)
	if err != nil {
		glog.Infof("Error parsing USER_NEW payload error=%q payload=%q", err, string(body))
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false")) // nolint:errcheck
		return
	}
	resp, err := d.broker.TriggerUserNew(ctx, &payload)
	if err != nil {
		glog.Infof("Error handling USER_NEW payload error=%q payload=%q", err, string(body))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("false")) // nolint:errcheck
		return
	}
	w.Write([]byte(resp)) // nolint:errcheck
}
