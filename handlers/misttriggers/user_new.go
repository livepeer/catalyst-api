package misttriggers

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/log"
)

type UserNewPayload struct {
	StreamName     string
	Hostname       string
	ConnectionID   string
	Protocol       string
	URL            *url.URL
	FullURL        string
	SessionID      string
	AccessKey      string
	JWT            string
	OriginIP       string
	OriginalURL    string
	Referrer       string
	UserAgent      string
	ForwardedProto string
	Host           string
	Origin         string
}

func ParseUserNewPayload(payload MistTriggerBody) (UserNewPayload, error) {
	lines := payload.Lines()
	if len(lines) < 6 || len(lines) > 7 {
		return UserNewPayload{}, fmt.Errorf("expected 6 or 7 lines in USER_NEW payload but got lines=%d payload=%s", len(lines), payload)
	}

	u, err := url.Parse(lines[4])
	if err != nil {
		return UserNewPayload{}, fmt.Errorf("unparsable URL in USER_NEW payload err=%s payload=%s", err, payload)
	}

	var originalURL string

	glog.Infof("Got USER_NEW payload streamName=%s hostname=%s connectionID=%s protocol=%s url=%s fullURL=%s sessionID=%s", lines[0], lines[1], lines[2], lines[3], u, lines[4], lines[5])

	if len(lines) == 6 {
		originalURL = ""
	} else {
		originalURL = lines[6]
		glog.Infof("Got USER_NEW payload originalURL=%s", originalURL)
	}

	return UserNewPayload{
		StreamName:   lines[0],
		Hostname:     lines[1],
		ConnectionID: lines[2],
		Protocol:     lines[3],
		URL:          u,
		FullURL:      lines[4],
		SessionID:    lines[5],
		OriginalURL:  originalURL,
	}, nil
}

func (d *MistCallbackHandlersCollection) TriggerUserNew(ctx context.Context, w http.ResponseWriter, req *http.Request, body MistTriggerBody) {
	payload, err := ParseUserNewPayload(body)
	cookies := req.Cookies()

	var accessKey, jwt string
	for _, cookie := range cookies {
		switch cookie.Name {
		case "Livepeer-Access-Key":
			accessKey = cookie.Value
		case "Livepeer-Jwt":
			jwt = cookie.Value
		case "X-Fowarded-For":
			payload.OriginIP = cookie.Value
		case "Referer":
			payload.Referrer = cookie.Value
		case "User-Agent":
			payload.UserAgent = cookie.Value
		case "X-Forwarded-Proto":
			payload.ForwardedProto = cookie.Value
		case "Host":
			payload.Host = cookie.Value
		case "Origin":
			payload.Origin = cookie.Value
		}
	}

	payload.AccessKey = accessKey
	payload.JWT = jwt

	if err != nil {
		log.LogCtx(ctx, "Error parsing USER_NEW payload",
			"err", err,
			"body", string(body))
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
