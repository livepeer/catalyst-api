package misttriggers

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang/glog"
)

// We only pass these on to the analytics pipeline, so leave as strings for now
type UserEndPayload struct {
	ConnectionToken string
	StreamNames     []string
	Protocols       []string
	IPs             []string
	TimeActiveSecs  string
	UploadedBytes   string
	DownloadedBytes string
	Tags            []string
	PerIPSecs       []string
	PerProtocolSecs []string
	PerStreamSecs   []string
	SessionID       string
}

// connection token ("tkn" param), string
// comma-separated list of streams watched, string
// comma-separated list of protocols used, string
// comma-separated list of IP addresses, string
// time in seconds session was active for, uint
// total bytes uploaded, uint
// total bytes downloaded, uint
// list of tags applied to the session, each surrounded by [ and ], string
// comma-separated list of seconds spend connected to each IP address, same order as IP address list, string
// comma-separated list of seconds spend connected to each protocol, same order as protocol list, string
// comma-separated list of seconds spend connected to each stream, same order as stream list, string
// the session ID, string
func ParseUserEndPayload(payload MistTriggerBody) (UserEndPayload, error) {
	lines := payload.Lines()
	if len(lines) != 12 {
		return UserEndPayload{}, fmt.Errorf("expected 12 lines in USER_NEW payload but got lines=%d payload=%s", len(lines), payload)
	}

	return UserEndPayload{
		ConnectionToken: lines[0],
		StreamNames:     strings.Split(lines[1], ","),
		Protocols:       strings.Split(lines[2], ","),
		IPs:             strings.Split(lines[3], ","),
		TimeActiveSecs:  lines[4],
		UploadedBytes:   lines[5],
		DownloadedBytes: lines[6],
		Tags:            strings.Split(lines[7], ","),
		PerIPSecs:       strings.Split(lines[8], ","),
		PerProtocolSecs: strings.Split(lines[9], ","),
		PerStreamSecs:   strings.Split(lines[10], ","),
		SessionID:       lines[11],
	}, nil
}

func (d *MistCallbackHandlersCollection) TriggerUserEnd(ctx context.Context, w http.ResponseWriter, req *http.Request, body MistTriggerBody) {
	payload, err := ParseUserEndPayload(body)
	if err != nil {
		glog.Infof("Error parsing USER_END payload error=%q payload=%q", err, string(body))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	d.broker.TriggerUserEnd(ctx, &payload)
	w.WriteHeader(http.StatusOK)
}
