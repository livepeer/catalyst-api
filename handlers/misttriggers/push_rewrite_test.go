package misttriggers

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/livepeer/catalyst-api/config"
	"github.com/stretchr/testify/require"
)

var pushRewritePayload = MistTriggerBody(`
	rtmp://127.0.0.1:1935/live/c447-3l8v-1vmz-ej5t
	127.0.0.1
	c447-3l8v-1vmz-ej5t
`)

var pushRewritePayloadInvalid = MistTriggerBody(`
	rtmp://127.0.0.1:1935/live/c447-3l8v-1vmz-ej5t
`)

var pushRewritePayloadBadUrl = MistTriggerBody(`
	http://hostname with spaces.com
	127.0.0.1
	c447-3l8v-1vmz-ej5t
`)

func TestItCanParseAValidPushRewritePayload(t *testing.T) {
	p, err := ParsePushRewritePayload(pushRewritePayload)
	require.NoError(t, err)
	require.Equal(t, p.FullURL, "rtmp://127.0.0.1:1935/live/c447-3l8v-1vmz-ej5t")
	require.Equal(t, p.URL.String(), "rtmp://127.0.0.1:1935/live/c447-3l8v-1vmz-ej5t")
	require.Equal(t, p.Hostname, "127.0.0.1")
	require.Equal(t, p.StreamName, "c447-3l8v-1vmz-ej5t")
}

func ItCanRejectABadPushRewritePayload(t *testing.T) {
	_, err := ParsePushRewritePayload(pushRewritePayloadInvalid)
	require.Error(t, err)
	_, err = ParsePushRewritePayload(pushRewritePayloadBadUrl)
	require.Error(t, err)
}

func doRequest(t *testing.T, payload MistTriggerBody, cb func(ctx context.Context, prp *PushRewritePayload) (string, error)) *httptest.ResponseRecorder {
	broker := NewTriggerBroker()
	broker.OnPushRewrite(cb)
	d := NewMistCallbackHandlersCollection(config.Cli{}, broker)
	req, err := http.NewRequest("POST", "/trigger", bytes.NewBuffer([]byte(payload)))
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	d.TriggerPushRewrite(context.Background(), rr, req, payload)
	return rr
}

func TestItCanHandlePushRewriteRequests(t *testing.T) {
	rr := doRequest(t, pushRewritePayload, func(ctx context.Context, prp *PushRewritePayload) (string, error) {
		require.Equal(t, prp.StreamName, "c447-3l8v-1vmz-ej5t")
		return "funky-stream", nil
	})
	require.Equal(t, rr.Result().StatusCode, 200)
	require.Equal(t, rr.Body.String(), "funky-stream")
}

func TestItCanRejectPushRewriteRequests(t *testing.T) {
	rr := doRequest(t, pushRewritePayload, func(ctx context.Context, prp *PushRewritePayload) (string, error) {
		// Proper way to reject Mist
		return "", nil
	})
	require.Equal(t, rr.Result().StatusCode, 200)
	require.Equal(t, rr.Body.String(), "")
}

func TestItCanHandleFailureToHandle(t *testing.T) {
	rr := doRequest(t, pushRewritePayload, func(ctx context.Context, prp *PushRewritePayload) (string, error) {
		return "", fmt.Errorf("something went wrong")
	})
	require.Equal(t, rr.Result().StatusCode, 500)
}

func TestItCanErrorPushRewriteRequests(t *testing.T) {
	rr := doRequest(t, pushRewritePayloadBadUrl, func(ctx context.Context, prp *PushRewritePayload) (string, error) {
		require.Fail(t, "test should be failing before it gets to me")
		return "", nil
	})
	require.Equal(t, rr.Result().StatusCode, 400)

	rr = doRequest(t, pushRewritePayloadInvalid, func(ctx context.Context, prp *PushRewritePayload) (string, error) {
		require.Fail(t, "test should be failing before it gets to me")
		return "", nil
	})
	require.Equal(t, rr.Result().StatusCode, 400)
}
