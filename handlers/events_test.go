package handlers

import (
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/serf/serf"
	"github.com/julienschmidt/httprouter"
	mockcluster "github.com/livepeer/catalyst-api/mocks/cluster"
	mock_mistapiconnector "github.com/livepeer/catalyst-api/mocks/mistapiconnector"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEventHandler(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		requestBody  string
		wantHttpCode int
	}{
		{
			requestBody: `{
				"resource": "stream",
				"playback_id": "123456789"
			}`,
			wantHttpCode: 200,
		},
		{
			requestBody:  "",
			wantHttpCode: 400,
		},
		{
			requestBody:  "invalid payload",
			wantHttpCode: 400,
		},
		{
			requestBody: `{
				"resource": "stream"
			}`,
			wantHttpCode: 400,
		},
		{
			requestBody: `{
				"resource": "unknown"
			}`,
			wantHttpCode: 400,
		},
		{
			requestBody: `{
				"resource": "stream",
				"playback_id": "123456789",
				"additional": "field"
			}`,
			wantHttpCode: 400,
		},
	}

	ctrl := gomock.NewController(t)
	mc := mockcluster.NewMockCluster(ctrl)
	mc.EXPECT().BroadcastEvent(gomock.Any()).DoAndReturn(func(event serf.UserEvent) error {
		return nil
	}).AnyTimes()

	catalystApiHandlers := NewEventsHandlersCollection(mc, nil, nil, "")
	router := httprouter.New()
	router.POST("/events", catalystApiHandlers.Events())

	for _, tt := range tests {
		req, _ := http.NewRequest("POST", "/events", strings.NewReader(tt.requestBody))
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		require.Equal(rr.Result().StatusCode, tt.wantHttpCode)
	}
}

func TestReceiveUserEventHandler(t *testing.T) {
	require := require.New(t)
	playbackId := "123456789"

	tests := []struct {
		name           string
		requestBody    string
		functionCalled string
	}{
		{
			name: "Refresh Stream",
			requestBody: `{
				"resource": "stream",
				"playback_id": "123456789"
			}`,
			functionCalled: "RefreshStreamIfNeeded",
		},
		{
			name: "Nuke Stream",
			requestBody: `{
				"resource": "nuke",
				"playback_id": "123456789"
			}`,
			functionCalled: "NukeStream",
		},
		{
			name: "Stop Sessions",
			requestBody: `{
				"resource": "stopSessions",
				"playback_id": "123456789"
			}`,
			functionCalled: "StopSessions",
		},
	}

	ctrl := gomock.NewController(t)
	mac := mock_mistapiconnector.NewMockIMac(ctrl)

	catalystApiHandlers := NewEventsHandlersCollection(nil, mac, nil, "")
	router := httprouter.New()
	router.POST("/receiveUserEvent", catalystApiHandlers.ReceiveUserEvent())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.functionCalled {
			case "RefreshStreamIfNeeded":
				mac.EXPECT().RefreshStreamIfNeeded(playbackId).Times(1)
			case "NukeStream":
				mac.EXPECT().NukeStream(playbackId).Times(1)
			case "StopSessions":
				mac.EXPECT().StopSessions(playbackId).Times(1)
			}

			req, _ := http.NewRequest("POST", "/receiveUserEvent", strings.NewReader(tt.requestBody))
			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			require.Equal(rr.Result().StatusCode, 200)
		})
	}
}
