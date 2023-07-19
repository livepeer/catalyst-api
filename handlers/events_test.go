package handlers

import (
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/serf/serf"
	"github.com/julienschmidt/httprouter"
	mockcluster "github.com/livepeer/catalyst-api/mocks/cluster"
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

	catalystApiHandlers := EventsHandlersCollection{Cluster: mc}
	router := httprouter.New()
	router.POST("/events", catalystApiHandlers.Events())

	for _, tt := range tests {
		req, _ := http.NewRequest("POST", "/events", strings.NewReader(tt.requestBody))
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		require.Equal(rr.Result().StatusCode, tt.wantHttpCode)
	}
}
