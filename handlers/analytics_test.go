package handlers

import (
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/go-api-client"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleLog(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name         string
		requestBody  string
		wantHttpCode int
	}{
		{
			name: "valid payload",
			requestBody: `{
				"session_id": "abcdef",
				"playback_id": "123456",
				"protocol": "video/mp4",
				"page_url": "https://www.fishtank.live/",
				"source_url": "https://vod-cdn.lp-playback.studio/raw/jxf4iblf6wlsyor6526t4tcmtmqa/catalyst-vod-com/hls/362f9l7ekeoze518/1080p0.mp4?tkn=8b140ec6b404a",
				"player": "video-@livepeer/react@3.1.9",
				"user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
				"uid": "abcdef",
				"events": [
					{
						"type": "heartbeat",
						"timestamp": 1234567895,
						"errors": 0,
						"playtime_ms": 4500,
						"ttff_ms": 300,
						"preload_time_ms": 1000,
						"buffer_ms": 50
					}
				]
			}`,
			wantHttpCode: 200,
		},
		{
			name: "additional field",
			requestBody: `{
				"unknown_field: "12355",
				"session_id": "abcdef",
				"playback_id": "123456",
				"protocol": "video/mp4",
				"page_url": "https://www.fishtank.live/",
				"source_url": "https://vod-cdn.lp-playback.studio/raw/jxf4iblf6wlsyor6526t4tcmtmqa/catalyst-vod-com/hls/362f9l7ekeoze518/1080p0.mp4?tkn=8b140ec6b404a",
				"player": "video-@livepeer/react@3.1.9",
				"user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
				"uid": "abcdef",
				"events": [
					{
						"type": "heartbeat",
						"timestamp": 1234567895,
						"errors": 0,
						"playtime_ms": 4500,
						"ttff_ms": 300,
						"preload_time_ms": 1000,
						"buffer_ms": 50
					}
				]
			}`,
			wantHttpCode: 400,
		},
		{
			name: "missing field",
			requestBody: `{
				"playback_id": "123456",
				"protocol": "video/mp4",
				"page_url": "https://www.fishtank.live/",
				"source_url": "https://vod-cdn.lp-playback.studio/raw/jxf4iblf6wlsyor6526t4tcmtmqa/catalyst-vod-com/hls/362f9l7ekeoze518/1080p0.mp4?tkn=8b140ec6b404a",
				"player": "video-@livepeer/react@3.1.9",
				"user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
				"uid": "abcdef",
				"events": [
					{
						"type": "heartbeat",
						"timestamp": 1234567895,
						"errors": 0,
						"playtime_ms": 4500,
						"ttff_ms": 300,
						"preload_time_ms": 1000,
						"buffer_ms": 50
					}
				]
			}`,
			wantHttpCode: 400,
		},
	}

	analyticsApiHandlers := AnalyticsHandlersCollection{}
	router := httprouter.New()
	router.POST("/analytics/log", analyticsApiHandlers.Log())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("POST", "/analytics/log", strings.NewReader(tt.requestBody))
			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			require.Equal(rr.Result().StatusCode, tt.wantHttpCode)
		})
	}
}

func TestParseAnalyticsGeo(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name              string
		header            http.Header
		exp               AnalyticsGeo
		wantErrorContains []string
	}{
		{
			name: "Correct Headers",
			header: map[string][]string{
				"X-Latitude":          {"50.06580"},
				"X-Longitude":         {"19.94010"},
				"X-Continent-Name":    {"Europe"},
				"X-City-Country-Name": {"Poland"},
				"X-Subregion-Name":    {"Lesser Poland"},
				"X-Time-Zone":         {"Europe/Warsaw"},
			},
			exp: AnalyticsGeo{
				GeoHash:     "u2yhvdpyqj",
				Continent:   "Europe",
				Country:     "Poland",
				Subdivision: "Lesser Poland",
				Timezone:    "Europe/Warsaw",
			},
			wantErrorContains: nil,
		},
		{
			name: "Missing Headers",
			header: map[string][]string{
				"X-Latitude":          {"50.06580"},
				"X-Longitude":         {"19.94010"},
				"X-City-Country-Name": {"Poland"},
			},
			exp: AnalyticsGeo{
				GeoHash: "u2yhvdpyqj",
				Country: "Poland",
			},
			wantErrorContains: []string{
				"missing geo headers",
				"X-Continent-Name",
				"X-Subregion-Name",
				"X-Time-Zone",
			},
		},
		{
			name: "Incorrect X-Longitude",
			header: map[string][]string{
				"X-Latitude":  {"sometext"},
				"X-Longitude": {"19.94010"},
			},
			exp:               AnalyticsGeo{},
			wantErrorContains: []string{"error parsing header X-Latitude"},
		},
		{
			name: "Incorrect Longitude",
			header: map[string][]string{
				"X-Latitude":  {"50.06580"},
				"X-Longitude": {"sometext"},
			},
			exp:               AnalyticsGeo{},
			wantErrorContains: []string{"error parsing header X-Longitude"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := http.Request{Header: tt.header}

			res, err := parseAnalyticsGeo(&req)

			if tt.wantErrorContains != nil {
				for _, errMsg := range tt.wantErrorContains {
					require.Contains(err.Error(), errMsg)
				}
			} else {
				require.NoError(err)
			}
			require.NotNil(res)
			if res.GeoHash != "" || tt.exp.GeoHash != "" {
				require.Equal(tt.exp.GeoHash[:GeoHashPrecision], res.GeoHash)
			}
			require.Equal(tt.exp.Continent, res.Continent)
			require.Equal(tt.exp.Country, res.Country)
			require.Equal(tt.exp.Subdivision, res.Subdivision)
			require.Equal(tt.exp.Timezone, res.Timezone)
		})
	}
}

type MockMapicCache struct {
	streams   map[string]*api.Stream
	callCount int
}

func (c *MockMapicCache) GetCachedStream(playbackID string) *api.Stream {
	c.callCount = c.callCount + 1
	return c.streams[playbackID]
}

func TestEnrichExtData(t *testing.T) {
	require := require.New(t)

	playbackID := "playback-id-1"
	userID := "user-id-1"

	mockMapicCache := &MockMapicCache{streams: map[string]*api.Stream{
		playbackID: {UserID: userID},
	}}

	c := NewAnalyticsHandlersCollection(mockMapicCache, nil, "")

	// First call
	res, err := c.enrichExtData(playbackID)
	require.NoError(err)
	require.Equal(userID, res.UserID)
	require.Equal(1, mockMapicCache.callCount)

	// Second call, use cache
	res, err = c.enrichExtData(playbackID)
	require.NoError(err)
	require.Equal(userID, res.UserID)
	require.Equal(1, mockMapicCache.callCount)
}

func TestLogProcessor(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name     string
		data     []AnalyticsData
		expected []string
	}{
		{
			name: "Multiple same logs results in 1 metric",
			data: []AnalyticsData{
				{
					sessionID:  "session-1",
					playbackID: "playback-1",
					browser:    "chrome",
					deviceType: "mobile",
					country:    "Poland",
					userID:     "user-1",
				},
				{
					sessionID:  "session-1",
					playbackID: "playback-1",
					browser:    "chrome",
					deviceType: "mobile",
					country:    "Poland",
					userID:     "user-1",
				},
			},
			expected: []string{
				`viewcount{user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 1`,
			},
		},
		{
			name: "Multiple same logs with different session IDs results in 1 metric",
			data: []AnalyticsData{
				{
					sessionID:  "session-1",
					playbackID: "playback-1",
					browser:    "chrome",
					deviceType: "mobile",
					country:    "Poland",
					userID:     "user-1",
				},
				{
					sessionID:  "session-2",
					playbackID: "playback-1",
					browser:    "chrome",
					deviceType: "mobile",
					country:    "Poland",
					userID:     "user-1",
				},
			},
			expected: []string{
				`viewcount{user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 2`,
			},
		},
		{
			name: "Different logs result in separate metrics",
			data: []AnalyticsData{
				{
					sessionID:  "session-1",
					playbackID: "playback-1",
					browser:    "chrome",
					deviceType: "mobile",
					country:    "Poland",
					userID:     "user-1",
				},
				{
					sessionID:  "session-2",
					playbackID: "playback-1",
					browser:    "firefox",
					deviceType: "mobile",
					country:    "Poland",
					userID:     "user-1",
				},
			},
			expected: []string{
				`viewcount{user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 1`,
				`viewcount{user_id="user-1",playback_id="playback-1",device_type="mobile",browser="firefox",country="Poland"} 1`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			var recordedRequest string
			promMockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				require.NoError(err)
				recordedRequest = string(body)
			}))
			defer promMockServer.Close()

			lp := NewLogProcessor(promMockServer.URL)

			// when
			for _, d := range tt.data {
				lp.processLog(d)
			}
			lp.sendMetrics()

			// then
			recordedLines := strings.Split(recordedRequest, "\n")
			if recordedLines[len(recordedLines)-1] == "" {
				recordedLines = recordedLines[:len(recordedLines)-1]
			}

			require.Equal(len(tt.expected), len(recordedLines))
			for _, exp := range tt.expected {
				require.Contains(recordedRequest, exp)
			}
		})
	}
}
