package handlers

import (
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/handlers/analytics"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const userID = "user-id"

type MockExternalDataFetcher struct {
	calledPlaybackIDs map[string]bool
}

func (f *MockExternalDataFetcher) Fetch(playbackID string) (analytics.ExternalData, error) {
	f.calledPlaybackIDs[playbackID] = true
	return analytics.ExternalData{UserID: userID}, nil
}

type MockLogProcessor struct {
	processed chan analytics.LogData
}

func (p *MockLogProcessor) Start(ch chan analytics.LogData) {
	p.processed = ch
}

func TestHandleLog(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name                     string
		requestBody              string
		wantHttpCode             int
		wantExtFetchedPlaybackID string
		wantProcessedLog         analytics.LogData
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
						"errors": 2,
						"playtime_ms": 4500,
						"ttff_ms": 300,
						"preload_time_ms": 1000,
						"buffer_ms": 50
					},
					{
						"type": "error",
						"timestamp": 1234567895
					}
				]
			}`,
			wantHttpCode:             200,
			wantExtFetchedPlaybackID: "123456",
			wantProcessedLog: analytics.LogData{
				SessionID:  "abcdef",
				PlaybackID: "123456",
				Browser:    "Chrome",
				DeviceType: "desktop",
				UserID:     userID,
				PlaytimeMs: 4500,
				BufferMs:   50,
				Errors:     2,
			},
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			mockFetcher := MockExternalDataFetcher{calledPlaybackIDs: make(map[string]bool)}
			mockProcessor := MockLogProcessor{}

			analyticsApiHandlers := AnalyticsHandlersCollection{
				extFetcher:   &mockFetcher,
				logProcessor: &mockProcessor,
			}
			router := httprouter.New()
			router.POST("/analytics/log", analyticsApiHandlers.Log())

			// when
			req, _ := http.NewRequest("POST", "/analytics/log", strings.NewReader(tt.requestBody))
			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			// then
			require.Equal(tt.wantHttpCode, rr.Result().StatusCode)
			if tt.wantHttpCode == http.StatusOK {
				require.Equal(1, len(mockFetcher.calledPlaybackIDs))
				require.True(mockFetcher.calledPlaybackIDs[tt.wantExtFetchedPlaybackID])
				require.Equal(tt.wantProcessedLog, <-mockProcessor.processed)
			} else {
				require.Equal(0, len(mockFetcher.calledPlaybackIDs))
			}
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
				"X-City-Country-Name": {"Poland"},
				"X-Region-Name":       {"Lesser Poland"},
				"X-Time-Zone":         {"Europe/Warsaw"},
			},
			exp: AnalyticsGeo{
				GeoHash:     "u2yhvdpyqj",
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
				"X-Region-Name",
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
			require.Equal(tt.exp.Country, res.Country)
			require.Equal(tt.exp.Subdivision, res.Subdivision)
			require.Equal(tt.exp.Timezone, res.Timezone)
		})
	}
}
