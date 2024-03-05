package analytics

import (
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLogProcessor(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name     string
		data     []LogData
		expected []string
	}{
		{
			name: "Multiple logs from the same viewer",
			data: []LogData{
				{
					SessionID:  "session-1",
					PlaybackID: "playback-1",
					Browser:    "chrome",
					DeviceType: "mobile",
					Country:    "Poland",
					UserID:     "user-1",
					PlaytimeMs: 4500,
					BufferMs:   500,
					Errors:     0,
				},
				{
					SessionID:  "session-1",
					PlaybackID: "playback-1",
					Browser:    "chrome",
					DeviceType: "mobile",
					Country:    "Poland",
					UserID:     "user-1",
					PlaytimeMs: 5000,
					BufferMs:   0,
					Errors:     5,
				},
			},
			expected: []string{
				`view_count{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 1`,
				`rebuffer_ratio{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 0.05`,
				`error_rate{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 0.5`,
			},
		},
		{
			name: "Multiple same logs from different viewers with the same labels",
			data: []LogData{
				{
					SessionID:  "session-1",
					PlaybackID: "playback-1",
					Browser:    "chrome",
					DeviceType: "mobile",
					Country:    "Poland",
					UserID:     "user-1",
					PlaytimeMs: 4500,
					BufferMs:   500,
					Errors:     0,
				},
				{
					SessionID:  "session-2",
					PlaybackID: "playback-1",
					Browser:    "chrome",
					DeviceType: "mobile",
					Country:    "Poland",
					UserID:     "user-1",
					PlaytimeMs: 5000,
					BufferMs:   0,
					Errors:     5,
				},
			},
			expected: []string{
				`view_count{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 2`,
				`rebuffer_ratio{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 0.05`,
				`error_rate{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 0.5`,
			},
		},
		{
			name: "Multiple logs from different viewers with different logs",
			data: []LogData{
				{
					SessionID:  "session-1",
					PlaybackID: "playback-1",
					Browser:    "chrome",
					DeviceType: "mobile",
					Country:    "Poland",
					UserID:     "user-1",
					PlaytimeMs: 4500,
					BufferMs:   500,
					Errors:     0,
				},
				{
					SessionID:  "session-2",
					PlaybackID: "playback-1",
					Browser:    "firefox",
					DeviceType: "mobile",
					Country:    "Poland",
					UserID:     "user-1",
					PlaytimeMs: 5000,
					BufferMs:   0,
					Errors:     5,
				},
			},
			expected: []string{
				`view_count{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 1`,
				`view_count{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="firefox",country="Poland"} 1`,
				`rebuffer_ratio{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 0.1`,
				`rebuffer_ratio{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="firefox",country="Poland"} 0`,
				`error_rate{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="chrome",country="Poland"} 0`,
				`error_rate{host="hostname",user_id="user-1",playback_id="playback-1",device_type="mobile",browser="firefox",country="Poland"} 1`,
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

			lp := NewLogProcessor(promMockServer.URL, "hostname")

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