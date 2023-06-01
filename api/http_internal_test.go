package api

// func TestTriggerStreamBufferE2E(t *testing.T) {
// 	// Start an HTTP test server to simulate the webhook endpoint
// 	var receivedPayload clients.StreamHealthPayload
// 	var receivedAuthHeader string
// 	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 		receivedAuthHeader = r.Header.Get("Authorization")

// 		defer r.Body.Close()
// 		err := json.NewDecoder(r.Body).Decode(&receivedPayload)
// 		if err != nil {
// 			w.WriteHeader(http.StatusBadRequest)
// 			_, err := w.Write([]byte("error unmarshalling payload"))
// 			require.NoError(t, err)
// 			return
// 		}

// 		w.WriteHeader(http.StatusNoContent)
// 	}))
// 	defer server.Close()

// 	// Prepare the request and payload
// 	payload := strings.NewReader(strings.Join(streamBufferPayloadIssues, "\n"))
// 	req, err := http.NewRequest("GET", "http://example.com", payload)
// 	require.NoError(t, err)
// 	req.Header.Set("X-UUID", "session1")

// 	// Call the TriggerStreamBuffer function
// 	cli := &config.Cli{
// 		StreamHealthHookURL: server.URL,
// 		APIToken:            "apiToken",
// 	}
// 	err = TriggerStreamBuffer(cli, req, streamBufferPayloadIssues)
// 	require.NoError(t, err)

// 	// Check the payload received by the test server
// 	require.Equal(t, receivedAuthHeader, "Bearer apiToken")
// 	require.Equal(t, receivedPayload.StreamName, "stream1")
// 	require.Equal(t, receivedPayload.SessionID, "session1")
// 	require.Equal(t, receivedPayload.IsActive, true)
// 	require.Equal(t, receivedPayload.IsHealthy, false)
// 	require.Len(t, receivedPayload.Tracks, 1)
// 	require.Contains(t, receivedPayload.Tracks, "track1")
// 	require.Equal(t, receivedPayload.HumanIssues, []string{"Stream is feeling under the weather"})
// }
