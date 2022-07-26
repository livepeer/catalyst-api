package misttriggers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/mokeypatching"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/stretchr/testify/require"
)

func TestPipelineId(t *testing.T) {
	records := []StreamSample{
		{"catalyst_vod_110442dc-5b7d-4725-a92f-231677ac6167", Segmenting},
		{"bigBucksBunny1080p", Unrelated},
		{"tr_rend_+10a40c88-dcf7-4d77-8ac2-4ef07cb23807", Transcoding},
		{"video2b1e43cd-f0df-4fc9-be6f-8bd91f9758a9", Recording},
	}
	for _, record := range records {
		require.Equal(t, record.expected, streamNameToPipeline(record.streamName), record.streamName)
	}
}

func TestRecordingStart(t *testing.T) {
	testStartTime := time.Now().UnixMilli()
	mistCallbackHandlers := &MistCallbackHandlersCollection{VODEngine: pipeline.NewStubCoordinator()}
	callbackHappened := make(chan bool, 10)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(200)
		message := clients.RecordingEvent{}
		err = json.Unmarshal(payload, &message)
		require.NoError(t, err)
		require.Equal(t, "videoSomeStreamName", message.StreamName)
		require.Equal(t, "start", message.Event)
		require.GreaterOrEqual(t, message.Timestamp, testStartTime)
		require.Less(t, message.Timestamp, testStartTime+100)
		require.NotEmpty(t, message.RecordingId)
		callbackHappened <- true
	}))
	defer callbackServer.Close()
	patch_cleanup := changeDefaultRecordingCallback(t, callbackServer.URL)
	defer patch_cleanup()

	router := httprouter.New()
	router.POST("/api/mist/trigger", mistCallbackHandlers.Trigger())
	pushOutTriggerPayload := "videoSomeStreamName\ns3+https://creds:passwd@s3.storage.com/region/livepeer-recordings-bucket/$stream/index.m3u8"
	req, _ := http.NewRequest("POST", "/api/mist/trigger", bytes.NewBuffer([]byte(pushOutTriggerPayload)))
	req.Header.Set("X-Trigger", "PUSH_OUT_START")
	req.Header.Set("Host", "test.livepeer.monster")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	require.Equal(t, 200, rr.Result().StatusCode)
	result := rr.Body.String()
	require.Equal(t, "s3+https://creds:passwd@s3.storage.com/region/livepeer-recordings-bucket/$stream/", result[:81])
	require.Greater(t, len(result), 92)
	require.Equal(t, "/index.m3u8", result[len(result)-11:])
	select {
	case <-callbackHappened:
	case <-time.After(1 * time.Second):
		require.FailNow(t, "no callback happened")
	}
}

func TestRecordingCompleted(t *testing.T) {
	testStartTime := time.Now().UnixMilli()
	mistCallbackHandlers := &MistCallbackHandlersCollection{VODEngine: pipeline.NewStubCoordinator()}
	callbackHappened := make(chan bool, 10)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(200)
		message := clients.RecordingEvent{}
		err = json.Unmarshal(payload, &message)
		require.NoError(t, err)
		require.Equal(t, "videoSomeStreamName", message.StreamName)
		require.Equal(t, "0b152108-0bee-4333-8cb7-e859b800c57f", message.RecordingId)
		require.Equal(t, "end", message.Event)
		require.NotNil(t, message.Success)
		require.True(t, *message.Success)
		require.GreaterOrEqual(t, message.Timestamp, testStartTime)
		require.Less(t, message.Timestamp, testStartTime+100)
		callbackHappened <- true
	}))
	defer callbackServer.Close()
	patch_cleanup := changeDefaultRecordingCallback(t, callbackServer.URL)
	defer patch_cleanup()

	router := httprouter.New()
	router.POST("/api/mist/trigger", mistCallbackHandlers.Trigger())
	pushOutTriggerPayload := "123\nvideoSomeStreamName\ns3+https://creds:passwd@s3.storage.com/region/livepeer-recordings-bucket/$stream/0b152108-0bee-4333-8cb7-e859b800c57f/index.m3u8\ns3+https://creds:passwd@s3.storage.com/region/livepeer-recordings-bucket/videoSomeStreamName/0b152108-0bee-4333-8cb7-e859b800c57f/index.m3u8\n[]\nnull"
	req, _ := http.NewRequest("POST", "/api/mist/trigger", bytes.NewBuffer([]byte(pushOutTriggerPayload)))
	req.Header.Set("X-Trigger", "PUSH_END")
	req.Header.Set("Host", "test.livepeer.monster")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	require.Equal(t, 200, rr.Result().StatusCode)
	select {
	case <-callbackHappened:
	case <-time.After(1 * time.Second):
		require.FailNow(t, "no callback happened")
	}
}

func changeDefaultRecordingCallback(t *testing.T, callback string) func() {
	mokeypatching.MonkeypatchingMutex.Lock()
	originalValue := config.RecordingCallback
	config.RecordingCallback = callback
	return func() {
		// Restore original value
		config.RecordingCallback = originalValue
		mokeypatching.MonkeypatchingMutex.Unlock()
	}
}

type StreamSample struct {
	streamName string
	expected   PipelineId
}
