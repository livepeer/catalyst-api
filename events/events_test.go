package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItCanHandleStreamEvents(t *testing.T) {
	payload := []byte(`{"resource": "stream", "playback_id": "abc123"}`)
	e, err := Unmarshal(payload)
	require.NoError(t, err)
	event, ok := e.(*StreamEvent)
	require.True(t, ok)
	require.Equal(t, event.PlaybackID, "abc123")
}

func TestItCanHandleNukeEvents(t *testing.T) {
	payload := []byte(`{"resource": "nuke", "playback_id": "abc123"}`)
	e, err := Unmarshal(payload)
	require.NoError(t, err)
	event, ok := e.(*NukeEvent)
	require.True(t, ok)
	require.Equal(t, event.PlaybackID, "abc123")
}

func TestItFailsUnknownEvents(t *testing.T) {
	payload := []byte(`{"resource": "not-real-thing"}`)
	_, err := Unmarshal(payload)
	require.Error(t, err)
}

func TestItFailsBadJSON(t *testing.T) {
	payload := []byte(`this is not valid JSON text`)
	_, err := Unmarshal(payload)
	require.Error(t, err)
}

func TestItFailsBadShapes(t *testing.T) {
	payloads := [][]byte{
		[]byte(`{"resource": "stream", "playback_id": 5.5}`),
		[]byte(`{"resource": "nuke", "playback_id": 5.5}`),
	}
	for _, payload := range payloads {
		_, err := Unmarshal(payload)
		require.Error(t, err)
	}
}
