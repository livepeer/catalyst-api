package misttriggers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItCanParseAValidRecordingEndPayload(t *testing.T) {
	var payload = "1\n2\n3\n4\n5\n6\n7\n8\n9\n10"
	p, err := ParseRecordingEndPayload(payload)
	require.NoError(t, err)
	require.Equal(t, p.StreamName, "1")
	require.Equal(t, p.WrittenFilepath, "2")
	require.Equal(t, p.OutputProtocol, "3")
	require.Equal(t, p.WrittenBytes, 4)
	require.Equal(t, p.WritingDurationSecs, 5)
	require.Equal(t, p.ConnectionStartTimeUnix, 6)
	require.Equal(t, p.ConnectionEndTimeUnix, 7)
	require.Equal(t, p.StreamMediaDurationMillis, 8)
	require.Equal(t, p.FirstMediaTimestampMillis, 9)
	require.Equal(t, p.LastMediaTimestampMillis, 10)
}

func TestItFailsToParseAnInvalidRecordingEndPayload(t *testing.T) {
	var payload = "1\n2\n3\n4\n5\nThis Should Be A Number\n7\n8\n9\n10"
	_, err := ParseRecordingEndPayload(payload)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error parsing line 5 of RECORDING_END payload")
	require.Contains(t, err.Error(), "This Should Be A Number")
}
