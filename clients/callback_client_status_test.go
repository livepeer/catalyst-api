package clients

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItCanMarshalStatusToAString(t *testing.T) {
	var statuses = []TranscodeStatus{
		TranscodeStatusError,
		TranscodeStatusCompleted,
	}

	jsonBytes, err := json.Marshal(statuses)
	require.NoError(t, err)

	require.JSONEq(t, `["error", "success"]`, string(jsonBytes))
}

func TestItCanUnmarshalStatusJSON(t *testing.T) {
	var statusList []TranscodeStatus
	err := json.Unmarshal([]byte(`["preparing", "success"]`), &statusList)
	require.NoError(t, err)

	require.Equal(
		t,
		[]TranscodeStatus{TranscodeStatusPreparing, TranscodeStatusCompleted},
		statusList,
	)
}
