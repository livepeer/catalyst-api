package misttriggers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItErrorsOnBadPushEndPayload(t *testing.T) {
	_, err := ParsePushEndPayload("only\nthree\nlines\n")
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 6 lines in PUSH_END payload but got 3")
}

func TestItCanParseAValidPushEndPayload(t *testing.T) {
	var payload = "1\n2\n3\n4\n5\n6"
	p, err := ParsePushEndPayload(payload)
	require.NoError(t, err)
	require.Equal(t, p.StreamName, "2")
	require.Equal(t, p.Destination, "3")
	require.Equal(t, p.ActualDestination, "4")
	require.Equal(t, p.Last10LogLines, "5")
	require.Equal(t, p.PushStatus, "6")
}
