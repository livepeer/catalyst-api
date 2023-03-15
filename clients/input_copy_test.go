package clients

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestItAlwaysReturnsAtLeastTheMinimumTimeout(t *testing.T) {
	// Calculate the timeout for a file that's a single byte in size
	dur := CalculateFileCopyTimeout(1)
	require.Equal(t, time.Hour, dur)
}

func TestItReturnsAtMostTheMinimumTimeout(t *testing.T) {
	// Calculate the timeout for a file that's 40Gb in size
	dur := CalculateFileCopyTimeout(40 * 1024 * 1024)
	require.Equal(t, 8*time.Hour, dur)
}

func TestItReturnsATimeoutBasedOnFileSize(t *testing.T) {
	// Calculate the timeout for a file that's 4Gb in size
	dur := CalculateFileCopyTimeout(4 * 1024 * 1024)
	require.Equal(t, 2*time.Hour, dur)

	// Calculate the timeout for a file that's 7Gb in size
	dur = CalculateFileCopyTimeout(7 * 1024 * 1024)
	require.Equal(t, 3*time.Hour+30*time.Minute, dur)
}
