package catalyst

import (
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSystemUsage(t *testing.T) {
	got, err := GetSystemUsage()
	require.NoError(t, err)
	log.Println(got)
	require.Greater(t, got.CPUUsagePercentage, 0.0)
	require.Greater(t, got.RAMUsagePercentage, 0.0)
}
