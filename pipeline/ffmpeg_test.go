package pipeline

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestItCleansUpLocalFiles(t *testing.T) {
	// Create some temporary files
	f1, err := os.CreateTemp(os.TempDir(), "tempfile1")
	require.NoError(t, err)
	f2, err := os.CreateTemp(os.TempDir(), "tempfile_2")
	require.NoError(t, err)
	f3, err := os.CreateTemp(os.TempDir(), "tempfilethree")
	require.NoError(t, err)
	f4, err := os.CreateTemp(os.TempDir(), "do_not_delete")
	require.NoError(t, err)

	// Try to clean them up
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, cleanUpLocalTmpFiles(os.TempDir(), "tempfile*", time.Microsecond))

	// Confirm that the ones we expected to be deleted are gone
	_, err = os.Stat(f1.Name())
	require.Error(t, err)
	_, err = os.Stat(f2.Name())
	require.Error(t, err)
	_, err = os.Stat(f3.Name())
	require.Error(t, err)

	// Confirm that the ones we expected to not be deleted isn't gone
	_, err = os.Stat(f4.Name())
	require.NoError(t, err)
}
