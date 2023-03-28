package middleware

import (
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
	"time"
)

func TestShellFailsWithIncorrectTimeFields(t *testing.T) {
	_, err := NewShell(-2, 10*time.Second, "echo", "hello world")
	require.ErrorContains(t, err, "shell needs to be set with a valid interval")

	_, err = NewShell(10*time.Second, -1, "echo", "hello world")
	require.ErrorContains(t, err, "shell needs a valid timeout")
}

func TestShellKilledWithTimeout(t *testing.T) {
	app, err := NewShell(10*time.Second, 1*time.Second, "sleep", "5")
	require.NoError(t, nil, err)
	err = app.Run()
	require.ErrorContains(t, err, "signal: killed")
}

func TestShellGetScheduledWithInterval(t *testing.T) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "shell.test")
	defer os.Remove(tmpFile.Name())
	require.NoError(t, nil, err)

	// schedule every 2s, with a timeout of 1s
	app, err := NewShell(2*time.Second, 1*time.Second, "sh", "-c", "echo 'Hello, Clarice' >> "+tmpFile.Name())
	require.NoError(t, nil, err)
	tick := app.RunBg()
	defer tick.Stop()
	time.Sleep(5 * time.Second)

	dat, err := os.ReadFile(tmpFile.Name())
	require.NoError(t, nil, err)
	count := strings.Count(string(dat), "Hello")
	require.Equal(t, count, 2)
}
