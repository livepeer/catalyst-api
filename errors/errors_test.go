package errors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
)

func TestIsObjectNotFound(t *testing.T) {
	err := NewObjectNotFoundError("foo", fmt.Errorf("bar"))
	require.True(t, IsObjectNotFound(err))
	require.True(t, IsUnretriable(err))
	var permErr *backoff.PermanentError
	require.False(t, errors.As(err, &permErr))
}

func TestUnretriable(t *testing.T) {
	err := Unretriable(fmt.Errorf("bar"))
	require.True(t, IsUnretriable(err))
	var permErr *backoff.PermanentError
	require.True(t, errors.As(err, &permErr))
}
