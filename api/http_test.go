package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitServer(t *testing.T) {
	require := require.New(t)
	router := NewCatalystAPIRouter(nil)

	handle, _, _ := router.Lookup("GET", "/ok")
	require.NotNil(handle)
}
