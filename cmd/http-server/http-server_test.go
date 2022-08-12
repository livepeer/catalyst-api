package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInitServer(t *testing.T) {
	require := require.New(t)
	router := StartCatalystAPIRouter(nil)

	handle, _, _ := router.Lookup("GET", "/ok")
	require.NotNil(handle)
}
