package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitServer(t *testing.T) {
	require := require.New(t)

	const listen = "localhost:8081"
	router := StartDMSAPIRouter()

	handle, _, _ := router.Lookup("GET", "/ok")
	require.NotNil(handle)
}
