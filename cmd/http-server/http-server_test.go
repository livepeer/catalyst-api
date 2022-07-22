package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitServer(t *testing.T) {
	require := require.New(t)

	const listen = "localhost:8081"
	server := StartDMSAPIServer(listen)

	require.Equal(server.Addr, listen)

	server.Close()
}
