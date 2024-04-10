package handlers

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItReturnsA200WithSuccessBody(t *testing.T) {
	handlers := CatalystAPIHandlersCollection{}
	healthcheckHandler := handlers.Healthcheck()

	resp := httptest.NewRecorder()
	healthcheckHandler(resp, nil, nil)

	require.Equal(t, 200, resp.Code)
	require.Equal(t, resp.Body.String(), `{"status":"healthy"}`)
}
