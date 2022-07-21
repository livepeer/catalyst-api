package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOKHandler(t *testing.T) {
	require := require.New(t)

	req, _ := http.NewRequest("GET", "/ok", nil)
	rr := httptest.NewRecorder()
	h := DMSAPIHandlers.Ok()
	h.ServeHTTP(rr, req)

	require.Equal(rr.Body.String(), "OK")
}
