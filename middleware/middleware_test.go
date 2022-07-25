package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/livepeer/dms-api/handlers"
	"github.com/stretchr/testify/require"
)

func TestNoAuthHeader(t *testing.T) {
	require := require.New(t)

	req, _ := http.NewRequest("GET", "/ok", nil)
	rr := httptest.NewRecorder()
	h := IsAuthorized(handlers.DMSAPIHandlers.Ok())
	h.ServeHTTP(rr, req)

	require.Equal(rr.Code, 401, "should return 401")
	require.Equal(strings.TrimRight(rr.Body.String(), "\n"), `{"error":"No authorization header"}`)
}

func TestWrongKey(t *testing.T) {
	require := require.New(t)

	req, _ := http.NewRequest("GET", "/ok", nil)
	req.Header.Set("Authorization", "Bearer gibberish")

	rr := httptest.NewRecorder()
	h := IsAuthorized(handlers.DMSAPIHandlers.Ok())
	h.ServeHTTP(rr, req)

	require.Equal(rr.Code, 401, "should return 401")
	require.Equal(strings.TrimRight(rr.Body.String(), "\n"), `{"error":"Invalid Token"}`)
}
