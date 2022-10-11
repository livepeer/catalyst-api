package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/handlers"
	"github.com/stretchr/testify/require"
)

func TestNoAuthHeader(t *testing.T) {
	require := require.New(t)

	router := httprouter.New()
	req, _ := http.NewRequest("GET", "/ok", nil)
	rr := httptest.NewRecorder()
	catalystApiHandlers := handlers.CatalystAPIHandlersCollection{}
	router.GET("/ok", IsAuthorized("IAmAuthorized", catalystApiHandlers.Ok()))
	router.ServeHTTP(rr, req)

	require.Equal(rr.Code, 401, "should return 401")
	require.JSONEq(rr.Body.String(), `{"error":"No authorization header", "error_detail":""}`)
}

func TestWrongKey(t *testing.T) {
	require := require.New(t)

	router := httprouter.New()
	req, _ := http.NewRequest("GET", "/ok", nil)
	req.Header.Set("Authorization", "Bearer gibberish")

	rr := httptest.NewRecorder()
	catalystApiHandlers := handlers.CatalystAPIHandlersCollection{}
	router.GET("/ok", IsAuthorized("IAmAuthorized", catalystApiHandlers.Ok()))
	router.ServeHTTP(rr, req)

	require.Equal(rr.Code, 401, "should return 401")
	require.JSONEq(rr.Body.String(), `{"error":"Invalid Token", "error_detail":""}`)
}
