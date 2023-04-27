package handlers

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func PreflightOptionsHandler() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		w.Header().Set("allow", "GET, HEAD, OPTIONS")
		w.Header().Set("content-length", "0")
		w.Header().Set("accept-ranges", "bytes")
		w.WriteHeader(http.StatusOK)
	}
}
