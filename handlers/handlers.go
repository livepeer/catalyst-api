package handlers

import (
	"io"
	"net/http"
)

type DMSAPIHandlersCollection struct{}

var DMSAPIHandlers = DMSAPIHandlersCollection{}

func (d *DMSAPIHandlersCollection) Ok() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		io.WriteString(w, "OK")
	})
}
