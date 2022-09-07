package handlers

import (
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (d *CatalystAPIHandlersCollection) Ok() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		io.WriteString(w, "OK")
	}
}
