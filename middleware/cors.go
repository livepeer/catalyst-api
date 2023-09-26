package middleware

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func AllowCORS() func(httprouter.Handle) httprouter.Handle {
	return func(next httprouter.Handle) httprouter.Handle {
		handler := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			// Safari doesn't allow a wildcard for this so we just list them all
			w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE")

			next(w, r, ps)
		}
		return handler
	}
}
