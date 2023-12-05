package middleware

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func AllowCORS() func(httprouter.Handle) httprouter.Handle {
	return func(next httprouter.Handle) httprouter.Handle {
		handler := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			originDomain := r.Header.Get("Origin")
			if originDomain == "" {
				originDomain = "*"
			}
			w.Header().Set("Access-Control-Allow-Origin", originDomain)
			w.Header().Set("Access-Control-Allow-Headers", "*")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			// Safari doesn't allow a wildcard for this so we just list them all
			w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE")

			// If this is a preflight request, we don't need to call the next handler
			if r.Method == "OPTIONS" {
				w.Header().Set("allow", "GET, HEAD, OPTIONS")
				w.Header().Set("content-length", "0")
				w.Header().Set("accept-ranges", "bytes")
				w.WriteHeader(http.StatusOK)
				return
			}

			next(w, r, ps)
		}
		return handler
	}
}
