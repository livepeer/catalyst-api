package middleware

import (
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/errors"
)

var testToken = "IAmAuthorized"

func IsAuthorized(next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		authHeader := r.Header.Get("Authorization")

		if authHeader == "" {
			errors.WriteHTTPUnauthorized(w, "No authorization header", nil)
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")

		if token != testToken {
			errors.WriteHTTPUnauthorized(w, "Invalid Token", nil)
			return
		}

		next(w, r, ps)
	}
}
