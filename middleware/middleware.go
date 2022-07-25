package middleware

import (
	"net/http"
	"strings"

	"github.com/livepeer/dms-api/errors"
)

var testToken = "IAmAuthorized"

func IsAuthorized(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")

		if authHeader == "" {
			errors.HTTPUnauthorized(w, "No authorization header", nil)
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")

		if token != testToken {
			errors.HTTPUnauthorized(w, "Invalid Token", nil)
			return
		}

		next(w, r)
	})
}
