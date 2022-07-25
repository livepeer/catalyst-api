package errors

import (
	"encoding/json"
	"log"
	"net/http"
)

type apiError struct {
	Msg    string `json:"message"`
	Status int    `json:"status"`
	Err    error  `json:"-"`
}

func httpError(w http.ResponseWriter, msg string, status int, err error) apiError {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
	if err != nil {
		log.Println(msg, err)
	}
	return apiError{msg, status, err}
}

// HTTP Errors
func HTTPUnauthorized(w http.ResponseWriter, msg string, err error) apiError {
	return httpError(w, msg, http.StatusUnauthorized, err)
}
