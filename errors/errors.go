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

func writeHttpError(w http.ResponseWriter, msg string, status int, err error) apiError {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
	if err != nil {
		log.Println(msg, err)
	}
	return apiError{msg, status, err}
}

// HTTP Errors
func WriteHTTPUnauthorized(w http.ResponseWriter, msg string, err error) apiError {
	return writeHttpError(w, msg, http.StatusUnauthorized, err)
}

func WriteHTTPBadRequest(w http.ResponseWriter, msg string, err error) apiError {
	return writeHttpError(w, msg, http.StatusBadRequest, err)
}

func WriteHTTPUnsupportedMediaType(w http.ResponseWriter, msg string, err error) apiError {
	return writeHttpError(w, msg, http.StatusUnsupportedMediaType, err)
}

func WriteHTTPInternalServerError(w http.ResponseWriter, msg string, err error) apiError {
	return writeHttpError(w, msg, http.StatusInternalServerError, err)
}
