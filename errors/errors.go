package errors

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/xeipuuv/gojsonschema"
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

func WriteHTTPBadBodySchema(where string, w http.ResponseWriter, errors []gojsonschema.ResultError) apiError {
	sb := strings.Builder{}
	sb.WriteString("Body validation error in ")
	sb.WriteString(where)
	sb.WriteString(" ")
	for i := 0; i < len(errors); i++ {
		sb.WriteString(errors[i].String())
		sb.WriteString(" ")
	}
	return writeHttpError(w, sb.String(), http.StatusBadRequest, nil)
}
