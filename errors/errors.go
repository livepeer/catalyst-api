package errors

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/livepeer/catalyst-api/log"
	"github.com/xeipuuv/gojsonschema"
)

type APIError struct {
	Msg    string `json:"message"`
	Status int    `json:"status"`
	Err    error  `json:"-"`
}

func writeHttpError(w http.ResponseWriter, msg string, status int, err error) APIError {
	w.WriteHeader(status)

	var errorDetail string
	if err != nil {
		errorDetail = err.Error()
	}

	if err := json.NewEncoder(w).Encode(map[string]string{"error": msg, "error_detail": errorDetail}); err != nil {
		log.LogNoRequestID("error writing HTTP error", "http_error_msg", msg, "error", err)
	}
	return APIError{msg, status, err}
}

// HTTP Errors
func WriteHTTPUnauthorized(w http.ResponseWriter, msg string, err error) APIError {
	return writeHttpError(w, msg, http.StatusUnauthorized, err)
}

func WriteHTTPBadRequest(w http.ResponseWriter, msg string, err error) APIError {
	return writeHttpError(w, msg, http.StatusBadRequest, err)
}

func WriteHTTPUnsupportedMediaType(w http.ResponseWriter, msg string, err error) APIError {
	return writeHttpError(w, msg, http.StatusUnsupportedMediaType, err)
}

func WriteHTTPNotFound(w http.ResponseWriter, msg string, err error) APIError {
	return writeHttpError(w, msg, http.StatusNotFound, err)
}

func WriteHTTPInternalServerError(w http.ResponseWriter, msg string, err error) APIError {
	return writeHttpError(w, msg, http.StatusInternalServerError, err)
}

func WriteHTTPBadBodySchema(where string, w http.ResponseWriter, errors []gojsonschema.ResultError) APIError {
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

// Special wrapper for errors that should set the `Unretriable` field in the
// error callback sent on VOD upload jobs.
type UnretriableError struct{ error }

func Unretriable(err error) error {
	return UnretriableError{err}
}

// Returns whether the given error is an unretriable error.
func IsUnretriable(err error) bool {
	return errors.As(err, &UnretriableError{})
}

var (
	EmptyGatingParamError = errors.New("EmptyGatingParamError")
	ObjectNotFoundError   = errors.New("ObjectNotFoundError")
	UnauthorisedError     = errors.New("UnauthorisedError")
	InvalidJWT            = errors.New("InvalidJWTError")
)
