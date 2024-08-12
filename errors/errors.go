package errors

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/log"
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
		switch status {
		case http.StatusInternalServerError:
			log.LogNoRequestID("returning HTTP 500", "http_error_msg", msg, "err", err)
		default:
			errorDetail = err.Error()
		}
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

type unretriableError struct{ error }

// Unretriable returns an error that should be treated as final. This effectively means that the error stops backoff
// retry loops automatically and that it should be propagated back to the caller as such. This is done through the
// status callback through the "unretriable" field.
func Unretriable(err error) error {
	// Notice that permanent errors get unwrapped by the backoff lib when they're used to stop the retry loop. So we need
	// to keep the unretriableError inside it so it's propagated upstream.
	return backoff.Permanent(unretriableError{err})
}

// IsUnretriable returns whether the given error is an unretriable error.
func IsUnretriable(err error) bool {
	return errors.As(err, &unretriableError{})
}

func (e unretriableError) Unwrap() error {
	return e.error
}

type ObjectNotFoundError struct {
	msg   string
	cause error
}

func (e ObjectNotFoundError) Error() string {
	return e.msg
}

func (e ObjectNotFoundError) Unwrap() error {
	return e.cause
}

func NewObjectNotFoundError(msg string, cause error) error {
	if cause != nil {
		msg = fmt.Sprintf("ObjectNotFoundError: %s: %s", msg, cause)
	} else {
		msg = fmt.Sprintf("ObjectNotFoundError: %s", msg)
	}

	// we want 404s to be unretriable at the studio task level but we still want retries at the catalyst-api app level
	// so we don't use backoff.Permanent or the Unretriable func which uses backoff.Permanent
	return unretriableError{ObjectNotFoundError{msg: msg, cause: cause}}
}

// IsObjectNotFound checks if the error is an ObjectNotFoundError.
func IsObjectNotFound(err error) bool {
	return errors.As(err, &ObjectNotFoundError{})
}

var (
	UnauthorisedError = errors.New("UnauthorisedError")
	InvalidJWT        = errors.New("InvalidJWTError")
)
