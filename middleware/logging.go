package middleware

import (
	"net/http"
	"runtime/debug"
	"time"

	log "github.com/go-kit/kit/log"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/errors"
)

type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func wrapResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w}
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}

	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
	rw.wroteHeader = true

	return
}

func LogRequest(logger log.Logger) func(httprouter.Handle) httprouter.Handle {
	return func(next httprouter.Handle) httprouter.Handle {
		fn := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			start := time.Now()
			wrapped := wrapResponseWriter(w)

			defer func() {
				if err := recover(); err != nil {
					errors.WriteHTTPInternalServerError(wrapped, "Internal Server Error", nil)
					logger.Log("err", err, "trace", debug.Stack())
				}
			}()

			next(wrapped, r, ps)
			logger.Log(
				"remote", r.RemoteAddr,
				"proto", r.Proto,
				"method", r.Method,
				"uri", r.URL.RequestURI(),
				"duration", time.Since(start),
				"status", wrapped.status,
			)

		}

		return fn
	}
}
