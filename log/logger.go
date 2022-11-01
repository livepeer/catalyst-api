package log

import (
	"os"
	"time"

	"github.com/go-kit/log"
	kitlog "github.com/go-kit/log"
	"github.com/patrickmn/go-cache"
)

var loggerCache *cache.Cache
var default_logger_cache_expiry = 6 * time.Hour

func init() {
	loggerCache = cache.New(default_logger_cache_expiry, 10*time.Minute)
}

// Permanently add context to the logger. Any future logging for this Request ID will include this context
func AddContext(requestID string, keyvals ...interface{}) {
	_ = loggerCache.Add(requestID, kitlog.With(getLogger(requestID), keyvals...), default_logger_cache_expiry)
}

func Log(requestID string, message string, keyvals ...interface{}) {
	_ = kitlog.With(getLogger(requestID), "msg", message).Log(keyvals...)
}

// Log in situations where we don't have access to the Request ID.
// Should be used sparingly and with as much context inserted into the message as possible
func LogNoRequestID(message string, keyvals ...interface{}) {
	_ = kitlog.With(newLogger(), "msg", message).Log(keyvals...)
}

func LogError(requestID string, message string, err error, keyvals ...interface{}) {
	msgLogger := kitlog.With(getLogger(requestID), "msg", message)
	errLogger := kitlog.With(msgLogger, "err", err.Error())
	_ = errLogger.Log(keyvals...)
}

func getLogger(requestID string) kitlog.Logger {
	logger, found := loggerCache.Get(requestID)
	if found {
		return logger.(kitlog.Logger)
	}

	newLogger := kitlog.With(newLogger(), "request_id", requestID)
	err := loggerCache.Add(requestID, newLogger, default_logger_cache_expiry)
	if err != nil {
		_ = newLogger.Log("msg", "error adding logger to cache", "request_id", requestID)
	}
	return newLogger
}

func newLogger() kitlog.Logger {
	newLogger := kitlog.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	return kitlog.With(newLogger, "ts", kitlog.DefaultTimestampUTC)
}
