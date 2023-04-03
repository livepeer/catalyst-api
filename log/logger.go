package log

import (
	"net/url"
	"os"
	"strings"
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
	logger := kitlog.With(getLogger(requestID), redactKeyvals(keyvals...)...)

	err := loggerCache.Replace(requestID, logger, default_logger_cache_expiry)
	if err != nil {
		_ = logger.Log("msg", "error replacing logger in cache: "+err.Error())
	}
}

func Log(requestID string, message string, keyvals ...interface{}) {
	_ = kitlog.With(getLogger(requestID), "msg", message).Log(redactKeyvals(keyvals...)...)
}

// Log in situations where we don't have access to the Request ID.
// Should be used sparingly and with as much context inserted into the message as possible
func LogNoRequestID(message string, keyvals ...interface{}) {
	_ = kitlog.With(newLogger(), "msg", message).Log(redactKeyvals(keyvals...)...)
}

func LogError(requestID string, message string, err error, keyvals ...interface{}) {
	msgLogger := kitlog.With(getLogger(requestID), "msg", message)
	errLogger := kitlog.With(msgLogger, "err", err.Error())
	_ = errLogger.Log(redactKeyvals(keyvals...)...)
}

func getLogger(requestID string) kitlog.Logger {
	logger, found := loggerCache.Get(requestID)
	if found {
		return logger.(kitlog.Logger)
	}

	newLogger := kitlog.With(newLogger(), "request_id", requestID)
	err := loggerCache.Add(requestID, newLogger, default_logger_cache_expiry)
	if err != nil {
		_ = newLogger.Log("msg", "error adding logger to cache", "request_id", requestID, "err", err.Error())
	}
	return newLogger
}

func newLogger() kitlog.Logger {
	newLogger := kitlog.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	return kitlog.With(newLogger, "ts", kitlog.DefaultTimestampUTC)
}

func redactKeyvals(keyvals ...interface{}) []interface{} {
	var res []interface{}
	for i := range keyvals {
		if i%2 == 1 {
			k, v := keyvals[i-1], keyvals[i]
			res = append(res, k)
			switch s := v.(type) {
			case string:
				res = append(res, RedactURL(s))
			case url.URL:
				res = append(res, s.Redacted())
			case *url.URL:
				if s != nil {
					res = append(res, s.Redacted())
				}
			default:
				res = append(res, v)
			}
		}
	}
	return res
}

func RedactLogs(str, delim string) string {
	if delim == "" {
		return str
	}

	splitstr := strings.Split(str, delim)
	if len(splitstr) == 1 {
		return str
	}

	redactedstr := []string{}
	for _, v := range splitstr {
		r := RedactURL(v)
		redactedstr = append(redactedstr, r)
	}
	return strings.Join(redactedstr[:], delim)
}

func RedactURL(str string) string {
	strLower := strings.ToLower(str)
	if !strings.HasPrefix(strLower, "http") && !strings.HasPrefix(strLower, "s3") {
		return str
	}

	u, err := url.Parse(str)
	if err != nil {
		return "REDACTED"
	}
	return u.Redacted()
}
