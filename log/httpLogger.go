package log

import (
	"github.com/golang/glog"
	"github.com/hashicorp/go-retryablehttp"
)

var _ retryablehttp.LeveledLogger = retryableHTTPLogger{}

type retryableHTTPLogger struct {
}

func NewRetryableHTTPLogger() retryablehttp.LeveledLogger {
	return retryableHTTPLogger{}
}

func (r retryableHTTPLogger) Error(msg string, keysAndValues ...interface{}) {
	if glog.V(3) {
		LogNoRequestID(msg, keysAndValues...)
	}
}

func (r retryableHTTPLogger) Warn(msg string, keysAndValues ...interface{}) {
	if glog.V(4) {
		LogNoRequestID(msg, keysAndValues...)
	}
}

func (r retryableHTTPLogger) Info(msg string, keysAndValues ...interface{}) {
	if glog.V(5) {
		LogNoRequestID(msg, keysAndValues...)
	}
}

func (r retryableHTTPLogger) Debug(msg string, keysAndValues ...interface{}) {
	if glog.V(6) {
		LogNoRequestID(msg, keysAndValues...)
	}
}
