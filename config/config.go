package config

import (
	"fmt"
	"os"

	"github.com/go-kit/log"
)

var Version string

// Used so that we can generate fixed timestamps in tests
var Clock TimestampGenerator = RealTimestampGenerator{}

// Path to Mist's binaries that we shell out to for transcoding and header file creation
var PathMistDir = "/usr/local/bin"

// Port that the local Broadcaster runs on
const DefaultBroadcasterPort = 8935

var DefaultBroadcasterURL = fmt.Sprintf("http://127.0.0.1:%d", DefaultBroadcasterPort)

const DefaultCustomAPIUrl = "https://origin.livepeer.com/api/"

// Global variable, but easier than passing a logger around throughout the system
var Logger log.Logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

func init() {
	Logger = log.With(Logger, "ts", log.DefaultTimestampUTC)
}

var RecordingCallback string = "http://127.0.0.1:8008/recording/status"
