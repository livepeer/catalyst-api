package config

import (
	"os"

	"github.com/go-kit/log"
)

var Version string

// Used so that we can generate fixed timestamps in tests
var Clock TimestampGenerator = RealTimestampGenerator{}

// Path to Mist's "Livepeer" process that we shell out to for the transcoding
const PathMistProcLivepeer = "./MistProcLivepeer"

// Port that the local Broadcaster runs on
const BroadcasterPort = 8935

// Global variable, but easier than passing a logger around throughout the system
var Logger log.Logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

func init() {
	Logger = log.With(Logger, "ts", log.DefaultTimestampUTC)
}
