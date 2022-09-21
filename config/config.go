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

// Global variable, but easier than passing a logger around throughout the system
var Logger log.Logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

func init() {
	Logger = log.With(Logger, "ts", log.DefaultTimestampUTC)
}

// Prefixes used in Mist stream names to let us determine whether a given "stream" in Mist is being used
// for the segmenting or transcoding phase
const SEGMENTING_PREFIX = "catalyst_vod_"
const SOURCE_PREFIX = "tr_src_"
const RENDITION_PREFIX = "tr_rend_+"
const RECORDING_PREFIX = "video"
