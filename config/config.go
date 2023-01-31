package config

import (
	"fmt"
	"net/url"
	"time"
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

var RecordingCallback string = "http://127.0.0.1:8008/recording/status"

var TranscodingParallelJobs int = 2

var TranscodingParallelSleep time.Duration = 713 * time.Millisecond

var DownloadOSURLRetries uint64 = 10

var ImportIPFSGatewayURLs []*url.URL = []*url.URL{}

var ImportArweaveGatewayURLs []*url.URL = []*url.URL{}

var UcanKey string
