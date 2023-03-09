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

// Default segment size to produce for HLS streaming
const DefaultSegmentSizeSecs = 10

// Maximum segment size to allow people to override to
const MaxSegmentSizeSecs = 20

// Somewhat arbitrary and conservative number of maximum Catalyst VOD jobs in the system
// at one time. We can look at more sophisticated strategies for calculating capacity in
// the future.
const MAX_JOBS_IN_FLIGHT = 8

// The maximum allowed input file size
const MaxInputFileSizeBytes = 30 * 1024 * 1024 * 1024 // 30 GiB

var TranscodingParallelJobs int = 2

var TranscodingParallelSleep time.Duration = 713 * time.Millisecond

var DownloadOSURLRetries uint64 = 10

var ImportIPFSGatewayURLs []*url.URL = []*url.URL{}

var ImportArweaveGatewayURLs []*url.URL = []*url.URL{}
