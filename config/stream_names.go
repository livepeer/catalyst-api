package config

import (
	"fmt"
	"math/rand"
	"strings"
)

// Prefixes used in Mist stream names to let us determine whether a given "stream" in Mist is being used
// for the segmenting or transcoding phase
const SOURCE_PREFIX = "tr_src_"
const RENDITION_PREFIX = "tr_rend_+"
const SEGMENTING_PREFIX = "catalyst_vod_"
const RECORDING_PREFIX = "video"

func IsTranscodeStream(streamName string) bool {
	return strings.HasPrefix(streamName, RENDITION_PREFIX)
}

func SegmentingStreamName(requestID string) string {
	return fmt.Sprintf("%s%s", SEGMENTING_PREFIX, requestID)
}

func RandomTrailer(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = charset[rand.Intn(len(charset))]
	}
	return string(res)
}
