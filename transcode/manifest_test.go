package transcode

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSourceSegmentsURLsFailsWhenItCantParseTheManifest(t *testing.T) {
	// Check it fails with a parsing error
	_, err := GetSourceSegmentURLs("/tmp/something/x.m3u8", strings.NewReader("This isn't a manifest file!"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "error decoding manifest")
}

func TestItCanConvertRelativeURLsToOSURLs(t *testing.T) {
	u := manifestURLToSegmentURL("/tmp/file/something.m3u8", "001.ts")
	require.Equal(t, "/tmp/file/001.ts", u)

	u = manifestURLToSegmentURL("s3+https://REDACTED:REDACTED@storage.googleapis.com/something/output.m3u8", "001.ts")
	require.Equal(t, "s3+https://REDACTED:REDACTED@storage.googleapis.com/something/001.ts", u)
}
