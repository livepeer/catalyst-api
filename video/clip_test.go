package video

import (
	"fmt"
	"strings"
	"testing"

	"github.com/grafov/m3u8"
	"github.com/stretchr/testify/require"
)

const manifestA = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.4160000000,
0.ts
#EXTINF:5.3340000000,
5000.ts
#EXT-X-ENDLIST`

const manifestB = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:EVENT
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PROGRAM-DATE-TIME:2023-06-06T00:27:38.157Z
#EXTINF:5.780,
0.ts
#EXT-X-PROGRAM-DATE-TIME:2023-06-06T00:27:43.937Z
#EXTINF:6.000,
1.ts
#EXT-X-PROGRAM-DATE-TIME:2023-06-06T00:27:49.937Z
#EXTINF:6.000,
2.ts
#EXT-X-PROGRAM-DATE-TIME:2023-06-06T00:27:55.937Z
#EXTINF:1.000,
3.ts
#EXT-X-ENDLIST`

const manifestC = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:EVENT
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
0.ts
1.ts
2.ts
3.ts
#EXT-X-ENDLIST`

// an example of a manifest that ffprobe fails on when
// trying to determine the duration.
const manifestD = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:EVENT
#EXT-X-TARGETDURATION:11.0000000000
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PROGRAM-DATE-TIME:2023-08-28T10:17:20.948Z
#EXTINF:10.634,
source/0.ts
#EXT-X-PROGRAM-DATE-TIME:2023-08-28T10:17:31.582Z
#EXTINF:10.000,
source/1.ts
#EXT-X-PROGRAM-DATE-TIME:2023-08-28T10:17:41.582Z
#EXTINF:10.000,
source/63744.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-05T00:13:30.682Z
#EXTINF:10.000,
source/63745.ts
`

func TestManifestDurationCalculation(t *testing.T) {
	sourceManifestB, _, err := m3u8.DecodeFrom(strings.NewReader(manifestB), true)
	require.NoError(t, err)
	plB := sourceManifestB.(*m3u8.MediaPlaylist)

	dur, segs := GetTotalDurationAndSegments(plB)
	require.Equal(t, 18.78, dur)
	require.Equal(t, uint64(4), segs)

	sourceManifestD, _, err := m3u8.DecodeFrom(strings.NewReader(manifestD), true)
	require.NoError(t, err)
	plD := sourceManifestD.(*m3u8.MediaPlaylist)

	dur, segs = GetTotalDurationAndSegments(plD)
	require.Equal(t, 40.634, dur)
	require.Equal(t, uint64(4), segs)
}

func TestClippingFailsWhenInvalidManifestIsUsed(t *testing.T) {

	sourceManifestC, _, err := m3u8.DecodeFrom(strings.NewReader(manifestC), true)
	require.NoError(t, err)
	plC := sourceManifestC.(*m3u8.MediaPlaylist)

	_, _, err = ClipManifest("1234", plC, 1, 5)
	require.ErrorContains(t, err, "error clipping")
}

func TestClippingSucceedsWhenValidManifestIsUsed(t *testing.T) {
	sourceManifestA, _, err := m3u8.DecodeFrom(strings.NewReader(manifestA), true)
	require.NoError(t, err)
	plA := sourceManifestA.(*m3u8.MediaPlaylist)

	// start/end falls in same segment: ensure only 0.ts is returned
	segs, csi, err := ClipManifest("1234", plA, 1, 5)
	length := len(segs)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(segs[0].SeqId))
	require.Equal(t, 1, length)
	require.Equal(t, 10.4160000000, csi[0].Duration)

	// start/end falls in different segments: ensure only 0.ts and 1.ts is returned
	segs, csi, err = ClipManifest("1234", plA, 1, 10.5)
	length = len(segs)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(segs[0].SeqId))
	require.Equal(t, uint64(1), uint64(segs[1].SeqId))
	require.Equal(t, 2, length)
	require.Equal(t, 10.4160000000, csi[0].Duration)
	require.Equal(t, 5.3340000000, csi[1].Duration)
	require.Equal(t, float64(1), csi[0].ClipOffsetSecs)
	require.Equal(t, fmt.Sprintf("%.3f", 0.084), fmt.Sprintf("%.3f", csi[1].ClipOffsetSecs))

	// start/end with millisecond precision: ensure 0.ts and 1.ts is returned
	segs, csi, err = ClipManifest("1234", plA, 10.416, 10.5)
	length = len(segs)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(segs[0].SeqId))
	require.Equal(t, uint64(1), uint64(segs[1].SeqId))
	require.Equal(t, 2, length)
	require.Equal(t, float64(10.416), csi[0].ClipOffsetSecs)
	require.Equal(t, fmt.Sprintf("%.3f", 0.084), fmt.Sprintf("%.3f", csi[1].ClipOffsetSecs))

	sourceManifestB, _, err := m3u8.DecodeFrom(strings.NewReader(manifestB), true)
	require.NoError(t, err)
	plB := sourceManifestB.(*m3u8.MediaPlaylist)

	// start/end spans the full duration of playlist: ensure 0.ts and 3.ts is returned
	segs, csi, err = ClipManifest("1234", plB, 0, 18.78)
	length = len(segs)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(segs[0].SeqId))
	require.Equal(t, uint64(3), uint64(segs[3].SeqId))
	require.Equal(t, 4, length)
	require.Equal(t, float64(0), csi[0].ClipOffsetSecs)
	require.Equal(t, fmt.Sprintf("%.3f", 1.000), fmt.Sprintf("%.3f", csi[1].ClipOffsetSecs))

	// start exceeds the duration of playlist: ensure no segments are returned
	segs, _, err = ClipManifest("1234", plB, 30, 20.78)
	require.ErrorContains(t, err, "start time specified exceeds duration of manifest")
	require.Equal(t, segs, []*m3u8.MediaSegment(nil))

	// end exceeds the duration of playlist: ensure only 0.ts is returned
	segs, csi, err = ClipManifest("1234", plB, 0, 20.78)
	length = len(segs)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(segs[0].SeqId))
	require.Equal(t, uint64(3), uint64(segs[3].SeqId))
	require.Equal(t, 4, length)
	require.Equal(t, float64(0), csi[0].ClipOffsetSecs)
}
