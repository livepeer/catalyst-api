package video

import (
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

const manifestE = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:EVENT
#EXT-X-TARGETDURATION:17.0000000000
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T16:58:41.271Z
#EXTINF:16.850,
source/0.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T16:58:58.121Z
#EXTINF:10.427,
source/1.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T16:59:08.548Z
#EXTINF:10.427,
source/2.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T16:59:18.975Z
#EXTINF:10.427,
source/3.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T16:59:29.402Z
#EXTINF:10.428,
source/4.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T16:59:39.830Z
#EXTINF:10.427,
source/5.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T16:59:50.257Z
#EXTINF:10.427,
source/6.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:00:00.684Z
#EXTINF:10.427,
source/7.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:00:11.111Z
#EXTINF:10.427,
source/8.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:00:21.538Z
#EXTINF:10.427,
source/9.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:00:31.965Z
#EXTINF:15.974,
source/10.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:00:47.939Z
#EXTINF:12.972,
source/11.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:01:00.911Z
#EXTINF:20.061,
source/12.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:01:20.972Z
#EXTINF:14.598,
source/13.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:01:35.570Z
#EXTINF:14.598,
source/14.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:01:50.168Z
#EXTINF:12.888,
source/15.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:02:03.056Z
#EXTINF:18.685,
source/16.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:02:21.741Z
#EXTINF:10.427,
source/17.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:02:32.168Z
#EXTINF:14.098,
source/18.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:02:46.266Z
#EXTINF:10.427,
source/19.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:02:56.693Z
#EXTINF:10.427,
source/20.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:03:07.120Z
#EXTINF:14.848,
source/21.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:03:21.968Z
#EXTINF:10.427,
source/22.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:03:32.395Z
#EXTINF:10.386,
source/23.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:03:42.781Z
#EXTINF:10.719,
source/24.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:03:53.500Z
#EXTINF:13.346,
source/25.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:04:06.846Z
#EXTINF:15.182,
source/26.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:04:22.028Z
#EXTINF:13.472,
source/27.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:04:35.500Z
#EXTINF:12.262,
source/28.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:04:47.762Z
#EXTINF:17.351,
source/29.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:05:05.113Z
#EXTINF:14.973,
source/30.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:05:20.086Z
#EXTINF:10.969,
source/31.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:05:31.055Z
#EXTINF:11.595,
source/32.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:05:42.650Z
#EXTINF:12.304,
source/33.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:05:54.954Z
#EXTINF:18.394,
source/34.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:06:13.348Z
#EXTINF:10.260,
source/35.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:06:23.608Z
#EXTINF:11.303,
source/36.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:06:34.911Z
#EXTINF:14.389,
source/37.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:06:49.300Z
#EXTINF:11.303,
source/38.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:07:00.603Z
#EXTINF:14.431,
source/39.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:07:15.034Z
#EXTINF:10.427,
source/40.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:07:25.461Z
#EXTINF:11.929,
source/41.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:07:37.390Z
#EXTINF:15.265,
source/42.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:07:52.655Z
#EXTINF:15.391,
source/43.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:08:08.046Z
#EXTINF:15.140,
source/44.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:08:23.186Z
#EXTINF:17.601,
source/45.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:08:40.787Z
#EXTINF:12.762,
source/46.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:08:53.549Z
#EXTINF:12.763,
source/47.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:09:06.312Z
#EXTINF:10.427,
source/48.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:09:16.739Z
#EXTINF:11.345,
source/49.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:09:28.084Z
#EXTINF:18.810,
source/50.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:09:46.894Z
#EXTINF:13.597,
source/51.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:10:00.491Z
#EXTINF:10.386,
source/52.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:10:10.877Z
#EXTINF:10.385,
source/53.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:10:21.262Z
#EXTINF:10.385,
source/54.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:10:31.647Z
#EXTINF:10.928,
source/55.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:10:42.575Z
#EXTINF:10.427,
source/56.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:10:53.002Z
#EXTINF:14.806,
source/57.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:11:07.808Z
#EXTINF:10.344,
source/58.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:11:18.152Z
#EXTINF:17.768,
source/59.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:11:35.920Z
#EXTINF:13.555,
source/60.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:11:49.475Z
#EXTINF:12.096,
source/61.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:12:01.571Z
#EXTINF:10.885,
source/62.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:12:12.456Z
#EXTINF:10.427,
source/63.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:12:22.883Z
#EXTINF:12.221,
source/64.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:12:35.104Z
#EXTINF:14.264,
source/65.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:12:49.368Z
#EXTINF:12.554,
source/66.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:13:01.922Z
#EXTINF:11.971,
source/67.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:13:13.893Z
#EXTINF:10.385,
source/68.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:13:24.278Z
#EXTINF:10.594,
source/69.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:13:34.872Z
#EXTINF:10.219,
source/70.ts
#EXT-X-PROGRAM-DATE-TIME:2023-09-12T17:13:45.091Z
#EXTINF:3.105,
source/71.ts
#EXT-X-ENDLIST`

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

	_, err = ClipManifest("1234", plC, 1, 5)
	require.ErrorContains(t, err, "error clipping")
}

func TestClippingSucceedsWhenValidManifestIsUsed(t *testing.T) {
	sourceManifestA, _, err := m3u8.DecodeFrom(strings.NewReader(manifestA), true)
	require.NoError(t, err)
	plA := sourceManifestA.(*m3u8.MediaPlaylist)

	// start/end falls in same segment: ensure only 0.ts is returned
	segs, err := ClipManifest("1234", plA, 1, 5)
	length := len(segs)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(segs[0].SeqId))
	require.Equal(t, 1, length)

	// start/end falls in different segments: ensure only 0.ts and 1.ts is returned
	segs, err = ClipManifest("1234", plA, 1, 10.5)
	length = len(segs)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(segs[0].SeqId))
	require.Equal(t, uint64(1), uint64(segs[1].SeqId))
	require.Equal(t, 2, length)

	// start/end with millisecond precision: ensure 0.ts and 1.ts is returned
	segs, err = ClipManifest("1234", plA, 10.416, 10.5)
	length = len(segs)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(segs[0].SeqId))
	require.Equal(t, uint64(1), uint64(segs[1].SeqId))
	require.Equal(t, 2, length)

	sourceManifestB, _, err := m3u8.DecodeFrom(strings.NewReader(manifestB), true)
	require.NoError(t, err)
	plB := sourceManifestB.(*m3u8.MediaPlaylist)

	// start/end spans the full duration of playlist: ensure 0.ts and 3.ts is returned
	segs, err = ClipManifest("1234", plB, 0, 18.78)
	length = len(segs)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(segs[0].SeqId))
	require.Equal(t, uint64(3), uint64(segs[3].SeqId))
	require.Equal(t, 4, length)

	// start exceeds the duration of playlist: ensure no segments are returned
	segs, err = ClipManifest("1234", plB, 30, 20.78)
	require.ErrorContains(t, err, "start time specified exceeds duration of manifest")
	require.Equal(t, segs, []*m3u8.MediaSegment(nil))

	// end exceeds the duration of playlist: ensure only 0.ts is returned
	segs, err = ClipManifest("1234", plB, 0, 20.78)
	length = len(segs)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(segs[0].SeqId))
	require.Equal(t, uint64(3), uint64(segs[3].SeqId))
	require.Equal(t, 4, length)
}
