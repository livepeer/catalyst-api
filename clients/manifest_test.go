package clients

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
)

const validMasterManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-INDEPENDENT-SEGMENTS
#EXT-X-STREAM-INF:BANDWIDTH=2665726,AVERAGE-BANDWIDTH=2526299,RESOLUTION=960x540,FRAME-RATE=29.970,CODECS="avc1.640029,mp4a.40.2",SUBTITLES="subtitles"
index_1.m3u8`

const validMediaManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.4160000000,
0.ts
#EXTINF:5.3340000000,
5000.ts
#EXT-X-ENDLIST`

func DownloadRetryBackoffFailInstantly() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(0*time.Second), 0)
}

func TestDownloadRenditionManifestFailsWhenItCantFindTheManifest(t *testing.T) {
	DownloadRetryBackoff = DownloadRetryBackoffFailInstantly
	defer func() { DownloadRetryBackoff = DownloadRetryBackoffLong }()
	_, err := DownloadRenditionManifest("blah", "/tmp/something/x.m3u8")
	require.Error(t, err)
	require.Contains(t, err.Error(), "the specified file does not exist")
	require.Contains(t, err.Error(), "ObjectNotFoundError")
}

func TestDownloadRenditionManifestFailsWhenItCantParseTheManifest(t *testing.T) {
	DownloadRetryBackoff = DownloadRetryBackoffFailInstantly
	defer func() { DownloadRetryBackoff = DownloadRetryBackoffLong }()
	manifestFile, err := os.CreateTemp(os.TempDir(), "manifest-*.m3u8")
	require.NoError(t, err)
	_, err = manifestFile.WriteString("This isn't a manifest!")
	require.NoError(t, err)

	_, err = DownloadRenditionManifest("blah", manifestFile.Name())
	require.Error(t, err)
	require.Contains(t, err.Error(), "error decoding manifest")
}

func TestDownloadRenditionManifestFailsWhenItReceivesAMasterManifest(t *testing.T) {
	manifestFile, err := os.CreateTemp(os.TempDir(), "manifest-*.m3u8")
	require.NoError(t, err)
	_, err = manifestFile.WriteString(validMasterManifest)
	require.NoError(t, err)

	_, err = DownloadRenditionManifest("blah", manifestFile.Name())
	require.Error(t, err)
	require.Contains(t, err.Error(), "only Media playlists are supported")
}

func TestItCanDownloadAValidRenditionManifest(t *testing.T) {
	manifestFile, err := os.CreateTemp(os.TempDir(), "manifest-*.m3u8")
	require.NoError(t, err)
	_, err = manifestFile.WriteString(validMediaManifest)
	require.NoError(t, err)

	_, err = DownloadRenditionManifest("blah", manifestFile.Name())
	require.NoError(t, err)
}

func TestItCanConvertRelativeURLsToOSURLs(t *testing.T) {
	u, err := ManifestURLToSegmentURL("/tmp/file/something.m3u8", "001.ts")
	require.NoError(t, err)
	require.Equal(t, "/tmp/file/001.ts", u.String())

	u, err = ManifestURLToSegmentURL("s3+https://REDACTED:REDACTED@storage.googleapis.com/something/output.m3u8", "001.ts")
	require.NoError(t, err)
	require.Equal(t, "s3+https://REDACTED:REDACTED@storage.googleapis.com/something/001.ts", u.String())
}

func TestItParsesManifestAndConvertsRelativeURLs(t *testing.T) {
	sourceManifest, _, err := m3u8.DecodeFrom(strings.NewReader(validMediaManifest), true)
	require.NoError(t, err)

	sourceMediaManifest, ok := sourceManifest.(*m3u8.MediaPlaylist)
	require.True(t, ok)

	us, err := GetSourceSegmentURLs("s3+https://REDACTED:REDACTED@storage.googleapis.com/something/output.m3u8", *sourceMediaManifest)
	require.NoError(t, err)

	require.Equal(t, 2, len(us))
	require.Equal(t, "s3+https://REDACTED:REDACTED@storage.googleapis.com/something/0.ts", us[0].URL.String())
	require.Equal(t, "s3+https://REDACTED:REDACTED@storage.googleapis.com/something/5000.ts", us[1].URL.String())
}

func TestItCanGenerateAndWriteManifests(t *testing.T) {
	// Set up the parameters we pass in
	sourceManifest, _, err := m3u8.DecodeFrom(strings.NewReader(validMediaManifest), true)
	require.NoError(t, err)

	sourceMediaPlaylist, ok := sourceManifest.(*m3u8.MediaPlaylist)
	require.True(t, ok)

	outputDir, err := os.MkdirTemp(os.TempDir(), "TestItCanGenerateAndWriteManifests-*")
	require.NoError(t, err)

	// Do the thing
	masterManifestURL, err := GenerateAndUploadManifests(
		*sourceMediaPlaylist,
		outputDir,
		[]*video.RenditionStats{
			{
				Name:          "lowlowlow",
				FPS:           60,
				Width:         800,
				Height:        600,
				BitsPerSecond: 1,
			},
			{
				Name:          "super-high-def",
				FPS:           30,
				Width:         1080,
				Height:        720,
				BitsPerSecond: 1,
			},
			{
				Name:          "bit-more-super-high-def",
				FPS:           30,
				Width:         2560,
				Height:        1440,
				BitsPerSecond: 1,
			},
			{
				Name:          "super-duper-high-def",
				FPS:           30,
				Width:         3240,
				Height:        2160,
				BitsPerSecond: 1,
			},
		},
		false,
	)
	require.NoError(t, err)

	// Confirm we wrote out the master manifest that we expected
	masterManifest := filepath.Join(outputDir, "index.m3u8")
	require.FileExists(t, masterManifest)
	require.Equal(t, masterManifest, masterManifestURL)
	masterManifestContents, err := os.ReadFile(masterManifest)
	require.NoError(t, err)

	const expectedMasterManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=1,RESOLUTION=2560x1440,NAME="0-bit-more-super-high-def",FRAME-RATE=30.000
bit-more-super-high-def/index.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=1,RESOLUTION=3240x2160,NAME="1-super-duper-high-def",FRAME-RATE=30.000
super-duper-high-def/index.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=1,RESOLUTION=1080x720,NAME="2-super-high-def",FRAME-RATE=30.000
super-high-def/index.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=1,RESOLUTION=800x600,NAME="3-lowlowlow",FRAME-RATE=60.000
lowlowlow/index.m3u8
`
	require.Equal(t, expectedMasterManifest, string(masterManifestContents))

	// Confirm we wrote out the rendition manifests that we expected
	require.FileExists(t, filepath.Join(outputDir, "super-high-def/index.m3u8"))
	require.FileExists(t, filepath.Join(outputDir, "lowlowlow/index.m3u8"))
	require.NoFileExists(t, filepath.Join(outputDir, "small-high-def/index.m3u8"))
}

func TestCompliantMasterManifestOrdering(t *testing.T) {
	// Set up the parameters we pass in
	sourceManifest, _, err := m3u8.DecodeFrom(strings.NewReader(validMediaManifest), true)
	require.NoError(t, err)

	sourceMediaPlaylist, ok := sourceManifest.(*m3u8.MediaPlaylist)
	require.True(t, ok)

	outputDir, err := os.MkdirTemp(os.TempDir(), "TestCompliantMasterManifestOrdering-*")
	require.NoError(t, err)

	_, err = GenerateAndUploadManifests(
		*sourceMediaPlaylist,
		outputDir,
		[]*video.RenditionStats{
			{
				Name:          "lowlowlow",
				FPS:           60,
				Width:         800,
				Height:        600,
				BitsPerSecond: 1000000,
			},
			{
				Name:          "medium-high-def",
				FPS:           60,
				Width:         1280,
				Height:        720,
				BitsPerSecond: 1000000,
			},
			{
				Name:          "super-high-def",
				FPS:           30,
				Width:         1080,
				Height:        720,
				BitsPerSecond: 2000000,
			},
			{
				Name:          "small-high-def",
				FPS:           30,
				Width:         800,
				Height:        600,
				BitsPerSecond: 2000000,
			},
		},
		false,
	)
	require.NoError(t, err)

	masterManifest := filepath.Join(outputDir, "index.m3u8")
	masterManifestContents, err := os.ReadFile(masterManifest)
	require.NoError(t, err)
	const expectedMasterManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=2000000,RESOLUTION=800x600,NAME="0-small-high-def",FRAME-RATE=30.000
small-high-def/index.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=2000000,RESOLUTION=1080x720,NAME="1-super-high-def",FRAME-RATE=30.000
super-high-def/index.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=1000000,RESOLUTION=1280x720,NAME="2-medium-high-def",FRAME-RATE=60.000
medium-high-def/index.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=1000000,RESOLUTION=800x600,NAME="3-lowlowlow",FRAME-RATE=60.000
lowlowlow/index.m3u8
`
	require.Equal(t, expectedMasterManifest, string(masterManifestContents))
}

func TestCompliantClippedManifest(t *testing.T) {
	const expectedClippedManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-TARGETDURATION:15
#EXTINF:10.000,blah0
source/0.ts
#EXT-X-DISCONTINUITY
#EXTINF:15.000,blah1
../source/1.ts
#EXT-X-DISCONTINUITY
#EXTINF:10.000,blah2
source/2.ts
#EXT-X-ENDLIST
`

	dummyPlaylist := createDummyMediaPlaylistWithSegments()
	dummyClippedSegs := createDummyMediaSegments()
	clipManifest, err := CreateClippedPlaylist(dummyPlaylist, dummyClippedSegs)
	require.NoError(t, err)
	require.Equal(t, expectedClippedManifest, clipManifest.String())
}

func createDummyMediaPlaylistWithSegments() m3u8.MediaPlaylist {
	segs := createDummyMediaSegments()
	playlist := m3u8.MediaPlaylist{
		TargetDuration:   35.0,
		SeqNo:            0,
		Segments:         segs,
		Args:             "sampleArgs",
		Iframe:           false,
		Closed:           false,
		MediaType:        m3u8.EVENT,
		DiscontinuitySeq: 0,
		StartTime:        0.0,
		StartTimePrecise: false,
	}

	return playlist
}

func createDummyMediaSegments() []*m3u8.MediaSegment {

	layout := "2006-01-02T15:04:05.999999-07:00"
	currentTimeStr := "2023-09-20T23:07:45.854388-07:00"
	currentTime, err := time.Parse(layout, currentTimeStr)
	if err != nil {
		return nil
	}

	return []*m3u8.MediaSegment{
		{
			SeqId:           0,
			Title:           "blah0",
			URI:             "source/0.ts",
			Duration:        10.0,
			Limit:           0,
			Offset:          0,
			Key:             nil,
			Map:             nil,
			Discontinuity:   false,
			SCTE:            nil,
			ProgramDateTime: currentTime,
		},
		{
			SeqId:           1,
			Title:           "blah1",
			URI:             "source/1.ts",
			Duration:        15.0,
			Limit:           0,
			Offset:          0,
			Key:             nil,
			Map:             nil,
			Discontinuity:   false,
			SCTE:            nil,
			ProgramDateTime: currentTime.Add(10 * time.Second),
		},
		{
			SeqId:           2,
			Title:           "blah2",
			URI:             "source/2.ts",
			Duration:        10.0,
			Limit:           0,
			Offset:          0,
			Key:             nil,
			Map:             nil,
			Discontinuity:   false,
			SCTE:            nil,
			ProgramDateTime: currentTime.Add(15 * time.Second),
		},
	}
}

func TestDownloadRenditionManifestWithBackup(t *testing.T) {
	completeManifest := `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-TARGETDURATION:10
#EXTINF:10.000000,
seg-0.ts
#EXTINF:10.000000,
seg-1.ts
#EXTINF:10.000000,
seg-2.ts
#EXTINF:10.000000,
seg-3.ts
#EXT-X-ENDLIST
`
	inCompleteManifest := `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-TARGETDURATION:10
#EXTINF:10.000000,
seg-0.ts
#EXTINF:10.000000,
seg-1.ts
#EXT-X-ENDLIST
`

	tests := []struct {
		name            string
		primaryManifest string
		backupManifest  string
		primarySegments []string
		backupSegments  []string
	}{
		{
			name:            "happy. all segments and manifest available on primary",
			primaryManifest: completeManifest,
			backupManifest:  "",
			primarySegments: []string{"seg-0.ts", "seg-1.ts", "seg-2.ts", "seg-3.ts"},
		},
		{
			name:            "all segments and manifest available on backup",
			primaryManifest: inCompleteManifest,
			backupManifest:  completeManifest,
			backupSegments:  []string{"seg-0.ts", "seg-1.ts", "seg-2.ts", "seg-3.ts"},
		},
		{
			name:            "all segments on backup and newest manifest on primary",
			primaryManifest: completeManifest,
			backupManifest:  inCompleteManifest,
			backupSegments:  []string{"seg-0.ts", "seg-1.ts", "seg-2.ts", "seg-3.ts"},
		},
		{
			name:            "all segments on primary and newest manifest on backup",
			primaryManifest: inCompleteManifest,
			backupManifest:  completeManifest,
			primarySegments: []string{"seg-0.ts", "seg-1.ts", "seg-2.ts", "seg-3.ts"},
		},
		{
			name:            "segments split between primary and backup, newest manifest on primary",
			primaryManifest: completeManifest,
			backupManifest:  inCompleteManifest,
			primarySegments: []string{"seg-0.ts", "seg-2.ts"},
			backupSegments:  []string{"seg-1.ts", "seg-3.ts"},
		},
		{
			name:            "segments split between primary and backup, newest manifest on backup",
			primaryManifest: inCompleteManifest,
			backupManifest:  completeManifest,
			primarySegments: []string{"seg-0.ts", "seg-2.ts"},
			backupSegments:  []string{"seg-1.ts", "seg-3.ts"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := os.MkdirTemp(os.TempDir(), "manifest-test-*")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			err = os.Mkdir(filepath.Join(dir, "primary"), 0755)
			require.NoError(t, err)
			err = os.Mkdir(filepath.Join(dir, "backup"), 0755)
			require.NoError(t, err)
			config.StorageFallbackURLs = map[string]string{filepath.Join(dir, "primary"): filepath.Join(dir, "backup")}

			err = os.WriteFile(filepath.Join(dir, "primary", "index.m3u8"), []byte(tt.primaryManifest), 0644)
			require.NoError(t, err)
			if tt.backupManifest != "" {
				err = os.WriteFile(filepath.Join(dir, "backup", "index.m3u8"), []byte(tt.backupManifest), 0644)
				require.NoError(t, err)
			}

			for _, segment := range tt.primarySegments {
				err = os.WriteFile(filepath.Join(dir, "primary", segment), []byte{}, 0644)
				require.NoError(t, err)
			}
			for _, segment := range tt.backupSegments {
				err = os.WriteFile(filepath.Join(dir, "backup", segment), []byte{}, 0644)
				require.NoError(t, err)
			}

			renditionUrl, err := RecordingBackupCheck("requestID", toUrl(t, filepath.Join(dir, "primary", "index.m3u8")), toUrl(t, filepath.Join(dir, "transfer")))
			require.NoError(t, err)

			file, err := os.Open(renditionUrl.String())
			require.NoError(t, err)

			// read resulting playlist
			playlist, playlistType, err := m3u8.DecodeFrom(file, true)
			require.NoError(t, err)
			require.Equal(t, m3u8.MEDIA, playlistType)
			mediaPlaylist, ok := playlist.(*m3u8.MediaPlaylist)
			require.True(t, ok)

			require.Len(t, mediaPlaylist.GetAllSegments(), 4)
			for i, segment := range mediaPlaylist.GetAllSegments() {
				require.True(t, filepath.IsAbs(segment.URI))
				require.True(t, true, strings.HasSuffix(segment.URI, fmt.Sprintf("seg-%d.ts", i)))
			}
		})
	}
}

func toUrl(t *testing.T, in string) *url.URL {
	u, err := url.Parse(in)
	require.NoError(t, err)
	return u
}
