package transcode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const exampleMediaManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.4160000000,
0.ts
#EXTINF:5.3340000000,
5000.ts
#EXTINF:6.0000000000,
10000.ts
#EXT-X-ENDLIST`

// Create 2 layers of subdirectories to ensure runs of the test don't interfere with each other
// and that it simulates the production layout
var testDataDir = filepath.Join(os.TempDir(), "unit-test-dir-"+config.RandomTrailer(8))

type StubBroadcasterClient struct {
	tr clients.TranscodeResult
}

func (c StubBroadcasterClient) TranscodeSegment(segment io.Reader, sequenceNumber int64, durationMillis int64, manifestID string, conf clients.LivepeerTranscodeConfiguration) (clients.TranscodeResult, error) {
	return c.tr, nil
}

func TestItCanTranscode(t *testing.T) {
	dir := filepath.Join(testDataDir, "it-can-transcode")
	inputDir := filepath.Join(dir, "input")
	err := os.MkdirAll(inputDir, os.ModePerm)
	require.NoError(t, err)

	// Create temporary manifest + segment files on the local filesystem
	manifestFile, err := os.CreateTemp(inputDir, "index.m3u8")
	require.NoError(t, err)

	segment0, err := os.Create(inputDir + "/0.ts")
	require.NoError(t, err)

	segment1, err := os.Create(inputDir + "/5000.ts")
	require.NoError(t, err)

	segment2, err := os.Create(inputDir + "/10000.ts")
	require.NoError(t, err)

	totalSegments := 0
	err = filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(info.Name(), ".ts") {
			totalSegments++
		}

		return nil
	})
	require.NoError(t, err)

	// Write some data to it
	_, err = manifestFile.WriteString(exampleMediaManifest)
	require.NoError(t, err)
	_, err = segment0.WriteString("segment data")
	require.NoError(t, err)
	_, err = segment1.WriteString("lots of segment data")
	require.NoError(t, err)
	_, err = segment2.WriteString("and all your base are belong to us")
	require.NoError(t, err)

	// Set up a server to receive callbacks and store them in an array for future verification
	var callbacks []map[string]interface{}
	var callbacksLock = sync.Mutex{}
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbacksLock.Lock()
		defer callbacksLock.Unlock()
		// Check that we got the callback we're expecting
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var callback map[string]interface{}
		err = json.Unmarshal(body, &callback)
		require.NoError(t, err)
		callbacks = append(callbacks, callback)
	}))
	defer callbackServer.Close()

	sourceVideoTrack := video.VideoTrack{
		Width:  2020,
		Height: 2020,
	}
	// Set up a fake Broadcaster that returns the rendition segments we'd expect based on the
	// transcode request we send in the next step
	localBroadcaster := StubBroadcasterClient{
		tr: clients.TranscodeResult{
			Renditions: []*clients.RenditionSegment{
				{
					Name:      "low-bitrate",
					MediaData: make([]byte, 512*1024),
				},
				{
					Name:      strconv.FormatInt(int64(sourceVideoTrack.Height), 10) + "p0",
					MediaData: make([]byte, 3*1024*1024),
				},
			},
		},
	}

	statusClient := clients.NewPeriodicCallbackClient(100*time.Minute, map[string]string{})
	// Check we don't get an error downloading or parsing it
	outputs, segmentsCount, err := RunTranscodeProcess(
		TranscodeSegmentRequest{
			CallbackURL:       callbackServer.URL,
			SourceManifestURL: manifestFile.Name(),
			ReportProgress: func(stage clients.TranscodeStatus, completionRatio float64) {
				err := statusClient.SendTranscodeStatus(clients.NewTranscodeStatusProgress(callbackServer.URL, "", stage, completionRatio))
				require.NoError(t, err)
			},
			HlsTargetURL: dir,
		},
		"streamName",
		video.InputVideo{
			Duration:  123.0,
			Format:    "some-format",
			SizeBytes: 123,
			Tracks: []video.InputTrack{
				{
					Type:       "video",
					VideoTrack: sourceVideoTrack,
				},
			},
		},
		&localBroadcaster,
	)
	require.NoError(t, err)

	// Confirm the master manifest was created and that it looks like a manifest
	var expectedMasterManifest = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=532610,RESOLUTION=2020x2020,NAME="0-low-bitrate"
low-bitrate/index.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=0,BANDWIDTH=3195660,RESOLUTION=2020x2020,NAME="1-2020p0"
2020p0/index.m3u8
`

	masterManifestBytes, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))

	require.NoError(t, err)
	require.Greater(t, len(masterManifestBytes), 0)
	// One segment at end should be dropped to simulate an audio-only track in last segment
	// which should not be sent to B to transcode
	require.Equal(t, totalSegments-1, segmentsCount)
	require.Equal(t, expectedMasterManifest, string(masterManifestBytes))

	// Start the callback client, to let it run for one iteration
	statusClient.SendCallbacks()

	// Wait for the callbacks to arrive
	time.Sleep(100 * time.Millisecond)

	// Check we received periodic progress callbacks
	callbacksLock.Lock()
	defer callbacksLock.Unlock()
	require.Equal(t, 1, len(callbacks))
	require.Equal(t, 0.9, callbacks[0]["completion_ratio"])

	// Check we received a final Transcode Completed callback
	require.Equal(t, 1, len(outputs))
	require.Equal(t, path.Join(dir, "index.m3u8"), outputs[0].Manifest)
	require.Equal(t, 2, len(outputs[0].Videos))
}

func TestProcessTranscodeResult(t *testing.T) {
	dir := filepath.Join(testDataDir, "process-transcode-result")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	tests := []struct {
		name                       string
		segment                    segmentInfo
		transcodeRequest           TranscodeSegmentRequest
		sourceSegment              *bytes.Buffer
		transcodeResult            clients.TranscodeResult
		encodedProfiles            []video.EncodedProfile
		targetOSURL                *url.URL
		expectedTranscodedStats    []*video.RenditionStats
		expectedRenditionList      *video.TRenditionList
		expectedSegmentChannelMsgs []video.TranscodedSegmentInfo
		expectedError              string
	}{
		{
			name:             "Error when profile not found",
			segment:          segmentInfo{Index: 0, IsLastSegment: false},
			transcodeRequest: TranscodeSegmentRequest{IsClip: false, RequestID: "request-id"},
			sourceSegment:    bytes.NewBuffer([]byte("source data")),
			transcodeResult: clients.TranscodeResult{
				Renditions: []*clients.RenditionSegment{{Name: "profile2", MediaData: []byte("media data")}},
			},
			encodedProfiles:         []video.EncodedProfile{{Name: "profile1", Copy: false}},
			targetOSURL:             &url.URL{Scheme: "file", Path: dir},
			expectedTranscodedStats: []*video.RenditionStats{},

			expectedError: "failed to find rendition with name \"profile1\"",
		},
		{
			name:             "Successful with only copy profile",
			segment:          segmentInfo{Index: 0, IsLastSegment: false, Input: clients.SourceSegment{DurationMillis: 4000}},
			transcodeRequest: TranscodeSegmentRequest{IsClip: false, RequestID: "request-id"},
			sourceSegment:    bytes.NewBuffer([]byte("source data")),
			transcodeResult:  clients.TranscodeResult{},
			encodedProfiles:  []video.EncodedProfile{{Name: "profile1", Copy: true}},
			targetOSURL:      &url.URL{Scheme: "file", Path: dir},
			expectedTranscodedStats: []*video.RenditionStats{
				{
					Name:          "profile1",
					Bytes:         11, // len("source data")
					BitsPerSecond: 22,
					DurationMs:    4000,
				}},
			expectedRenditionList: &video.TRenditionList{RenditionSegmentTable: map[string]*video.TSegmentList{}},
		},
		{
			name:             "Successful with transcode profiles",
			segment:          segmentInfo{Index: 0, IsLastSegment: false, Input: clients.SourceSegment{DurationMillis: 4000}},
			transcodeRequest: TranscodeSegmentRequest{IsClip: false, RequestID: "request-id"},
			sourceSegment:    bytes.NewBuffer([]byte("source data")),
			transcodeResult: clients.TranscodeResult{
				Renditions: []*clients.RenditionSegment{
					{Name: "profile1", MediaData: []byte("media data")},
					{Name: "profile2", MediaData: []byte("mdat")},
				},
			},
			encodedProfiles: []video.EncodedProfile{
				{Name: "profile1", Width: 1280, Height: 720, Bitrate: 3_000_000},
				{Name: "profile2", Width: 640, Height: 420, Bitrate: 1_500_000},
			},
			targetOSURL: &url.URL{Scheme: "file", Path: dir},
			expectedTranscodedStats: []*video.RenditionStats{
				{
					Name:          "profile1",
					Width:         1280,
					Height:        720,
					Bytes:         10, // len("media data")
					BitsPerSecond: 20,
					DurationMs:    4000,
				},
				{
					Name:          "profile2",
					Width:         640,
					Height:        420,
					Bytes:         4, // len("mdat")
					BitsPerSecond: 8,
					DurationMs:    4000,
				},
			},
			expectedRenditionList: &video.TRenditionList{RenditionSegmentTable: map[string]*video.TSegmentList{}},
		},
		{
			name:             "Successful with copy and transcode profiles",
			segment:          segmentInfo{Index: 0, IsLastSegment: false, Input: clients.SourceSegment{DurationMillis: 4000}},
			transcodeRequest: TranscodeSegmentRequest{IsClip: false, RequestID: "request-id"},
			sourceSegment:    bytes.NewBuffer([]byte("source data")),
			transcodeResult: clients.TranscodeResult{
				Renditions: []*clients.RenditionSegment{
					{Name: "profile1", MediaData: []byte("media data")},
					{Name: "profile2", MediaData: []byte("mdat")},
				},
			},
			encodedProfiles: []video.EncodedProfile{
				{Name: "profile0", Width: 1920, Height: 1080, Copy: true},
				{Name: "profile1", Width: 1280, Height: 720, Bitrate: 3_000_000},
				{Name: "profile2", Width: 640, Height: 420, Bitrate: 1_500_000},
			},
			targetOSURL: &url.URL{Scheme: "file", Path: dir},
			expectedTranscodedStats: []*video.RenditionStats{
				{
					Name:          "profile0",
					Width:         1920,
					Height:        1080,
					Bytes:         11, // len("source data")
					BitsPerSecond: 22,
					DurationMs:    4000,
				},
				{
					Name:          "profile1",
					Width:         1280,
					Height:        720,
					Bytes:         10, // len("media data")
					BitsPerSecond: 20,
					DurationMs:    4000,
				},
				{
					Name:          "profile2",
					Width:         640,
					Height:        420,
					Bytes:         4, // len("mdat")
					BitsPerSecond: 8,
					DurationMs:    4000,
				},
			},
			expectedRenditionList: &video.TRenditionList{RenditionSegmentTable: map[string]*video.TSegmentList{}},
		},
		{
			name:             "Propagates segments for mp4 generation",
			segment:          segmentInfo{Index: 0, IsLastSegment: false, Input: clients.SourceSegment{DurationMillis: 4000}},
			transcodeRequest: TranscodeSegmentRequest{IsClip: false, RequestID: "request-id", GenerateMP4: true},
			sourceSegment:    bytes.NewBuffer([]byte("source data")),
			transcodeResult: clients.TranscodeResult{
				Renditions: []*clients.RenditionSegment{
					{Name: "profile1", MediaData: []byte("media data")},
				},
			},
			encodedProfiles: []video.EncodedProfile{
				{Name: "profile0", Width: 1920, Height: 1080, Copy: true},
				{Name: "profile1", Width: 1280, Height: 720, Bitrate: 3_000_000},
			},
			targetOSURL: &url.URL{Scheme: "file", Path: dir},
			expectedTranscodedStats: []*video.RenditionStats{
				{
					Name:          "profile0",
					Width:         1920,
					Height:        1080,
					Bytes:         11, // len("source data")
					BitsPerSecond: 22,
					DurationMs:    4000,
				},
				{
					Name:          "profile1",
					Width:         1280,
					Height:        720,
					Bytes:         10, // len("media data")
					BitsPerSecond: 20,
					DurationMs:    4000,
				},
			},
			expectedRenditionList: &video.TRenditionList{
				RenditionSegmentTable: map[string]*video.TSegmentList{
					"profile0": {
						SegmentDataTable: map[int][]byte{
							0: []byte("source data"),
						},
					},
					"profile1": {
						SegmentDataTable: map[int][]byte{
							0: []byte("media data"),
						},
					},
				},
			},
			expectedSegmentChannelMsgs: []video.TranscodedSegmentInfo{
				{
					RequestID:     "request-id",
					RenditionName: "profile0",
					SegmentIndex:  0,
				},
				{
					RequestID:     "request-id",
					RenditionName: "profile1",
					SegmentIndex:  0,
				},
			},
		},
	}

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)

			// patch the output dir not to interfere with each other
			dir := filepath.Join(dir, fmt.Sprintf("case-%d", idx))
			err := os.MkdirAll(dir, os.ModePerm)
			require.NoError(err)
			tt.targetOSURL.Path = dir

			transcodedStats := statsFromProfiles(tt.encodedProfiles)
			renditionList := &video.TRenditionList{RenditionSegmentTable: make(map[string]*video.TSegmentList)}
			if tt.transcodeRequest.GenerateMP4 {
				renditionList.AddRenditionSegment("profile0", &video.TSegmentList{SegmentDataTable: make(map[int][]byte)})
				renditionList.AddRenditionSegment("profile1", &video.TSegmentList{SegmentDataTable: make(map[int][]byte)})
			}
			segmentChannel := make(chan video.TranscodedSegmentInfo, 100)
			err = processTranscodeResult(
				tt.segment,
				tt.transcodeRequest,
				tt.sourceSegment,
				tt.transcodeResult,
				tt.encodedProfiles,
				tt.targetOSURL,
				transcodedStats,
				renditionList,
				segmentChannel,
			)

			if tt.expectedError != "" {
				require.ErrorContains(err, tt.expectedError)
				return
			}

			require.NoError(err)
			assert.EqualValues(tt.expectedTranscodedStats, transcodedStats)
			assert.EqualValues(tt.expectedRenditionList, renditionList)

			// check segment channel msgs
			for _, expectedMsg := range tt.expectedSegmentChannelMsgs {
				select {
				case actualMsg := <-segmentChannel:
					assert.EqualValues(expectedMsg, actualMsg)
				default:
					require.Fail("expected message not found in segment channel")
				}
			}
			select {
			case msg := <-segmentChannel:
				require.Fail(fmt.Sprintf("unexpected message in segment channel: %v", msg))
			default:
			}

			// check that segment files are written to disk
			for _, profile := range tt.encodedProfiles {
				fileName := filepath.Join(dir, profile.Name, "0.ts")
				_, err := os.Stat(fileName)
				assert.NoError(err, "expected segment file to exist: %s", fileName)
			}
		})
	}
}

func TestItCalculatesTheTranscodeCompletionPercentageCorrectly(t *testing.T) {
	require.Equal(t, 0.5, calculateCompletedRatio(2, 1))
	require.Equal(t, 0.5, calculateCompletedRatio(4, 2))
	require.Equal(t, 0.1, calculateCompletedRatio(10, 1))
	require.Equal(t, 0.01, calculateCompletedRatio(100, 1))
	require.Equal(t, 0.6, calculateCompletedRatio(100, 60))
}

func TestParallelJobFailureStopsNextBatch(t *testing.T) {
	config.TranscodingParallelJobs = 3
	config.TranscodingParallelSleep = 0
	sourceSegmentURLs := []clients.SourceSegment{
		// First 3 jobs run in parallel, second one fails
		{URL: segmentURL(t, "1.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "2.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "3.ts"), DurationMillis: 1000},
		// Rest of jobs should not be processed
		{URL: segmentURL(t, "4.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "5.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "6.ts"), DurationMillis: 1000},
	}
	halted := fmt.Errorf("halted")
	m := sync.Mutex{}
	var handlerIndex int = 0
	jobs := NewParallelTranscoding(sourceSegmentURLs, func(segment segmentInfo) error {
		time.Sleep(50 * time.Millisecond) // simulate processing
		m.Lock()
		defer m.Unlock()
		defer func() { handlerIndex += 1 }()
		if handlerIndex == 0 {
			return nil
		}
		if handlerIndex == 1 {
			return halted
		}
		return fmt.Errorf("failure detected late")
	})
	jobs.Start()
	err := jobs.Wait()
	// Check we got first error
	require.Error(t, err)
	require.Error(t, err, halted)
	// Check progress state is properly set
	require.Equal(t, 6, jobs.GetTotalCount())
	require.Equal(t, 1, jobs.GetCompletedCount())
	time.Sleep(10 * time.Millisecond) // wait for other workers to exit
}

func segmentURL(t *testing.T, u string) *url.URL {
	out, err := url.Parse(u)
	require.NoError(t, err)
	return out
}

func TestParallelJobSaveTime(t *testing.T) {
	config.TranscodingParallelJobs = 3
	config.TranscodingParallelSleep = 0
	sourceSegmentURLs := []clients.SourceSegment{
		// First 3 jobs should end at ~51ms mark
		{URL: segmentURL(t, "1.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "2.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "3.ts"), DurationMillis: 1000},
		// Second 3 jobs should end at ~101ms mark
		{URL: segmentURL(t, "4.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "5.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "6.ts"), DurationMillis: 1000},
	}
	start := time.Now()
	jobs := NewParallelTranscoding(sourceSegmentURLs, func(segment segmentInfo) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	jobs.Start()
	require.NoError(t, jobs.Wait())
	elapsed := time.Since(start)
	require.Greater(t, elapsed, 60*time.Millisecond)
	require.Less(t, elapsed, 160*time.Millisecond) // usually takes less than 101ms on idle machine
	time.Sleep(10 * time.Millisecond)              // wait for other workers to exit
}

func TestNewParallelTranscoding(t *testing.T) {
	sourceSegmentURLs := []clients.SourceSegment{
		{URL: segmentURL(t, "1.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "2.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "3.ts"), DurationMillis: 1000},
		{URL: segmentURL(t, "4.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "5.ts"), DurationMillis: 1000}, {URL: segmentURL(t, "6.ts"), DurationMillis: 1000},
	}

	// Define a test work function that doesn't do anything.
	testWork := func(segmentInfo) error {
		return nil
	}

	jobs := NewParallelTranscoding(sourceSegmentURLs, testWork)

	for i, u := range sourceSegmentURLs {
		expectedIsLastSegment := i == len(sourceSegmentURLs)-1
		segment := <-jobs.queue
		if segment.Input != u || segment.IsLastSegment != expectedIsLastSegment {
			t.Errorf("Test case failed for segment #%d, expected IsLastSegment=%v, got IsLastSegment=%v", i, expectedIsLastSegment, segment.IsLastSegment)
		}
	}

	// Check if there are any remaining segments in the queue (should be closed).
	if _, more := <-jobs.queue; more {
		t.Error("Expected the queue to be closed after processing all segments.")
	}
}

func TestHandleAVStartTimeOffsets(t *testing.T) {
	const manifestA = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.4160000000,
0.ts
#EXTINF:5.3340000000,
1.ts
#EXTINF:5.3340000000,
2.ts
#EXT-X-ENDLIST`

	tests := []struct {
		name               string
		inputInfo          video.InputVideo
		expectedFirstURI   string
		expectedSegmentURL string
		expectedLength     int
	}{
		{
			name: "FirstSegmentDroppedWithLargeStartTimeOffset",
			inputInfo: video.InputVideo{
				Duration:  123.0,
				Format:    "some-format",
				SizeBytes: 123,
				Tracks: []video.InputTrack{
					{
						Type:         "video",
						StartTimeSec: 5.0,
						VideoTrack:   video.VideoTrack{Width: 2020, Height: 2020},
					},
					{
						Type:         "audio",
						StartTimeSec: 1.4,
						AudioTrack:   video.AudioTrack{Channels: 2},
					},
				},
			},
			expectedFirstURI:   "1.ts",
			expectedSegmentURL: "1.ts",
			expectedLength:     2,
		},
		{
			name: "FirstSegmentIsNotDroppedWithSmallStartTimeOffset",
			inputInfo: video.InputVideo{
				Duration:  123.0,
				Format:    "some-format",
				SizeBytes: 123,
				Tracks: []video.InputTrack{
					{
						Type:         "video",
						StartTimeSec: 1.0,
						VideoTrack:   video.VideoTrack{Width: 2020, Height: 2020},
					},
					{
						Type:         "audio",
						StartTimeSec: 1.4,
						AudioTrack:   video.AudioTrack{Channels: 2},
					},
				},
			},
			expectedFirstURI:   "0.ts",
			expectedSegmentURL: "0.ts",
			expectedLength:     3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			playlistA, _, err := m3u8.DecodeFrom(strings.NewReader(manifestA), true)
			require.NoError(t, err)
			sourceManifest := playlistA.(*m3u8.MediaPlaylist)

			sourceSegmentURLs := []clients.SourceSegment{
				{URL: segmentURL(t, "0.ts"), DurationMillis: 1000},
				{URL: segmentURL(t, "1.ts"), DurationMillis: 1000},
				{URL: segmentURL(t, "2.ts"), DurationMillis: 1000},
			}

			sourceManifest.Segments, sourceSegmentURLs = HandleAVStartTimeOffsets("test", tt.inputInfo, sourceManifest.Segments, sourceSegmentURLs)
			require.Equal(t, tt.expectedFirstURI, sourceManifest.Segments[0].URI)
			require.Equal(t, segmentURL(t, tt.expectedSegmentURL), sourceSegmentURLs[0].URL)
			require.Equal(t, tt.expectedLength, len(sourceSegmentURLs))
		})
	}
}

func TestHandleAVStartTimeOffsetsWhenSingleSegment(t *testing.T) {
	const manifestA = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-TARGETDURATION:5
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.4160000000,
0.ts
#EXT-X-ENDLIST`

	tests := []struct {
		name           string
		inputInfo      video.InputVideo
		expectedLength int
	}{
		{
			name: "FirstSegmentDroppedWithLargeStartTimeOffset",
			inputInfo: video.InputVideo{
				Duration:  123.0,
				Format:    "some-format",
				SizeBytes: 123,
				Tracks: []video.InputTrack{
					{
						Type:         "video",
						StartTimeSec: 5.0,
						VideoTrack:   video.VideoTrack{Width: 2020, Height: 2020},
					},
					{
						Type:         "audio",
						StartTimeSec: 1.4,
						AudioTrack:   video.AudioTrack{Channels: 2},
					},
				},
			},
			expectedLength: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			playlistA, _, err := m3u8.DecodeFrom(strings.NewReader(manifestA), true)
			require.NoError(t, err)
			sourceManifest := playlistA.(*m3u8.MediaPlaylist)

			sourceSegmentURLs := []clients.SourceSegment{
				{URL: segmentURL(t, "0.ts"), DurationMillis: 1000},
			}

			sourceManifest.Segments, sourceSegmentURLs = HandleAVStartTimeOffsets("test", tt.inputInfo, sourceManifest.Segments, sourceSegmentURLs)
			require.Equal(t, tt.expectedLength, len(sourceSegmentURLs))
		})
	}
}

func TestWithPipedSource(t *testing.T) {
	dummyProfiles := []video.EncodedProfile{{Name: "dummy"}}

	t.Run("returns original reader when copySource is false", func(t *testing.T) {
		require := require.New(t)
		in := strings.NewReader("hello")

		reader, buffer, err := withPipedSource(in, false, nil)
		require.NoError(err)
		require.Nil(buffer)
		require.Equal(in, reader)

		reader, buffer, err = withPipedSource(in, false, dummyProfiles)
		require.NoError(err)
		require.Nil(buffer)
		require.Equal(in, reader)
	})
	t.Run("returns nil reader and filled buffer when copySource is true and transcodeProfiles is empty", func(t *testing.T) {
		require := require.New(t)
		in := strings.NewReader("hello")

		reader, buffer, err := withPipedSource(in, true, nil)
		require.NoError(err)
		require.Equal(nil, reader)
		require.Equal("hello", buffer.String())

		// check input was consumed
		require.Equal(0, in.Len())
	})
	t.Run("returns tee'd reader and buffer when copySource is true and transcodeProfiles is not empty", func(t *testing.T) {
		require := require.New(t)
		in := strings.NewReader("hello")
		originalLen := in.Len()

		reader, buffer, err := withPipedSource(in, true, dummyProfiles)
		require.NoError(err)
		require.NotNil(reader)
		require.NotNil(buffer)

		// check input was not consumed
		require.Equal(originalLen, in.Len())
		require.Equal(0, buffer.Len())
		// check that bytes read from reader get copied into buffer
		buf := make([]byte, 2)
		n, err := io.ReadFull(reader, buf)
		require.NoError(err)
		require.Equal(2, n)
		require.Equal("he", string(buf))
		require.Equal("he", buffer.String())
		require.Equal(originalLen-2, in.Len())
	})
}
