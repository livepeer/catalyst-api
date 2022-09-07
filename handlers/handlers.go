package handlers

import (
	"fmt"
	"math/rand"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"
)

var CallbackClient = clients.NewCallbackClient()

type StreamInfo struct {
	callbackUrl string
}

type CatalystAPIHandlersCollection struct {
	MistClient  clients.MistAPIClient
	StreamCache map[string]StreamInfo
}

func HasContentType(r *http.Request, mimetype string) bool {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		return mimetype == "application/octet-stream"
	}

	for _, v := range strings.Split(contentType, ",") {
		t, _, err := mime.ParseMediaType(v)
		if err != nil {
			break
		}
		if t == mimetype {
			return true
		}
	}

	return false
}

func randomStreamName(prefix string) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	const length = 8
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = charset[r.Intn(length)]
	}
	return fmt.Sprintf("%s%s", prefix, string(res))
}

type MistCallbackHandlersCollection struct {
	MistClient  clients.MistAPIClient
	StreamCache map[string]StreamInfo
}

// This method is for Studio to have something to integrate with and to make sure we have all the callbacks
// in place that we'll need
func stubTranscodingCallbacksForStudio(callbackURL string, callbackClient clients.CallbackClient) {
	time.Sleep(5 * time.Second)
	callbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 0.3)
	time.Sleep(5 * time.Second)
	callbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 0.6)
	time.Sleep(5 * time.Second)
	callbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 0.9)
	time.Sleep(5 * time.Second)
	callbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 1)
	time.Sleep(5 * time.Second)
	callbackClient.SendTranscodeStatusCompleted(
		callbackURL,
		clients.InputVideo{
			Format:   "mp4",
			Duration: 1234.5678,
			Tracks: []clients.InputTrack{
				{
					Type:        "video",
					Codec:       "h264",
					DurationSec: 1.6,
					Bitrate:     358315,
					VideoTrack: clients.VideoTrack{
						FPS:         30,
						Width:       1920,
						Height:      1080,
						PixelFormat: "yuv420p",
					},
				},
				{
					Type:        "audio",
					Codec:       "aac",
					Bitrate:     141341,
					DurationSec: 1.599979,
					AudioTrack: clients.AudioTrack{
						Channels:   2,
						SampleRate: 48000,
					},
				},
			},
		},
		[]clients.OutputVideo{
			{
				Type:     "google-s3",
				Manifest: "s3://livepeer-studio-uploads/videos/<video-id>/master.m3u8",
				Videos: []clients.OutputVideoFile{
					{
						Type:      "mp4",
						SizeBytes: 12345,
						Location:  "s3://livepeer-studio-uploads/videos/<video-id>/video-480p.mp4",
					},
				},
			},
		},
	)
}
