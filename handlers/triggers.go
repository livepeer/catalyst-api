package handlers

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
)

func (d *MistCallbackHandlersCollection) Trigger() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		if t := req.Header.Get("X-Trigger"); t != "PUSH_END" {
			errors.WriteHTTPBadRequest(w, "Unsupported X-Trigger", fmt.Errorf("unknown trigger '%s'", t))
			return
		}
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		}
		lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
		if len(lines) < 2 {
			errors.WriteHTTPBadRequest(w, "Bad request payload", fmt.Errorf("unknown payload '%s'", string(payload)))
			return
		}

		// stream name is the second line in the Mist Trigger payload
		s := lines[1]
		// when uploading is done, remove trigger and stream from Mist
		errT := d.MistClient.DeleteTrigger(s, "PUSH_END")
		errS := d.MistClient.DeleteStream(s)
		if errT != nil {
			errors.WriteHTTPInternalServerError(w, fmt.Sprintf("Cannot remove PUSH_END trigger for stream '%s'", s), errT)
			return
		}
		if errS != nil {
			errors.WriteHTTPInternalServerError(w, fmt.Sprintf("Cannot remove stream '%s'", s), errS)
			return
		}

		callbackClient := clients.NewCallbackClient()
		if err := callbackClient.SendTranscodeStatus(d.StreamCache[s].callbackUrl, clients.TranscodeStatusTranscoding, 0.0); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot send transcode status", err)
		}

		delete(d.StreamCache, s)

		// TODO: add timeout for the stream upload
		// TODO: start transcoding
		stubTranscodingCallbacksForStudio(d.StreamCache[s].callbackUrl, callbackClient)
	}
}

// This method is for Studio to have something to integrate with and to make sure we have all the callbacks
// in place that we'll need
func stubTranscodingCallbacksForStudio(callbackURL string, callbackClient clients.CallbackClient) {
	time.Sleep(5 * time.Second)
	if err := callbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 0.3); err != nil {
		_ = config.Logger.Log("msg", "Error in stubTranscodingCallbacksForStudio", "err", err)
		return
	}

	time.Sleep(5 * time.Second)
	if err := callbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 0.6); err != nil {
		_ = config.Logger.Log("msg", "Error in stubTranscodingCallbacksForStudio", "err", err)
		return
	}

	time.Sleep(5 * time.Second)
	if err := callbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 0.9); err != nil {
		_ = config.Logger.Log("msg", "Error in stubTranscodingCallbacksForStudio", "err", err)
		return
	}

	time.Sleep(5 * time.Second)
	if err := callbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 1); err != nil {
		_ = config.Logger.Log("msg", "Error in stubTranscodingCallbacksForStudio", "err", err)
		return
	}

	time.Sleep(5 * time.Second)
	err := callbackClient.SendTranscodeStatusCompleted(
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
	if err != nil {
		_ = config.Logger.Log("msg", "Error sending Transcode Completed in stubTranscodingCallbacksForStudio", "err", err)
		return
	}
}
