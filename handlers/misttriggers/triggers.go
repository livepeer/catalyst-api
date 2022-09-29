package misttriggers

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

const TRIGGER_PUSH_END = "PUSH_END"
const TRIGGER_LIVE_TRACK_LIST = "LIVE_TRACK_LIST"

type MistCallbackHandlersCollection struct {
	MistClient clients.MistAPIClient
}

// Trigger dispatches request to mapped method according to trigger name
// Only single trigger callback is allowed on Mist.
// All created streams and our handlers (segmenting, transcoding, et.) must share this endpoint.
// If handler logic grows more complicated we may consider adding dispatch mechanism here.
func (d *MistCallbackHandlersCollection) Trigger() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Cannot read trigger payload", err)
			return
		}

		triggerName := req.Header.Get("X-Trigger")
		switch triggerName {
		case TRIGGER_PUSH_END:
			d.TriggerPushEnd(w, req, payload)
		case TRIGGER_LIVE_TRACK_LIST:
			d.TriggerLiveTrackList(w, req, payload)
		default:
			errors.WriteHTTPBadRequest(w, "Unsupported X-Trigger", fmt.Errorf("unknown trigger '%s'", triggerName))
			return
		}
	}
}

// This method is for Studio to have something to integrate with and to make sure we have all the callbacks + domain models
// in place that we'll need
func stubTranscodingCallbacksForStudio(callbackURL string) {
	time.Sleep(5 * time.Second)
	if err := clients.DefaultCallbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 0.3); err != nil {
		_ = config.Logger.Log("msg", "Error in stubTranscodingCallbacksForStudio", "err", err)
		return
	}

	time.Sleep(5 * time.Second)
	if err := clients.DefaultCallbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 0.6); err != nil {
		_ = config.Logger.Log("msg", "Error in stubTranscodingCallbacksForStudio", "err", err)
		return
	}

	time.Sleep(5 * time.Second)
	if err := clients.DefaultCallbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 0.9); err != nil {
		_ = config.Logger.Log("msg", "Error in stubTranscodingCallbacksForStudio", "err", err)
		return
	}

	time.Sleep(5 * time.Second)
	if err := clients.DefaultCallbackClient.SendTranscodeStatus(callbackURL, clients.TranscodeStatusTranscoding, 1); err != nil {
		_ = config.Logger.Log("msg", "Error in stubTranscodingCallbacksForStudio", "err", err)
		return
	}

	time.Sleep(5 * time.Second)
	err := clients.DefaultCallbackClient.SendTranscodeStatusCompleted(
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

// streamNameToPipeline returns pipeline that given stream belongs to. We use different stream name prefixes for each pipeline.
func streamNameToPipeline(name string) PipelineId {
	if strings.HasPrefix(name, config.RENDITION_PREFIX) {
		// config.SOURCE_PREFIX also belongs to Transcoding. So far no triggers installed for source streams.
		return Transcoding
	} else if strings.HasPrefix(name, config.SEGMENTING_PREFIX) {
		return Segmenting
	} else if strings.HasPrefix(name, config.RECORDING_PREFIX) {
		return Recording
	}
	return Unrelated
}

type PipelineId = int

const (
	Unrelated PipelineId = iota
	Segmenting
	Transcoding
	Recording
)
