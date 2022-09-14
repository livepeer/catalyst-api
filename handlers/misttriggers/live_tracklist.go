package misttriggers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
)

type MistTrack struct {
	Id          int32  `json:"trackid"`
	Kfps        int32  `json:"fpks"`
	Height      int32  `json:"height"`
	Width       int32  `json:"width"`
	Index       int32  `json:"idx"`
	Type        string `json:"type"`
	Codec       string `json:"codec"`
	StartTimeMs int32  `json:"firstms"`
	EndTimeMs   int32  `json:"lastms"`
}

type LiveTrackListTriggerJson = map[string]MistTrack

// TriggerLiveTrackList responds to LIVE_TRACK_LIST trigger.
// It is stream-specific and must be blocking. The payload for this trigger is multiple lines,
// each separated by a single newline character (without an ending newline), containing data:
//
//	stream name
//	track list (JSON)
//
// TriggerLiveTrackList is used only by transcoding.
func (d *MistCallbackHandlersCollection) TriggerLiveTrackList(w http.ResponseWriter, req *http.Request, payload []byte) {
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	streamName := lines[0]
	encodedTracks := lines[1]

	fmt.Println("XXX: LIVE HERE....................")
	fmt.Printf("XXX: %s\n", lines)
	fmt.Printf("XXX: %s\n", string(payload))

	// Check that the name looks right for a stream we've completed as part of the Transcode workflow
	if !config.IsTranscodeStream(streamName) {
		errors.WriteHTTPBadRequest(w, "PUSH_END trigger invoked for something that isn't a transcode stream: "+streamName, nil)
		return
	}

	// Fetch the stream info from cache (cached when we kicked off the transcode process)
	info := cache.DefaultStreamCache.Transcoding.Get(streamName)
	if info == nil {
		errors.WriteHTTPInternalServerError(w, "LIVE_TRACK_LIST unknown push source: "+streamName, nil)
		return
	}

	var streamEnded = (encodedTracks == "null")
	if streamEnded {
		// SOURCE_PREFIX stream is no longer needed
		suffix := strings.TrimPrefix(streamName, config.RENDITION_PREFIX)
		inputStream := fmt.Sprintf("%s%s", config.SOURCE_PREFIX, suffix)
		if err := d.MistClient.DeleteStream(inputStream); err != nil {
			log.Printf("ERROR LIVE_TRACK_LIST DeleteStream(%s) %v", inputStream, err)
		}
		// Multiple pushes from RENDITION_PREFIX are in progress.
		return
	}

	var tracks LiveTrackListTriggerJson
	if err := json.Unmarshal([]byte(encodedTracks), &tracks); err != nil {
		errors.WriteHTTPInternalServerError(w, "LiveTrackListTriggerJson json decode error: "+streamName, err)
		return
	}

	// Start push per each video track
	for i := range tracks {
		if tracks[i].Type != "video" {
			// Only produce an rendition per each video track, selecting best audio track
			continue
		}
		destination := fmt.Sprintf("%s/%s__%dx%d.ts?video=%d&audio=maxbps", info.UploadDir, streamName, tracks[i].Width, tracks[i].Height, tracks[i].Index) //.Id)
fmt.Printf("XXX: destination %s\n", destination)
		if err := d.MistClient.PushStart(streamName, destination); err != nil {
			log.Printf("> ERROR push to %s %v", destination, err)
		} else {
fmt.Println("XXX: PUSHING")
			cache.DefaultStreamCache.Transcoding.AddDestination(streamName, destination)
		}
	}
}
