package misttriggers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
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

	// Check if LIVE_TRACK_LIST trigger is being fired *after* the push-from-Mist-to-S3 is complete
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
fmt.Printf("XXX: TRACKS: %v\n", tracks)

	multivariantPlaylist := "#EXTM3U\r\n"

	// Upload each track (transcoded rendition) returned by Mist to S3
	for i := range tracks {
		// Only produce a rendition for each video track, selecting best audio track
		if tracks[i].Type != "video" {
			continue
		}

		// Build the full URL path that will be sent to Mist as the target upload location
		dirPathUrl, err := url.JoinPath(info.UploadDir, fmt.Sprintf("%s_%dx%d/stream.m3u8",
			streamName, tracks[i].Width, tracks[i].Height))
		if err != nil {
			log.Fatal(err)
		}
		fullPathUrl, err := url.Parse(dirPathUrl)
		if err != nil {
			log.Fatal(err)
		}

		// Add URL query parameters (e.g. ?video=0&audio=maxbps) used by Mist to select
		// the correct trancoded rendtion track(s)
		urlParams := fullPathUrl.Query()
		urlParams.Add("video", strconv.FormatInt(int64(tracks[i].Index), 10))
		urlParams.Add("audio", "maxbps")
		fullPathUrl.RawQuery = urlParams.Encode()

		destination := fullPathUrl.String()

                if err := d.MistClient.PushStart(streamName, destination); err != nil {
                        log.Printf("> ERROR push to %s %v", destination, err)
                } else {
fmt.Println("XXX: STARTING PUSH AFTER LIVE_TRACK_LIST")
                        cache.DefaultStreamCache.Transcoding.AddDestination(streamName, destination)


			profile, ok := info.GetMatchingProfile(tracks[i].Width, tracks[i].Height)
			if !ok {
				log.Printf("ERROR push doesn't match to any given profile %s", destination)
			} else {
				multivariantPlaylist += fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\r\n%s\r\n", profile.Bitrate, tracks[i].Width, tracks[i].Height, destination)
				log.Printf("YYY: multivariantPlaylist %s", multivariantPlaylist)

			}

                }
	}
}
