package misttriggers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/go-tools/drivers"
)

type MistTrack struct {
	// populated by mist when trigger is received
	Id          int32  `json:"trackid"`
	ByteRate    int32  `json:"bps"`
	Fpks        int32  `json:"fpks"`
	Height      int32  `json:"height"`
	Width       int32  `json:"width"`
	Index       int32  `json:"idx"`
	Type        string `json:"type"`
	Codec       string `json:"codec"`
	StartTimeMs int32  `json:"firstms"`
	EndTimeMs   int32  `json:"lastms"`
	// populated by us when processing trigger
	manifestDestPath string
}

type LiveTrackListTriggerJson = map[string]MistTrack

// create ByBitrate type which is a MistTrack slice
type ByBitrate []MistTrack

func (a ByBitrate) Len() int {
	return len(a)
}

func (a ByBitrate) Less(i, j int) bool {
	if a[i].ByteRate == a[j].ByteRate {
		// if two tracks have the same byterate, then sort by resolution
		return a[i].Width*a[i].Height < a[j].Width*a[j].Height
	} else {
		return a[i].ByteRate < a[j].ByteRate
	}
}

func (a ByBitrate) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func createPlaylist(multivariantPlaylist string, tracks []MistTrack) string {
	for _, track := range tracks {
		multivariantPlaylist += fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\r\n%s\r\n", track.ByteRate*8, track.Width, track.Height, track.manifestDestPath)
	}
	return multivariantPlaylist
}

func uploadPlaylist(uploadPath, manifest string) error {
	storageDriver, err := drivers.ParseOSURL(uploadPath, true)
	if err != nil {
		return fmt.Errorf("error parsing multivariant playlist's upload directory: %s, error: %s", uploadPath, err)
	}
	session := storageDriver.NewSession("")
	ctx := context.Background()
	_, err = session.SaveData(ctx, "", bytes.NewBuffer([]byte(manifest)), nil, 3*time.Second)
	if err != nil {
		return fmt.Errorf("failed to upload multivariant playlist to: %s, error: %s", uploadPath, err)
	}
	return nil
}

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
	uniqueName := streamName[len(config.RENDITION_PREFIX):]

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

	multivariantPlaylist := "#EXTM3U\r\n"

	trackList := []MistTrack{}

	// Build the full URL path that will be sent to Mist as the target upload location
	rootPathUrl, err := url.Parse(info.UploadDir)
	if err != nil {
		errors.WriteHTTPInternalServerError(w, "Failed to parse root path URL: "+streamName, err)
		return
	}

	// upload each track (transcoded rendition) returned by Mist to S3
	for i := range tracks {
		// Only produce a rendition for each video track, selecting best audio track
		if tracks[i].Type != "video" {
			continue
		}

		dirPath := fmt.Sprintf("_%s_%dx%d/stream.m3u8", uniqueName, tracks[i].Width, tracks[i].Height)
		dirPathUrl, err := url.JoinPath(info.UploadDir, dirPath)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Failed to generate the upload directory path: "+streamName, err)
			return
		}

		fullPathUrl, err := url.Parse(dirPathUrl)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Failed to parse the upload directory url: "+streamName, err)
			return
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
			cache.DefaultStreamCache.Transcoding.AddDestination(streamName, destination)
			trackList = append(trackList, tracks[i])
			trackList[len(trackList)-1].manifestDestPath = dirPath
		}
	}

	// Generate a sorted list for multivariant playlist (reverse order of bitrate then resolution):
	sort.Sort(sort.Reverse(ByBitrate(trackList)))
	manifest := createPlaylist(multivariantPlaylist, trackList)
	path := fmt.Sprintf("%s/%s-master.m3u8", rootPathUrl.String(), uniqueName)
	err = uploadPlaylist(path, manifest)
	if err != nil {
		errors.WriteHTTPInternalServerError(w, "Failed to upload multivariant master playlist: "+streamName, err)
		return
	}

	// Store the path back into our cached object to allow us to populate the final "Transcode Success" metadata callback
	info.Outputs = append(info.Outputs, clients.OutputVideo{
		Manifest: path,
	})
	cache.DefaultStreamCache.Transcoding.Store(streamName, *info)
}
