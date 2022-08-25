package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/xeipuuv/gojsonschema"
)

type CatalystAPIHandlersCollection struct {
	MistClient  MistAPIClient
	StreamCache *StreamCache
}

func (d *CatalystAPIHandlersCollection) Ok() httprouter.Handle {
	return func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		io.WriteString(w, "OK")
	}
}

// TranscodeSegment takes mpegts segment as input, ingests in Mist under SOURCE_PREFIX streamName, transcodes into RENDITION_PREFIX stream then pushes renditions to specified destination.
// Manually starts MistLivepeerProc binary. For this we require port of B-node that listens on 127.0.0.1 .
// When `MistProcLivepeer` starts, SOURCE_PREFIX stream is taken as input and (will automatically - WIP) start SOURCE_PREFIX stream.
// No trigger is registered on SOURCE_PREFIX so configured sourceUrl is used.
// Also `MistProcLivepeer` outputs to RENDITION_PREFIX stream where we listen for triggers to start new push to rendition destination.
// Because RENDITION_PREFIX stream contains multiple video tracks we start multiple push-es each selecting one video and one audio track to push to S3.
func (d *CatalystAPIHandlersCollection) TranscodeSegment(broadcasterPort int, mistProcPath string) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		t := Transcoding{httpResp: w, httpReq: req, broadcasterPort: broadcasterPort, mistProcPath: mistProcPath}
		if err := t.ValidateRequest(); err != nil {
			log.Printf("TranscodeSegment request validation failed %v", err)
			return
		}
		if err := t.PrepareStreams(d.MistClient); err != nil {
			log.Printf("TranscodeSegment input stream create failed %v", err)
			return
		}
		// run in background
		go t.RunTranscodeProcess(d.MistClient, d.StreamCache)

		io.WriteString(w, "Transcode done; Upload in progress")
	}
}

func (d *CatalystAPIHandlersCollection) UploadVOD() httprouter.Handle {
	schema := inputSchemasCompiled["UploadVOD"]

	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		var uploadVODRequest UploadVODRequest

		if !HasContentType(req, "application/json") {
			errors.WriteHTTPUnsupportedMediaType(w, "Requires application/json content type", nil)
			return
		} else if payload, err := io.ReadAll(req.Body); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		} else if result, err := schema.Validate(gojsonschema.NewBytesLoader(payload)); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot validate payload", err)
			return
		} else if !result.Valid() {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("%s", result.Errors()))
			return
		} else if err := json.Unmarshal(payload, &uploadVODRequest); err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}

		// find source segment URL
		var tURL string
		for _, o := range uploadVODRequest.OutputLocations {
			if o.Outputs.SourceSegments {
				tURL = o.URL
				break
			}
		}
		if tURL == "" {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("no source segment URL in request"))
			return
		}

		streamName := randomStreamName("catalyst_vod_")
		d.StreamCache.Segmenting.Store(streamName, uploadVODRequest.CallbackUrl)

		// process the request
		if err := d.processUploadVOD(streamName, uploadVODRequest.Url, tURL); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot process upload VOD request", err)
		}

		callbackClient := clients.NewCallbackClient()
		if err := callbackClient.SendTranscodeStatus(uploadVODRequest.CallbackUrl, clients.TranscodeStatusPreparing, 0.0); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot send transcode status", err)
		}

		io.WriteString(w, fmt.Sprint(len(uploadVODRequest.OutputLocations)))
	}
}

func (d *CatalystAPIHandlersCollection) processUploadVOD(streamName, sourceURL, targetURL string) error {
	if err := d.MistClient.AddStream(streamName, sourceURL); err != nil {
		return err
	}
	if err := d.MistClient.AddTrigger(streamName, "PUSH_END"); err != nil {
		return err
	}
	if err := d.MistClient.PushStart(streamName, targetURL); err != nil {
		return err
	}

	return nil
}

func randomTrailer() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	const length = 8
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = charset[r.Intn(length)]
	}
	return string(res)
}
func randomStreamName(prefix string) string {
	return fmt.Sprintf("%s%s", prefix, randomTrailer())
}

type MistCallbackHandlersCollection struct {
	MistClient  MistAPIClient
	StreamCache *StreamCache
}

// This trigger is stream-specific and must be blocking.
// The payload for this trigger is multiple lines, each separated by a single newline character
// (without an ending newline), containing data as such: `stream name`\n`push target URI`
func (d *MistCallbackHandlersCollection) Trigger_LIVE_TRACK_LIST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Following code is responding to transcoding handler:
	streamName := "unknown"
	errorOutInternal := func(where string, err error) {
		txt := fmt.Sprintf("%s name=%s", where, streamName)
		// Mist does not respond or handle returned error codes. We do this for correctness.
		errors.WriteHTTPInternalServerError(w, txt, err)
		log.Printf("ERROR LIVE_TRACK_LIST %s %v", txt, err)
	}
	payload, err := io.ReadAll(req.Body)
	if err != nil {
		errorOutInternal("Cannot read payload", err)
		return
	}
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	streamName = lines[0]
	encodedTracks := lines[1]

	yes, suffix := isTranscodeStream(streamName)
	if !yes {
		errorOutInternal("unknown streamName format", nil)
		return
	}

	if streamEnded := encodedTracks == "null"; streamEnded {
		// SOURCE_PREFIX stream is no longer needed
		inputStream := fmt.Sprintf("%s%s", SOURCE_PREFIX, suffix)
		if err = d.MistClient.DeleteStream(inputStream); err != nil {
			log.Printf("ERROR LIVE_TRACK_LIST DeleteStream(%s) %v", inputStream, err)
		}
		// Multiple pushes from RENDITION_PREFIX are in progress.
		return
	}
	tracks := make(LiveTrackListTriggerJson)
	if err = json.Unmarshal([]byte(encodedTracks), &tracks); err != nil {
		errorOutInternal("LiveTrackListTriggerJson json decode error", err)
		return
	}
	// Start push per each video track
	info, err := d.StreamCache.Transcoding.Get(streamName)
	if err != nil {
		errorOutInternal("LIVE_TRACK_LIST unknown push source", err)
		return
	}
	for i := range tracks { // i is generated name, not important, all info is contained in element
		if tracks[i].Type != "video" {
			// Only produce an rendition per each video track, selecting best audio track
			continue
		}
		destination := fmt.Sprintf("%s/%s__%dx%d.ts?video=%d&audio=maxbps", info.UploadDir, streamName, tracks[i].Width, tracks[i].Height, tracks[i].Index) //.Id)
		if err := d.MistClient.PushStart(streamName, destination); err != nil {
			log.Printf("> ERROR push to %s %v", destination, err)
		} else {
			d.StreamCache.Transcoding.AddDestination(streamName, destination)
		}
	}
}

// This trigger is run whenever an outgoing push stops, for any reason.
// This trigger is stream-specific and non-blocking. containing data as such:
//   push ID (integer)
//   stream name (string)
//   target URI, before variables/triggers affected it (string)
//   target URI, afterwards, as actually used (string)
//   last 10 log messages (JSON array string)
//   most recent push status (JSON object string)
func (d *MistCallbackHandlersCollection) Trigger_PUSH_END(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	streamName := "unknown"
	errorOutInternal := func(where string, err error) {
		txt := fmt.Sprintf("%s name=%s", where, streamName)
		// Mist does not respond or handle returned error codes. We do this for correctness.
		errors.WriteHTTPInternalServerError(w, txt, err)
		log.Printf("<ERROR LIVE_TRACK_LIST> %s %v", txt, err)
	}

	payload, err := io.ReadAll(req.Body)
	if err != nil {
		errorOutInternal("Cannot read payload", err)
		return
	}
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	if len(lines) != 6 {
		errors.WriteHTTPBadRequest(w, "Bad request payload", fmt.Errorf("unknown payload '%s'", string(payload)))
		return
	}
	// stream name is the second line in the Mist Trigger payload
	streamName = lines[1]
	destination := lines[2]
	actualDestination := lines[3]
	pushStatus := lines[5]
	if yes, _ := isTranscodeStream(streamName); yes {
		// Following code is responding to transcoding handler: TODO: encapsulate
		info, err := d.StreamCache.Transcoding.Get(streamName)
		if err != nil {
			errorOutInternal("unknown push source", err)
			return
		}
		inOurBooks := false
		for i := 0; i < len(info.Destionations); i++ {
			if info.Destionations[i] == destination {
				inOurBooks = true
				break
			}
		}
		if !inOurBooks {
			errorOutInternal("missing from our books", nil)
			return
		}
		callbackClient := clients.NewCallbackClient()
		if uploadSuccess := pushStatus == "null"; uploadSuccess {
			if err := callbackClient.SendRenditionUpload(info.CallbackUrl, info.Source, actualDestination); err != nil {
				errorOutInternal("Cannot send rendition transcode status", err)
			}
		} else {
			// We forward pushStatus json to callback
			if err := callbackClient.SendRenditionUploadError(info.CallbackUrl, info.Source, actualDestination, pushStatus); err != nil {
				errorOutInternal("Cannot send rendition transcode error", err)
			}
		}
		// We do not delete triggers as source stream is wildcard stream: RENDITION_PREFIX
		if empty := d.StreamCache.Transcoding.RemovePushDestination(streamName, destination); empty {
			if err := callbackClient.SendSegmentTranscodeStatus(info.CallbackUrl, info.Source); err != nil {
				errorOutInternal("Cannot send segment transcode status", err)
			}
		}
		return
	}

	// Following code is responding to segmenting handler: TODO: encapsulate
	// when uploading is done, remove trigger and stream from Mist
	errT := d.MistClient.DeleteTrigger(streamName, "PUSH_END")
	errS := d.MistClient.DeleteStream(streamName)
	if errT != nil {
		errorOutInternal(fmt.Sprintf("Cannot remove PUSH_END trigger for stream '%s'", streamName), errT)
		return
	}
	if errS != nil {
		errorOutInternal(fmt.Sprintf("Cannot remove stream '%s'", streamName), errS)
		return
	}

	callbackClient := clients.NewCallbackClient()
	callbackUrl, err := d.StreamCache.Segmenting.GetCallbackUrl(streamName)
	if err != nil {
		errorOutInternal("PUSH_END trigger invoked for unknown stream", err)
	}
	if err := callbackClient.SendTranscodeStatus(callbackUrl, clients.TranscodeStatusTranscoding, 0.0); err != nil {
		errorOutInternal("Cannot send transcode status", err)
	}
	d.StreamCache.Segmenting.Remove(streamName)

	// TODO: add timeout for the stream upload
	// TODO: start transcoding
}

// Only single trigger callback is allowed on Mist.
// All created streams and our handlers (segmenting, transcoding, et.) must share this endpoint.
// If handler logic grows more complicated we may consider adding dispatch mechanism here.
func (d *MistCallbackHandlersCollection) Trigger() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		whichOne := req.Header.Get("X-Trigger")
		switch whichOne {
		case "PUSH_END":
			d.Trigger_PUSH_END(w, req, params)
		case "LIVE_TRACK_LIST":
			d.Trigger_LIVE_TRACK_LIST(w, req, params)
		default:
			errors.WriteHTTPBadRequest(w, "Unsupported X-Trigger", fmt.Errorf("unknown trigger '%s'", whichOne))
			return
		}
	}
}
