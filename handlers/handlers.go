package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/livepeer/catalyst-api/clients"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/xeipuuv/gojsonschema"
)

type StreamInfo struct {
	callbackUrl string
}

type CatalystAPIHandlersCollection struct {
	MistClient  MistAPIClient
	StreamCache map[string]StreamInfo
}

func (d *CatalystAPIHandlersCollection) Ok() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		io.WriteString(w, "OK")
	}
}

// Takes mpegts segment as input, ingests in Mist under SOURCE_PREFIX streamName, transcodes into RENDITION_PREFIX stream then pushes renditions to specified destination.
// Manually starts MistLivepeerProc binary. For this we require port of B-node that listens on 127.0.0.1 .
// When `MistProcLivepeer` starts, SOURCE_PREFIX stream is taken as input and will automatically start SOURCE_PREFIX stream.
// No trigger is registered on SOURCE_PREFIX so configured sourceUrl is used.
// Also `MistProcLivepeer` outputs to RENDITION_PREFIX stream where we listen for triggers to start new push to rendition destination.
// Because RENDITION_PREFIX stream contains multiple video tracks we start multiple push-es each selecting one video and one audio track to push to S3.
func (d *CatalystAPIHandlersCollection) TranscodeSegment(broadcasterPort int, mistProcPath string) httprouter.Handle {
	schema := inputSchemasCompiled["TranscodeSegment"]
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		// Input validation:
		var transcodeRequest TranscodeSegmentRequest
		payload, err := ioutil.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read body", err)
			return
		}
		result, err := schema.Validate(gojsonschema.NewBytesLoader(payload))
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "body schema validation failed", err)
			return
		}
		if !result.Valid() {
			errors.WriteHTTPBadBodySchema("TranscodeSegment", w, result.Errors())
			return
		}
		if err := json.Unmarshal(payload, &transcodeRequest); err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}
		callbackClient := clients.NewCallbackClient()
		errorOutInternal := func(where string, err error) {
			errors.WriteHTTPInternalServerError(w, where, err)
			if err := callbackClient.SendTranscodeStatusError(transcodeRequest.CallbackUrl, where); err != nil {
				fmt.Printf("error send transcode error %v\n", err)
				return
			}
		}

		inputStream, renditionsStream := generateStreamNames()
		if err = d.MistClient.AddStream(inputStream, transcodeRequest.SourceFile); err != nil {
			errorOutInternal("error AddStream(inputStream)", err)
			return
		}
		// defer d.MistClient.DeleteStream(inputStream)

		if err = d.MistClient.AddStream(renditionsStream, "push://"); err != nil {
			errorOutInternal("error AddStream(renditionsStream)", err)
			return
		}
		// defer d.MistClient.DeleteStream(renditionsStream)

		// DEBUG: Experiment with all available triggers on renditionsStream.
		//        We are searching for event carrying info on new track-id.
		//        When having track-id we can start new push action selecting that track id + audio track-id
		// triggers := []string{"PUSH_OUT_START", "STREAM_READY", "LIVE_TRACK_LIST", "STREAM_END", "PUSH_REWRITE", "PUSH_OUT_START", "PUSH_END", "RECORDING_END", "STREAM_UNLOAD", "STREAM_LOAD", "STREAM_SOURCE", "OUTPUT_STOP", "OUTPUT_START"}
		triggers := []string{"LIVE_TRACK_LIST", "PUSH_END"}
		for _, value := range triggers {
			if err = d.MistClient.AddTrigger(renditionsStream, value); err != nil {
				errorOutInternal(fmt.Sprintf("error AddTrigger(renditionsStream, %s)", value), err)
			}
		}
		// mc := d.MistClient.(*MistClient)
		// trigs, err := mc.getCurrentTriggers()
		// fmt.Printf("triggers: %v", trigs)

		// start MistProcLivepeer
		configPayload, err := json.Marshal(configForSubprocess(&transcodeRequest, broadcasterPort, inputStream, renditionsStream))
		if err != nil {
			errorOutInternal("error ProcLivepeerConfig json encode", err)
			return
		}
		fmt.Printf("configPayload %s\n", configPayload)
		transcodeCommand := exec.Command(mistProcPath, "-")
		stdinPipe, err := transcodeCommand.StdinPipe()
		if err != nil {
			errorOutInternal("error transcodeCommand.StdinPipe()", err)
			return
		}
		commandOutputToLog(transcodeCommand, "coding")
		sent, err := stdinPipe.Write(configPayload)
		if err != nil {
			errorOutInternal("error stdinPipe.Write()", err)
			return
		}
		if sent != len(configPayload) {
			errorOutInternal("error short write on stdinPipe.Write()", err)
			return
		}
		// Do we need to send trailing \n ??
		err = stdinPipe.Close()
		if err != nil {
			errorOutInternal("error stdinPipe.Close()", err)
			return
		}
		err = transcodeCommand.Start()
		if err != nil {
			errorOutInternal("error start transcodeCommand", err)
			return
		}

		// Starting SOURCE_PREFIX stream. Why we need to start it manually? MistProcLivepeer should start it??
		if err := d.MistClient.PushStart(inputStream, "/opt/null.ts"); err != nil {
			errorOutInternal("error PushStart(inputStream)", err)
			return
		}

		err = transcodeCommand.Wait()
		if exit, ok := err.(*exec.ExitError); ok {
			fmt.Printf("MistProcLivepeer returned %d\n", exit.ExitCode())
		} else if err != nil {
			errorOutInternal("error exec transcodeCommand", err)
			return
		}

		// Push from B with track selector to s3
		// push is always from a stream to target
		// tracks selector is placed in s3 target url of push ("URL parameters" in the manual)
		// From mist manual: Any positive integer: Select this specific track ID. Does not apply if the given track ID does   not exist or is of the wrong type.
		// How to find track-id from B stream?
		// We can do that using trigger:
		// - LIVE_TRACK_LIST . Test this to see if that gets triggered !
		// - STREAM_READY

		// callbackClient := clients.NewCallbackClient()
		// if err := callbackClient.SendTranscodeStatusError(transcodeRequest.CallbackUrl, "NYI - not yet implemented"); err != nil {
		// 	errors.WriteHTTPInternalServerError(w, "error send transcode error", err)
		// 	return
		// }
		time.Sleep(2 * time.Second)
		io.WriteString(w, "OK") // TODO later
	}
}

func pipeToLog(pipe io.ReadCloser, name string) {
	data := make([]byte, 4096)
	for {
		count, err := pipe.Read(data)
		if err != nil {
			fmt.Printf("ERROR cmd=%s %v\n", name, err)
			return
		}
		fmt.Printf("out [%s] %s\n", name, string(data[0:count]))
	}
}

func commandOutputToLog(cmd *exec.Cmd, name string) {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("ERROR: cmd.StdoutPipe() %v\n", err)
		return
	}
	go pipeToLog(stdoutPipe, name)
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		fmt.Printf("ERROR: cmd.StderrPipe() %v\n", err)
		return
	}
	go pipeToLog(stderrPipe, name)
}

func (d *CatalystAPIHandlersCollection) UploadVOD() httprouter.Handle {
	schema := inputSchemasCompiled["UploadVOD"]

	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		var uploadVODRequest UploadVODRequest

		if !HasContentType(req, "application/json") {
			errors.WriteHTTPUnsupportedMediaType(w, "Requires application/json content type", nil)
			return
		} else if payload, err := ioutil.ReadAll(req.Body); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		} else if result, err := schema.Validate(gojsonschema.NewBytesLoader(payload)); err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot validate payload", err)
			return
		} else if !result.Valid() {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", nil)
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
		d.StreamCache[streamName] = StreamInfo{callbackUrl: uploadVODRequest.CallbackUrl}

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
	StreamCache map[string]StreamInfo
}

// This trigger is stream-specific and must be blocking.
// The payload for this trigger is multiple lines, each separated by a single newline character
// (without an ending newline), containing data as such: `stream name`\n`push target URI`
func (d *MistCallbackHandlersCollection) Trigger_LIVE_TRACK_LIST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Following code is responding to transcoding handler:
	streamName := "unknown"
	errorOutInternal := func(where string, err error) {
		txt := fmt.Sprintf("%s name=%s", where, streamName)
		errors.WriteHTTPInternalServerError(w, txt, err)
		fmt.Printf("<ERROR LIVE_TRACK_LIST> %s %v\n", txt, err)
	}
	payload, err := ioutil.ReadAll(req.Body)
	if err != nil {
		errorOutInternal("Cannot read payload", err)
		return
	}
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	streamName = lines[0]
	encodedTracks := lines[1]

	suffix, err := extractSuffix(streamName)
	if err != nil {
		errorOutInternal("unknown streamName format", err)
	}

	if streamEnded := encodedTracks == "null"; streamEnded {
		// SOURCE_PREFIX stream is no longer needed
		inputStream := fmt.Sprintf("%s%s", SOURCE_PREFIX, suffix)
		if err = d.MistClient.DeleteStream(inputStream); err != nil {
			fmt.Printf("<ERROR LIVE_TRACK_LIST> DeleteStream(%s) %v\n", inputStream, err)
		}
		// Multiple pushes from RENDITION_PREFIX are in progress. Lets close RENDITION_PREFIX stream at the end of all pushes.
		return
	}
	tracks := make(LiveTrackListTriggerJson)
	if err = json.Unmarshal([]byte(encodedTracks), &tracks); err != nil {
		errorOutInternal("LiveTrackListTriggerJson json decode error", err)
		return
	}
	// Start push per each video track
	for i := range tracks { // i is generated name, not important, all info is contained in element
		if tracks[i].Type != "video" {
			// Only produce an rendition per each video track, selecting best audio track
			continue
		}
		destination := fmt.Sprintf("/home/alex/livepeer/vod/mistserver/%s__%dx%d.ts?video=%d&audio=maxbps", streamName, tracks[i].Width, tracks[i].Height, tracks[i].Index) //.Id)
		fmt.Printf("> Starting push to %s\n", destination)
		if err := d.MistClient.PushStart(streamName, destination); err != nil {
			fmt.Printf("> ERROR push to %s %v\n", destination, err)
		}
	}
	fmt.Printf("> Trigger_LIVE_TRACK_LIST %v\n", lines)
}

func (d *MistCallbackHandlersCollection) Trigger_PUSH_END(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {

	// Following code is responding to segmenting handler:
	payload, err := ioutil.ReadAll(req.Body)
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
			if payload, err := ioutil.ReadAll(req.Body); err == nil {
				// print info for testing purposes
				fmt.Printf("TRIGGER %s\n%s\n", whichOne, string(payload))
			}
			errors.WriteHTTPBadRequest(w, "Unsupported X-Trigger", fmt.Errorf("unknown trigger '%s'", whichOne))
			return
		}
	}
}
