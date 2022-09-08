package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
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
//
// The process looks as follows:
// 1. Add SOURCE stream to Mist using MistClient
// 2. Execute the `MistProcLivepeer` process to perform the following operations:
//    - Take SOURCE stream and segment it
//    - Push each segment to Broadcaster
//    - Receive transcoded segment from Broadcaster, ingest into RENDITION stream
// 3. Start SOURCE stream by creating, unused, push to /tmp/mist/<stream-name>.mkv
// 4. `MistProcLivepeer` process is unblocked, starts to work
// 5. Return 200 OK response
// 5. Respond to LIVE_TRACK_LIST trigger:
//    - Extract track information
//    - For each video track create push to S3 destination
//    - Transcoding is complete if there is no tracks. Then delete SOURCE stream
// 6. Respond to PUSH_END trigger:
//    - Determine is it error or successful upload
//    - Invoke callback to studio
// 7. When all started pushes are complete invoke callback to studio; remove stream from StreamCache
func (d *CatalystAPIHandlersCollection) TranscodeSegment(mistProcPath string) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		t := Transcoding{httpResp: w, httpReq: req, broadcasterPort: 8935, mistProcPath: mistProcPath}
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
