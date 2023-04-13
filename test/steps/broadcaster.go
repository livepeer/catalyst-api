package steps

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
)

type Profiles struct {
	Profiles []Profile `json:"profiles"`
}

type Profile struct {
	Name    string `json:"name"`
	Profile string `json:"profile"`
}

func (s *StepContext) StartBroadcaster(listen string) error {
	s.BroadcasterSegmentsReceived = make(map[string]int)

	router := httprouter.New()
	router.POST("/live/:manifestID/:segmentNumber.ts", func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		manifestID := params.ByName("manifestID")
		s.latestManifestID = manifestID
		s.BroadcasterSegmentsReceived[manifestID] += 1

		// Extract the transcode profiles so we know what fake data to return
		transcodeConfig := r.Header.Get("Livepeer-Transcode-Configuration")
		var profiles Profiles
		err := json.Unmarshal([]byte(transcodeConfig), &profiles)
		if err != nil {
			http.Error(w, "couldn't unmarshal Livepeer-Transcode-Configuration:"+err.Error(), http.StatusMultipleChoices)
			return
		}

		mediatype, _, err := mime.ParseMediaType(r.Header.Get("Accept"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotAcceptable)
			return
		}
		if mediatype != "multipart/mixed" {
			http.Error(w, "set Accept: multipart/mixed", http.StatusMultipleChoices)
			return
		}

		boundary := fmt.Sprintf("boundary%d", rand.Int())
		accept := r.Header.Get("Accept")
		if accept == "multipart/mixed" {
			contentType := "multipart/mixed; boundary=" + boundary
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		mw := multipart.NewWriter(w)
		renditionData := "........fake rendition data........"
		var fw io.Writer
		for _, p := range profiles.Profiles {
			_ = mw.SetBoundary(boundary)
			length := len(renditionData)
			ext := ".ts"

			profile := p.Name
			fname := fmt.Sprintf(`"%s_%d%s"`, profile, s.BroadcasterSegmentsReceived[manifestID]-1, ext)
			hdrs := textproto.MIMEHeader{
				"Content-Type":        {"video/MP2T; name=" + fname},
				"Content-Length":      {strconv.Itoa(length)},
				"Content-Disposition": {"attachment; filename=" + fname},
				"Rendition-Name":      {profile},
			}
			fw, err = mw.CreatePart(hdrs)
			if err != nil {
				http.Error(w, fmt.Sprintf("Could not create multipart part err=%q", err), http.StatusInternalServerError)
				return
			}
			_, err = io.Copy(fw, bytes.NewBuffer([]byte(renditionData)))
			if err != nil {
				http.Error(w, fmt.Sprintf("errory copying multipart data err=%q", err), http.StatusInternalServerError)
				return
			}
		}
		if err == nil {
			err = mw.Close()
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("Error sending transcoded response url=%s err=%q", r.URL.String(), err), http.StatusInternalServerError)
			return
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

	})
	router.GET("/ok", func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		w.WriteHeader(http.StatusOK)
	})

	s.Broadcaster = http.Server{Addr: listen, Handler: router}
	go func() {
		_ = s.Broadcaster.ListenAndServe()
	}()

	WaitForStartup("http://" + listen + "/ok")

	return nil
}

func (s *StepContext) BroadcasterReceivesSegmentsWithinSeconds(numSegmentsExpected, secs int) error {
	for x := 0; x < secs*2; x++ {
		numSegmentsReceived := s.BroadcasterSegmentsReceived[s.latestManifestID]
		if numSegmentsExpected == numSegmentsReceived {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("expected %d segments to have been sent for transcoding but Broadcaster received %d", numSegmentsExpected, s.BroadcasterSegmentsReceived[s.latestManifestID])
}

func (s *StepContext) TranscodedSegmentsWrittenToDiskWithinSeconds(numSegmentsExpected int, profiles string, secs int) error {
	// Master manifest is written last, so we only need to do the timeout here and then can assume that all the other files are already written
	masterManifestPath := filepath.Join(s.TranscodedOutputDir, "index.m3u8")
	var err error
	for t := 0; t < secs*2; t++ {
		if _, err = os.Stat(masterManifestPath); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		return err
	}

	profileNames := strings.Split(profiles, ",")
	for _, profile := range profileNames {
		for segNum := 0; segNum < numSegmentsExpected; segNum++ {
			segPath := filepath.Join(s.TranscodedOutputDir, profile, fmt.Sprintf("%d.ts", segNum))
			if _, err := os.Stat(segPath); err != nil {
				return err
			}
		}
		renditionManifestPath := filepath.Join(s.TranscodedOutputDir, profile, "index.m3u8")
		if _, err := os.Stat(renditionManifestPath); err != nil {
			return err
		}
	}

	return nil
}
