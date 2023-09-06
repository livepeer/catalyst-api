package steps

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
)

type JobCreationRequest struct {
	AccelerationSettings struct {
		Mode string `json:"mode"`
	} `json:"accelerationSettings"`
	ClientRequestToken string `json:"clientRequestToken"`
	Role               string `json:"role"`
	Settings           struct {
		Inputs []struct {
			AudioSelectors struct {
				AudioSelector1 struct {
					DefaultSelection string `json:"defaultSelection"`
				} `json:"Audio Selector 1"`
			} `json:"audioSelectors"`
			FileInput      string `json:"fileInput"`
			TimecodeSource string `json:"timecodeSource"`
			VideoSelector  struct {
				Rotate string `json:"rotate"`
			} `json:"videoSelector"`
		} `json:"inputs"`
		OutputGroups []struct {
			CustomName          string `json:"customName"`
			Name                string `json:"name"`
			OutputGroupSettings struct {
				HlsGroupSettings struct {
					Destination      string `json:"destination"`
					MinSegmentLength int    `json:"minSegmentLength"`
					SegmentLength    int    `json:"segmentLength"`
				} `json:"hlsGroupSettings"`
				Type string `json:"type"`
			} `json:"outputGroupSettings"`
			Outputs []struct {
				AudioDescriptions []struct {
					CodecSettings struct {
						AacSettings struct {
							Bitrate    int    `json:"bitrate"`
							CodingMode string `json:"codingMode"`
							SampleRate int    `json:"sampleRate"`
						} `json:"aacSettings"`
						Codec string `json:"codec"`
					} `json:"codecSettings"`
				} `json:"audioDescriptions"`
				ContainerSettings struct {
					Container string `json:"container"`
				} `json:"containerSettings"`
				NameModifier string `json:"nameModifier"`
			} `json:"outputs"`
		} `json:"outputGroups"`
		TimecodeConfig struct {
			Source string `json:"source"`
		} `json:"timecodeConfig"`
	} `json:"settings"`
}

func (s *StepContext) StartMediaconvert(listen string) error {
	router := httprouter.New()
	router.POST("/2017-08-29/jobs", func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		requestBody, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "couldn't read mediaconvert CreateJob request body:"+err.Error(), http.StatusInternalServerError)
			return
		}
		s.MediaconvertJobsReceived = append(s.MediaconvertJobsReceived, requestBody)

		body, err := json.Marshal(map[string]interface{}{
			"job": map[string]string{
				"id": "job-id-123",
			},
		})
		if err != nil {
			http.Error(w, "couldn't marshal mediaconvert CreateJobOutput:"+err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := w.Write(body); err != nil {
			http.Error(w, "couldn't write mediaconvert response:"+err.Error(), http.StatusInternalServerError)
			return
		}
	})
	router.GET("/2017-08-29/jobs/:id", func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		id := params.ByName("id")
		body, err := json.Marshal(map[string]interface{}{
			"job": map[string]string{
				"id":     id,
				"status": "COMPLETE",
			},
		})
		if err != nil {
			http.Error(w, "couldn't marshal mediaconvert CreateJobOutput:"+err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := w.Write(body); err != nil {
			http.Error(w, "couldn't write mediaconvert response:"+err.Error(), http.StatusInternalServerError)
			return
		}
	})
	router.GET("/ok", func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		w.WriteHeader(http.StatusOK)
	})

	s.Mediaconvert = http.Server{
		Addr:    listen,
		Handler: router,
	}
	go func() {
		_ = s.Mediaconvert.ListenAndServe()
	}()

	WaitForStartup("http://" + listen + "/ok")
	return nil
}

func (s *StepContext) MediaconvertReceivesAValidRequestJobCreationRequest(withinSecs int) error {
	for x := 0; x < withinSecs; x++ {
		if len(s.MediaconvertJobsReceived) == 1 {
			var job JobCreationRequest
			if err := json.Unmarshal(s.MediaconvertJobsReceived[0], &job); err != nil {
				return fmt.Errorf("could not parse mediaconvert job creation request: %w", err)
			}

			if len(job.Settings.OutputGroups) != 1 {
				return fmt.Errorf("only expected 1 output group in the mediaconvert job creation request but received %d", len(job.Settings.OutputGroups))
			}

			if len(job.Settings.OutputGroups[0].Outputs) != 1 {
				return fmt.Errorf("only expected 1 output in the mediaconvert job creation request but received %d", len(job.Settings.OutputGroups[0].Outputs))
			}

			if job.Settings.OutputGroups[0].Outputs[0].NameModifier != "audioonly" {
				return fmt.Errorf("expected an audioonly output but received %s", job.Settings.OutputGroups[0].Outputs[0].NameModifier)
			}

			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("did not receive a valid Mediaconvert job creation request with %d seconds (actually received %d)", withinSecs, len(s.MediaconvertJobsReceived))
}
