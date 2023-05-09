package steps

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
)

type Callback struct {
	RequestID       string `json:"request_id"`
	CompletionRatio int    `json:"completion_ratio"`
	Status          string `json:"status"`
	Timestamp       int    `json:"timestamp"`
	Type            string `json:"type"`
	VideoSpec       struct {
		Format string `json:"format"`
		Tracks []struct {
			Type      string  `json:"type"`
			Codec     string  `json:"codec"`
			Bitrate   int     `json:"bitrate"`
			Duration  float64 `json:"duration"`
			Size      int     `json:"size"`
			StartTime int     `json:"start_time"`
			Width     int     `json:"width,omitempty"`
			Height    int     `json:"height,omitempty"`
			Fps       int     `json:"fps,omitempty"`
			Channels  int     `json:"channels,omitempty"`
		} `json:"tracks"`
		Duration float64 `json:"duration"`
	} `json:"video_spec"`
	Outputs []struct {
		Type     string `json:"type"`
		Manifest string `json:"manifest"`
		Videos   []struct {
			Type     string `json:"type"`
			Size     int    `json:"size"`
			Location string `json:"location"`
		} `json:"videos"`
	} `json:"outputs"`
}

func (s *StepContext) StartCallbackHandler(listen string) error {
	s.CallbacksReceived = []Callback{}

	router := httprouter.New()
	router.POST("/callback/:ID", func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("error reading body: %s", err), http.StatusInternalServerError)
			return
		}

		var callback Callback
		if err := json.Unmarshal(body, &callback); err != nil {
			http.Error(w, fmt.Sprintf("error parsing callback: %s", err), http.StatusInternalServerError)
			return
		}

		s.CallbacksReceived = append(s.CallbacksReceived, callback)
	})
	router.GET("/ok", func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		w.WriteHeader(http.StatusOK)
	})

	s.CallbackHandler = http.Server{Addr: listen, Handler: router}
	go func() {
		_ = s.CallbackHandler.ListenAndServe()
	}()

	WaitForStartup("http://" + listen + "/ok")

	return nil
}

func (s *StepContext) CheckCallback(callbackType string, seconds int) error {
	for i := 0; i < 2*seconds; i++ {
		for _, callback := range s.CallbacksReceived {
			if callback.Status == callbackType {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	var callbackTypes []string
	for _, callback := range s.CallbacksReceived {
		callbackTypes = append(callbackTypes, callback.Status)
	}

	return fmt.Errorf("Did not receive callback of type %q within %d seconds. Received %d total callbacks: %v", callbackType, seconds, len(s.CallbacksReceived), callbackTypes)
}
