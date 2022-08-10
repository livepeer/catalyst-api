package handlers

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/assert"
)

type WebhookReceiver struct {
	handler  func(w http.ResponseWriter, req *http.Request, _ httprouter.Params)
	port     int
	requests chan []byte
	router   *httprouter.Router
	server   *http.Server
}

func (s *WebhookReceiver) Init() {
	s.requests = make(chan []byte, 1000)
	s.router = httprouter.New()
	if s.handler == nil {
		s.handler = func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
			payload, err := ioutil.ReadAll(req.Body)
			if err != nil {
				fmt.Printf("WebhookReceiver error reading req body\n")
				w.WriteHeader(451)
				return
			}
			w.WriteHeader(200)
			s.requests <- payload
		}
	}
	s.router.POST("/callback", s.handler)
	s.server = &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", s.port), Handler: s.router}
	go func() {
		if err := s.server.ListenAndServe(); err != nil {
			if err.Error() == "http: Server closed" {
				return // normal exit
			}
			fmt.Printf("server.ListenAndServe() %v\n", err)
		}
	}()
}

func (s *WebhookReceiver) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		fmt.Printf("server.Shutdown() %v\n", err)
	}
}

func (s *WebhookReceiver) WaitForCallback(t *testing.T, timeout time.Duration) []byte {
	select {
	case data := <-s.requests:
		return data
	case <-time.After(timeout):
		assert.FailNow(t, "WaitForCallback timedout")
	}
	return nil
}

func NewWebhookReceiver(port int) *WebhookReceiver {
	server := &WebhookReceiver{port: port}
	server.Init()
	return server
}
