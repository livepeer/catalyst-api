package steps

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/julienschmidt/httprouter"
	"github.com/minio/madmin-go"
)

var minioAddress = "127.0.0.1:9000"

func (s *StepContext) StartStudioAPI(listen string) error {
	router := httprouter.New()
	router.POST("/cb", func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		_, _ = io.WriteString(w, "")
	})
	router.POST("/api/access-control/gate", func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			panic(err)
		}
		gateRequest := make(map[string]interface{})
		err = json.Unmarshal(body, &gateRequest)
		if err != nil {
			panic(err)
		}
		s.GateAPICallType = gateRequest["type"]
		s.GateAPICallCount++
		w.Header().Set("Cache-Control", "max-age=600, stale-while-revalidate=900")
		w.WriteHeader(s.GateAPIStatus)
	})
	router.GET("/ok", func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		w.WriteHeader(http.StatusOK)
	})

	s.Studio = http.Server{Addr: listen, Handler: router}
	go func() {
		_ = s.Studio.ListenAndServe()
	}()

	WaitForStartup("http://" + listen + "/ok")

	return nil
}

func WaitForStartup(url string) {
	retries := backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 5)
	err := backoff.Retry(func() error {
		_, err := http.Get(url)
		return err
	}, retries)
	if err != nil {
		panic(err)
	}
}

func (s *StepContext) StartMist(listen string) error {
	router := httprouter.New()
	router.POST("/api2", func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			panic(err)
		}

		query, err := url.ParseQuery(string(body))
		if err != nil {
			panic(err)
		}

		var command map[string]interface{}
		err = json.Unmarshal([]byte(query["command"][0]), &command)
		if err != nil {
			panic(err)
		}

		addstream := command["addstream"]
		push_start := command["push_start"]

		if addstream != nil {
			stream := addstream.(map[string]interface{})

			for name, value := range stream {
				source := value.(map[string]interface{})
				streamName := name
				_, _ = io.WriteString(w, `{"authorize":{"status":"OK"},"streams":{"`+streamName+`":{"name":"`+streamName+`","source":"`+source["source"].(string)+`"},"incomplete list":1}}`)
				break
			}
		} else if push_start != nil {
			s.AddMistPushStartURL(push_start.(map[string]interface{})["target"].(string))
			_, _ = io.WriteString(w, `{"authorize":{"status":"OK"}}`)
		}
	})

	s.Mist = http.Server{Addr: listen, Handler: router}
	go func() {
		_ = s.Mist.ListenAndServe()
	}()

	return nil
}

func (s *StepContext) StartObjectStore() error {
	app := exec.Command("./minio", "--address "+minioAddress, "server", fmt.Sprint(os.TempDir(), "/minio"))
	outfile, err := os.Create("logs/minio.log")
	if err != nil {
		return err
	}
	defer outfile.Close()
	app.Stdout = outfile
	app.Stderr = outfile
	if err := app.Start(); err != nil {
		return err
	}

	madmin, err := madmin.New(minioAddress, "minioadmin", "minioadmin", false)
	if err != nil {
		return err
	}

	s.MinioAdmin = madmin

	return nil
}
