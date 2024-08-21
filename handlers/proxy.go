package handlers

import (
	"io"
	"net/http"

	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/errors"
)

// ProxyRequest proxies a request to a target endpoint
func ProxyRequest(targetEndpoint string) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		// Create a new request to the target endpoint
		proxyReq, err := http.NewRequest(req.Method, targetEndpoint, req.Body)
		if err != nil {
			glog.Errorf("Cannot create proxy request: %s", err)
			errors.WriteHTTPInternalServerError(w, "Cannot create proxy request", err)
			return
		}
		for k, v := range req.Header {
			proxyReq.Header.Set(k, v[0])
		}

		// Send the request to the target endpoint
		client := &http.Client{}
		resp, err := client.Do(proxyReq)
		if err != nil {
			glog.Errorf("Cannot send proxy request: %s", err)
			errors.WriteHTTPInternalServerError(w, "Cannot send proxy request", err)
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			glog.Errorf("Cannot read response body: %s", err)
			errors.WriteHTTPInternalServerError(w, "Cannot read response body", err)
			return
		}
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		w.Write(body) // nolint:errcheck
	}
}
