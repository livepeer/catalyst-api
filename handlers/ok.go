package handlers

import (
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/log"
)

func (d *CatalystAPIHandlersCollection) Ok() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		if _, err := io.WriteString(w, "OK"); err != nil {
			log.LogNoRequestID("Failed to write HTTP response for " + req.URL.RawPath)
		}
	}
}
