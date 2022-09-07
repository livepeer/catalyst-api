package handlers

import (
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/config"
)

func (d *CatalystAPIHandlersCollection) Ok() httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		if _, err := io.WriteString(w, "OK"); err != nil {
			_ = config.Logger.Log("error", "Failed to write HTTP response for "+req.URL.RawPath)
		}
	}
}
