package handlers

import (
	"net/url"

	"github.com/livepeer/catalyst-api/pipeline"
)

type CatalystAPIHandlersCollection struct {
	VODEngine            *pipeline.Coordinator
	checkWritePermission func(string, string, ...*url.URL) error
}
