package video

import (
	"context"
	"time"

	"gopkg.in/vansante/go-ffprobe.v2"
)

func ProbeURL(url string) (*ffprobe.ProbeData, error) {
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer probeCancel()
	return ffprobe.ProbeURL(probeCtx, url)
}
