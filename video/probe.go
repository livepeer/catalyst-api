package video

import (
	"context"
	"time"

	"gopkg.in/vansante/go-ffprobe.v2"
)

type FFProbeClient interface {
	ProbeURL(url string) (*ffprobe.ProbeData, error)
}

type FFProbe struct {
}

func (f *FFProbe) ProbeURL(url string) (*ffprobe.ProbeData, error) {
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer probeCancel()
	return ffprobe.ProbeURL(probeCtx, url)
}
