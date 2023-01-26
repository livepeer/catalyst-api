package video

import (
	"context"
	"io"
	"time"

	"gopkg.in/vansante/go-ffprobe.v2"
)

func ProbeFileReader(ctx context.Context, fileReader io.Reader) (*ffprobe.ProbeData, error) {
	return ffprobe.ProbeReader(ctx, fileReader)
}

func ProbeFileFromOS(file io.Reader) (*ffprobe.ProbeData, error) {
	var probeInfo ffprobe.ProbeData
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer probeCancel()

	data, err := ProbeFileReader(probeCtx, file)
	if err != nil {
		return nil, err
	}

	probeInfo.Format = data.Format
	probeInfo.Streams = data.Streams
	return &probeInfo, err
}
