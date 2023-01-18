package video

import (
	"context"
	//	"fmt"
	"gopkg.in/vansante/go-ffprobe.v2"
	"io"
	"time"
)

/*
type ProbeInfo struct {
	Format  *ffprobe.Format
	Streams []*ffprobe.Stream
}

func (p ProbeInfo) String() string {
	return fmt.Sprintf("Format: %v \nStreams: %v", p.Format, p.Streams)
}
*/

func ProbeFileReader(ctx context.Context, fileReader io.Reader) (*ffprobe.ProbeData, error) {
	return ffprobe.ProbeReader(ctx, fileReader)
}

func ProbeFileFromOS(file io.Reader) (*ffprobe.ProbeData, error) {
	var probeInfo ffprobe.ProbeData
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer probeCancel()

	// TODO: add HTTP probing (via ffprobe.ProbeURL in addition to ffprobe.ProbeReader)
	data, err := ProbeFileReader(probeCtx, file)
	if err != nil {
		return nil, err
	}

	probeInfo.Format = data.Format
	probeInfo.Streams = data.Streams
	return &probeInfo, err
}
