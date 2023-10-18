package thumbnails

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/go-tools/drivers"
	ffmpeg "github.com/u2takey/ffmpeg-go"
)

const framesEverySecs = 5
const resolution = "320:240"

func GenerateThumbs(input *url.URL, output *url.URL) error {
	in, err := clients.SignURL(input)
	if err != nil {
		return err
	}
	tempDir, err := os.MkdirTemp(os.TempDir(), "thumbs-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	err = ffmpeg.Input(in).
		Output(
			path.Join(tempDir, "/keyframes_%03d.jpg"),
			ffmpeg.KwArgs{
				// video filter to extract frames every x seconds and resize
				"vf":  fmt.Sprintf("fps=1/%d,scale=%s:force_original_aspect_ratio=decrease", framesEverySecs, resolution),
				"q:v": "2", // quality level for the jpgs
			},
		).OverWriteOutput().ErrorToStdOut().Run()
	if err != nil {
		return fmt.Errorf("error running ffmpeg %w", err)
	}

	// generate the webvtt file
	files, err := filepath.Glob(path.Join(tempDir, "keyframes*"))
	if err != nil {
		return err
	}
	builder := &bytes.Buffer{}
	_, err = builder.WriteString("WEBVTT\n")
	if err != nil {
		return err
	}
	timestamp := time.Time{}
	outputLocation := output.JoinPath("thumbnails").String()
	for _, file := range files {
		filename := path.Base(file)
		const layout = "15:04:05.000"
		start := timestamp.Format(layout)
		timestamp = timestamp.Add(time.Duration(framesEverySecs) * time.Second)
		end := timestamp.Format(layout)

		_, err := builder.WriteString(fmt.Sprintf("%s --> %s\n%s\n\n", start, end, filename))
		if err != nil {
			return err
		}

		fileReader, err := os.Open(file)
		if err != nil {
			return err
		}
		defer fileReader.Close()
		err = clients.UploadToOSURL(outputLocation, filename, fileReader, time.Minute)
		if err != nil {
			return err
		}
	}

	err = clients.UploadToOSURLFields(outputLocation, "thumbnails.vtt", builder, time.Minute, &drivers.FileProperties{ContentType: "text/vtt"})
	if err != nil {
		return err
	}
	return nil
}
