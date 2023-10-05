package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"

	"github.com/livepeer/catalyst-api/clients"
)

const framesEverySecs = 5

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

	args := []string{
		"-i", in,
		"-vf", "select=eq(pict_type\\,I)",
		"-vsync", "vfr",
		"-vf", fmt.Sprintf("fps=1/%d", framesEverySecs),
		path.Join(tempDir, "/keyframes_%03d.jpg"),
	}

	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(timeout, "ffmpeg", args...)

	var outputBuf bytes.Buffer
	var stdErr bytes.Buffer
	cmd.Stdout = &outputBuf
	cmd.Stderr = &stdErr

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("error running ffmpeg [%s] [%s] %w", outputBuf.String(), stdErr.String(), err)
	}

	// generate the webvtt file
	files, err := filepath.Glob(path.Join(tempDir, "keyframes*"))
	if err != nil {
		return err
	}
	builder := bytes.Buffer{}
	_, err = builder.WriteString("WEBVTT\n")
	if err != nil {
		return err
	}
	timestamp := time.Time{}
	outputLocation := output.JoinPath("thumbnails").String()
	for _, file := range files {
		filename := path.Base(file)
		start := timestamp.Format("15:04:05.000")
		timestamp = timestamp.Add(time.Duration(framesEverySecs) * time.Second)
		end := timestamp.Format("15:04:05.000")

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

	// TODO set content-type
	err = clients.UploadToOSURL(outputLocation, "thumbnails.vtt", bytes.NewReader(builder.Bytes()), time.Minute)
	if err != nil {
		return err
	}
	return nil
}
