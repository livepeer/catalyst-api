package steps

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/grafov/m3u8"
	"github.com/livepeer/go-tools/drivers"
)

// Confirm that we have an ffmpeg binary on the system the tests are running on
func (s *StepContext) CheckFfmpeg() error {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return fmt.Errorf("could not find the 'ffmpeg' binary, which the tests depend on")
	}
	return nil
}

func (s *StepContext) AllOfTheSourceSegmentsAreWrittenToStorageWithinSeconds(numSegments, secs int) error {
	osDriver, err := drivers.ParseOSURL(s.SourceOutputDir, true)
	if err != nil {
		return fmt.Errorf("could not parse object store url: %w", err)
	}
	session := osDriver.NewSession(filepath.Join(s.latestRequestID, "source"))

	var latestNumSegments int
	for x := 0; x < secs; x++ { // retry loop
		if x > 0 {
			time.Sleep(time.Second)
		}
		page, err := session.ListFiles(context.Background(), "", "")
		if err != nil {
			log.Println("failed to list files: ", err)
			continue
		}

		latestNumSegments = len(page.Files())
		if latestNumSegments == numSegments+1 {
			return nil
		}
	}
	return fmt.Errorf("did not find the expected number of source segments in %s (wanted %d, got %d)", s.SourceOutputDir, numSegments, latestNumSegments)
}

func (s *StepContext) TheSourceManifestIsWrittenToStorageWithinSeconds(secs, numSegments int) error {
	osDriver, err := drivers.ParseOSURL(s.SourceOutputDir, true)
	if err != nil {
		return fmt.Errorf("could not parse object store url: %w", err)
	}
	session := osDriver.NewSession(filepath.Join(s.latestRequestID, "source/index.m3u8"))

	var (
		manifestBytes  []byte
		fileInfoReader *drivers.FileInfoReader
	)
	for x := 0; x < secs; x++ {
		fileInfoReader, err = session.ReadData(context.Background(), "")
		if err == nil {
			manifestBytes, err = io.ReadAll(fileInfoReader.Body)
			// Only break if the full manifest has been written
			if strings.HasSuffix(strings.TrimSpace(string(manifestBytes)), "#EXT-X-ENDLIST") {
				break
			}
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return err
	}
	if !strings.HasSuffix(strings.TrimSpace(string(manifestBytes)), "#EXT-X-ENDLIST") {
		return fmt.Errorf("did not receive a closing tag on the manifest within %d seconds. Got: %s", secs, string(manifestBytes))
	}

	// Do some basic checks that our manifest looks as we'd expect
	manifest, manifestType, err := m3u8.DecodeFrom(bytes.NewReader(manifestBytes), true)
	if err != nil {
		return fmt.Errorf("error parsing manifest: %w", err)
	}
	if manifestType != m3u8.MEDIA {
		return fmt.Errorf("expected Media playlist, but got a Master playlist")
	}

	mediaManifest := manifest.(*m3u8.MediaPlaylist)
	if len(mediaManifest.GetAllSegments()) != numSegments {
		return fmt.Errorf("expected %d segments but got %d in the following manifest: %s", numSegments, len(mediaManifest.GetAllSegments()), manifest)
	}
	if mediaManifest.Version() != 3 {
		return fmt.Errorf("expected manifest to be HLSv3 but got version: %d", mediaManifest.Version())
	}
	if mediaManifest.TargetDuration != 11.0 {
		return fmt.Errorf("expected manifest to have a Target Duration of 11 but got: %f", mediaManifest.TargetDuration)
	}
	if mediaManifest.MediaType != m3u8.VOD {
		return fmt.Errorf("expected manifest to have playlist type VOD but got: %v", mediaManifest.MediaType)
	}

	return nil
}
