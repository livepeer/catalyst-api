package steps

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Confirm that we have an ffmpeg binary on the system the tests are running on
func (s *StepContext) CheckFfmpeg() error {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return fmt.Errorf("could not find the 'ffmpeg' binary, which the tests depend on")
	}
	return nil
}

func (s *StepContext) AllOfTheSourceSegmentsAreWrittenToStorageWithinSeconds(numSegments, secs int) error {
	// Comes in looking like file:/var/folders/qr/sr8gs8916zd2wjbx50d3c3yc0000gn/T/livepeer/source
	// and we want /var/folders/qr/sr8gs8916zd2wjbx50d3c3yc0000gn/T/livepeer/source/aceaegdf/source/*.ts
	segmentingDir := filepath.Join(strings.TrimPrefix(s.SourceOutputDir, "file:"), s.latestRequestID, "source/*.ts")

	var latestNumSegments int
	for x := 0; x < secs; x++ {
		files, err := filepath.Glob(segmentingDir)
		if err != nil {
			return err
		}
		latestNumSegments = len(files)
		if latestNumSegments == numSegments {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("did not find the expected number of source segments in %s (wanted %d, got %d)", segmentingDir, numSegments, latestNumSegments)
}

func (s *StepContext) TheSourceManifestIsWrittenToStorageWithinSeconds(secs, numSegments int) error {
	// Comes in looking like file:/var/folders/qr/sr8gs8916zd2wjbx50d3c3yc0000gn/T/livepeer/source
	// and we want /var/folders/qr/sr8gs8916zd2wjbx50d3c3yc0000gn/T/livepeer/source/aceaegdf/source/index.m3u8
	sourceManifest := filepath.Join(strings.TrimPrefix(s.SourceOutputDir, "file:"), s.latestRequestID, "source/index.m3u8")

	var manifestBytes []byte
	var err error
	for x := 0; x < secs; x++ {
		manifestBytes, err = os.ReadFile(sourceManifest)
		if err == nil {
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
	manifest := string(manifestBytes)

	// Do some basic checks that our manifest looks as we'd expect
	if !strings.HasSuffix(strings.TrimSpace(manifest), "#EXT-X-ENDLIST") {
		return fmt.Errorf("did not receive a closing tag on the manifest within %d seconds. Got: %s", secs, manifest)
	}
	if !strings.HasPrefix(manifest, "#EXTM3U") {
		return fmt.Errorf("expected manifest to begin with #EXTM3U but got: %s", manifest)
	}
	if !strings.Contains(manifest, "#EXT-X-VERSION:3") {
		return fmt.Errorf("expected manifest to contain #EXT-X-VERSION:3 but got: %s", manifest)
	}
	if !strings.Contains(manifest, "#EXT-X-TARGETDURATION:10") {
		return fmt.Errorf("expected manifest to contain #EXT-X-TARGETDURATION:10 but got: %s", manifest)
	}

	for segNum := 0; segNum < numSegments; segNum++ {
		expectedSegmentFilename := fmt.Sprintf("index%d.ts", segNum)
		hasSegment := strings.Contains(manifest, expectedSegmentFilename)
		if !hasSegment {
			return fmt.Errorf("could not find segment %s in the following manifest: %s", expectedSegmentFilename, manifest)
		}
	}

	// Check one segment past the end to ensure we don't have more segments that we expect
	notExpectedSegmentFilename := fmt.Sprintf("index%d.ts", numSegments)
	if strings.Contains(manifest, notExpectedSegmentFilename) {
		return fmt.Errorf("found unexpected segment %s in the following manifest: %s", notExpectedSegmentFilename, manifest)
	}

	return nil
}
