package steps

import (
	"fmt"
	"os/exec"

	"github.com/cucumber/godog"
)

// Confirm that we have an ffmpeg binary on the system the tests are running on
func (s *StepContext) CheckFfmpeg() error {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return fmt.Errorf("could not find the 'ffmpeg' binary, which the tests depend on")
	}
	return nil
}

func (s *StepContext) AllOfTheSourceSegmentsAreWrittenToStorageWithinSeconds(secs int64) error {
	// s.SourceOutputDir
	return godog.ErrPending
}

func (s *StepContext) TheSourceManifestIsWrittenToStorageWithinSeconds(secs int64) error {
	return godog.ErrPending
}
