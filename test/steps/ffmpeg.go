package steps

import (
	"fmt"
	"os/exec"
)

// Confirm that we have an ffmpeg binary on the system the tests are running on
func (s *StepContext) CheckFfmpeg() error {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return fmt.Errorf("could not find the 'ffmpeg' binary, which the tests depend on")
	}
	return nil
}
