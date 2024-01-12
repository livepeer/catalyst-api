package video

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
)

// fixFps fixes the output mp4 FPS which is needed because of a bug in ffmpeg: https://trac.ffmpeg.org/ticket/7939
// If this fix is not applied then, the output video has sync issues between audio and video tracks
func fixFps(ctx context.Context, mp4File string, fps float64) error {
	if fps == 0 {
		return errors.New("failed to fix FPS, invalid FPS: 0")
	}
	fixedMp4File := fmt.Sprintf("%s-fixedfps", mp4File)

	args := []string{
		"-add",
		fmt.Sprintf("%s:fps=%f", mp4File, fps),
		fixedMp4File,
	}

	_, err := runCmd(exec.CommandContext(ctx, "mp4box", args...))
	if err == nil {
		return os.Rename(fixedMp4File, mp4File)
	}
	return err
}

func runCmd(cmd *exec.Cmd) (string, error) {
	var stdOut bytes.Buffer
	var stdErr bytes.Buffer
	cmd.Stdout = &stdOut
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return stdOut.String(), fmt.Errorf("failed fix mp4 FPS: %s", stdErr.String())
	}
	return stdOut.String(), nil
}
