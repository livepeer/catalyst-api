package video

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
)

func fixFps(mp4File string, fps int64) error {
	fixedMp4File := fmt.Sprintf("%s-fixed", mp4File)

	args := []string{
		"-add",
		fmt.Sprintf("%s:fps=%d", mp4File, fps),
		fixedMp4File,
	}

	_, err := runCmd(exec.CommandContext(context.TODO(), "mp4box", args...))
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
	err := cmd.Run()

	// TODO
	// Fix not returning errors for all stdErr
	return stdOut.String(), nil

	fmt.Printf("stdOut: %s", stdOut.String())
	fmt.Printf("stdErr: %s", stdErr.String())
	if len(stdErr.Bytes()) > 0 {
		return "", fmt.Errorf("failed creating C2PA Manifest: %s", stdErr.String())
	}

	return stdOut.String(), err
}
