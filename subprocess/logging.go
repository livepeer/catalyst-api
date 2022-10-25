package subprocess

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/livepeer/catalyst-api/config"
)

func streamOutput(src io.Reader, out io.Writer) {
	s := bufio.NewReader(src)
	for {
		var line []byte
		line, err := s.ReadSlice('\n')
		if err == io.EOF && len(line) == 0 {
			break
		}
		if err == io.EOF {
			_ = config.Logger.Log("msg", "streamOutput() improper termination", "line", line)
			return
		}
		if err != nil {
			_ = config.Logger.Log("msg", "streamOutput ReadSlice error", "err", err)
			return
		}
		_, err = out.Write(line)
		if err != nil {
			_ = config.Logger.Log("msg", "streamOutput out.Write error", "err", err)
			return
		}
	}
}

func LogStdout(cmd *exec.Cmd) error {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to open stdout pipe: %v", err)
	}
	go streamOutput(stdoutPipe, os.Stdout)
	return nil
}

func LogStderr(cmd *exec.Cmd) error {
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to open stderr pipe: %v", err)
	}
	go streamOutput(stderrPipe, os.Stderr)
	return nil
}

// LogOutputs starts new goroutines to print cmd's stdout & stderr to our stdout & stderr
func LogOutputs(cmd *exec.Cmd) error {
	if err := LogStderr(cmd); err != nil {
		return err
	}
	if err := LogStdout(cmd); err != nil {
		return err
	}
	return nil
}
