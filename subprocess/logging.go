package subprocess

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/livepeer/catalyst-api/log"
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
			log.LogNoRequestID("streamOutput() improper termination", "line", line)
			return
		}
		if err != nil {
			log.LogNoRequestID("streamOutput ReadSlice error", "err", err)
			return
		}
		_, err = out.Write(line)
		if err != nil {
			log.LogNoRequestID("streamOutput out.Write error", "err", err)
			return
		}
	}
}

// LogOutputs starts new goroutines to print cmd's stdout & stderr to our stdout & stderr
func LogOutputs(cmd *exec.Cmd) error {
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to open stderr pipe: %s", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to open stdout pipe: %s", err)
	}
	go streamOutput(stderrPipe, os.Stderr)
	go streamOutput(stdoutPipe, os.Stdout)
	return nil
}
