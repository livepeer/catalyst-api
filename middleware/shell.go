package middleware

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"sync"
	"time"
)

type Shell struct {
	mu           sync.Mutex
	Cmd          string
	Args         []string
	IntervalSecs time.Duration
	TimeoutSecs  time.Duration
}

func NewShell(interval, timeout time.Duration, cmd string, args ...string) (*Shell, error) {
	if interval < 0 {
		return &Shell{}, fmt.Errorf("shell needs to be set with a valid interval value")
	}
	if timeout < 0 {
		return &Shell{}, fmt.Errorf("shell needs a valid timeout value")
	}
	return &Shell{
		Cmd:          cmd,
		Args:         args,
		IntervalSecs: interval,
		TimeoutSecs:  timeout,
	}, nil
}

func (s *Shell) RunBg() *time.Ticker {
	ticker := time.NewTicker(s.IntervalSecs)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			err := s.Run()
			if err != nil {
				log.Println("cmd: failed to start", s.Cmd)
				break
			}
		}
	}()
	return ticker
}

func (s *Shell) Run() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), s.TimeoutSecs)
	defer cancel()

	log.Printf("cmd: running: %s %v, interval:%s, timeout:%s\n", s.Cmd, s.Args, s.IntervalSecs, s.TimeoutSecs)
	cmd := exec.CommandContext(ctx, s.Cmd, s.Args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("cmd: failed to Run(): %s\n", err)
	}
	log.Printf("cmd: output: %s\n", out)

	return err
}
