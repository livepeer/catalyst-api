package pprof

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
)

func ListenAndServe(port int) error {
	return fmt.Errorf("pprof listener stopped: %w", http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", port), nil))
}
