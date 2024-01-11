package video

import (
	"fmt"
	"testing"
)

// TODO: Update to the real test
func TestMp4Box(t *testing.T) {
	fmt.Println("HELLO")
	err := fixFps("/Users/rafalleszko/Downloads/test2/source.mp4", 24)
	fmt.Printf("%s\n", err)
}
