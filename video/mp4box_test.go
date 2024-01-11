package video

import (
	"context"
	"fmt"
	"testing"
)

// TODO: Update to the real test
func TestMp4Box(t *testing.T) {
	fmt.Println("HELLO")
	err := fixFps(context.TODO(), "/Users/rafalleszko/Downloads/test2/source.mp4", 24)
	fmt.Printf("%s\n", err)
}
