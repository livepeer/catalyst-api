package video

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"gopkg.in/vansante/go-ffprobe.v2"
	"io"
	"os"
	"os/exec"
	"testing"
)

func TestFixFPS(t *testing.T) {
	_, err := exec.LookPath("mp4box")
	if err != nil {
		fmt.Println("No mp4box installed, test skipped")
		return
	}

	mp4File := "fixtures/mp4_vp9_fps_10.mp4"
	err = copyFile("fixtures/mp4_vp9.mp4", mp4File)
	defer os.Remove(mp4File)
	require.NoError(t, err)

	err = fixFPS(context.TODO(), mp4File, 10)
	require.NoError(t, err)

	res, err := ffprobe.ProbeURL(context.TODO(), mp4File)
	require.NoError(t, err)
	require.Equal(t, "10/1", res.Streams[0].AvgFrameRate)

}

func copyFile(inFile string, outFile string) error {
	source, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(outFile)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	return err
}
