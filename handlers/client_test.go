package handlers

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPayload(t *testing.T) {
	require := require.New(t)

	require.Equal(
		"command=%7B%22addstream%22%3A%7B%22somestream%22%3A%7B%22source%22%3A%22http%3A%2F%2Fsome-storage-url.com%2Fvod.mp4%22%7D%7D%7D",
		payloadFor(commandAddStream("somestream", "http://some-storage-url.com/vod.mp4")),
	)
	require.Equal(
		"command=%7B%22push_start%22%3A%7B%22stream%22%3A%22somestream%22%2C%22target%22%3A%22http%3A%2F%2Fsome-target-url.com%2Ftarget.mp4%22%7D%7D",
		payloadFor(commandPushStart("somestream", "http://some-target-url.com/target.mp4")),
	)
	require.Equal(
		"command=%7B%22deletestream%22%3A%7B%22somestream%22%3A%7B%7D%7D%7D",
		payloadFor(commandDeleteStream("somestream")),
	)
}

// TODO: Remove after initial testing
func TestWorkflow(t *testing.T) {
	// first copy file into /home/Big_Buck_Bunny_1080_10s_1MB.mp4
	mc := MistClient{apiUrl: "http://localhost:4242/api2"}
	streamName, err := mc.AddStream("/home/Big_Buck_Bunny_1080_10s_1MB.mp4")
	fmt.Println(streamName)
	fmt.Println(err)
	defer mc.DeleteStream(streamName)
	resp, err := mc.PushStart(streamName, "/media/recording/result.ts")
	fmt.Println(resp)
	fmt.Println(err)
	// wait until push it complete
	time.Sleep(5 * time.Second)
}
