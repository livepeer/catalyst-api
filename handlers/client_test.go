package handlers

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPayload(t *testing.T) {
	require := require.New(t)

	require.Equal(
		"command=%7B%22addstream%22%3A%7B%22somestream%22%3A%7B%22source%22%3A%22http%3A%2F%2Fsome-storage-url.com%2Fvod.mp4%22%7D%7D%7D",
		testPayloadFor(t, commandAddStream("somestream", "http://some-storage-url.com/vod.mp4")),
	)
	require.Equal(
		"command=%7B%22push_start%22%3A%7B%22stream%22%3A%22somestream%22%2C%22target%22%3A%22http%3A%2F%2Fsome-target-url.com%2Ftarget.mp4%22%7D%7D",
		testPayloadFor(t, commandPushStart("somestream", "http://some-target-url.com/target.mp4")),
	)
	require.Equal(
		"command=%7B%22deletestream%22%3A%7B%22somestream%22%3Anull%7D%7D",
		testPayloadFor(t, commandDeleteStream("somestream")),
	)
}

func testPayloadFor(t *testing.T, command interface{}) string {
	c, err := toCommandString(command)
	require.NoError(t, err)
	return payloadFor(c)
}

// TODO: Remove after initial testing
func TestWorkflow(t *testing.T) {
	// first copy file into /home/Big_Buck_Bunny_1080_10s_1MB.mp4

	processUploadVOD("/home/Big_Buck_Bunny_1080_10s_1MB.mp4")
}
