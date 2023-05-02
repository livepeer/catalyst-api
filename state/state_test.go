package state

import (
	"testing"

	"github.com/livepeer/catalyst-api/events"
	v0 "github.com/livepeer/catalyst-api/schema/v0"
	"github.com/stretchr/testify/require"
)

func TestAction(t *testing.T) {
	machine := NewMachine()
	evt := &events.SignedEvent{
		Signature: "fake",
		Action: v0.ChannelDefinition{
			ID:   "my-awesome-stream",
			Time: int64(1681403259137),
			MultistreamTargets: []v0.MultistreamTarget{{
				URL: "rtmp://localhost/foo/bar",
			}},
		},
	}
	err := machine.HandleEvent(evt)
	require.NoError(t, err)

	require.Len(t, machine.State.Streams, 1)
	stream, ok := machine.State.Streams["my-awesome-stream"]
	require.True(t, ok)
	require.Len(t, stream.MultistreamTargets, 1)
	tar := stream.MultistreamTargets[0]
	require.Equal(t, tar.URL, "rtmp://localhost/foo/bar")
}
