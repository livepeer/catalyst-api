package state

import (
	"testing"

	"github.com/livepeer/catalyst-api/events"
	v0 "github.com/livepeer/catalyst-api/schema/v0"
	"github.com/stretchr/testify/require"
)

func TestSign(t *testing.T) {
	machine := NewMachine()
	evt := &events.SignedEvent{
		Signature: "fake",
		Event: events.Event{
			Message: v0.ChannelDefinition{
				ID: "awesome-stream",
				MultistreamTargets: []v0.MultistreamTarget{
					{URL: "rtmp://localhost/foo/bar"},
				},
			},
		},
	}
	machine.HandleEvent(evt)

	require.Len(t, machine.State.Streams, 1)
	stream, ok := machine.State.Streams["awesome-stream"]
	require.True(t, ok)
	require.Len(t, stream.MultistreamTargets, 1)
	tar := stream.MultistreamTargets[0]
	require.Equal(t, tar.URL, "rtmp://localhost/foo/bar")
}
