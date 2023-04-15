package schema

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/livepeer/catalyst-api/events"
	"github.com/stretchr/testify/require"
)

func TestSign(t *testing.T) {
	signer := events.Signer{Types: Types}
	var testMessage = ChannelDefinition{
		ID:   "my-awesome-stream",
		Time: *big.NewInt(1681403259137),
		MultistreamTargets: []MultistreamTarget{{
			URL: "rtmp://localhost/foo/bar",
		}},
	}
	event := events.Event{
		PrimaryType: "EventChannelDefinitionMeta",
		Domain:      Domain,
		Message:     testMessage,
	}
	signedEvent := signer.Sign(event)
	_, err := json.Marshal(signedEvent)
	require.NoError(t, err)

	addr, err := signer.Verify(signedEvent)
	require.NoError(t, err)
	panic(addr)
}
