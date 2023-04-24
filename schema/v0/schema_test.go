package v0

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

	_, err = signer.Verify(signedEvent)
	require.NoError(t, err)
}

// URL Capitalized
var invalidBody = []byte(`
	{
		"sig": "0xde88d39bcf39ea6033114394187d26031c31ecef6c2f847c64fb0ecb9e3a94977e7a34f0b2c3b0064bba34b43e92388494f87ce5b0b747a0e222b08579b1f9fe1c",
		"event": {
			"primaryType": "EventChannelDefinitionMeta",
			"domain": {
				"name": "Livepeer Decentralized Video Protocol",
				"version": "0.0.1",
				"salt": "f8b3858ac49ca50b138587d5dace09bd102b9d24d2567d9a5cde2f6122810931"
			},
			"message": {
				"id": "my-awesome-stream",
				"time": {},
				"multistreamTargets": [
					{
						"URL": "rtmp://localhost/foo/bar"
					}
				]
			}
		}
	}
`)

func TestInvalidVerify(t *testing.T) {
	signer := events.Signer{Types: Types}
	var signed events.SignedEvent
	err := json.Unmarshal(invalidBody, &signed)
	require.NoError(t, err)
	_, err = signer.Verify(signed)
	require.Error(t, err)
}
