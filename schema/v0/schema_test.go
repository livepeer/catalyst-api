package v0

import (
	"encoding/json"
	"testing"

	"github.com/livepeer/catalyst-api/events"
	"github.com/stretchr/testify/require"
)

func TestSign(t *testing.T) {
	schemas := []*events.Schema{&Schema}
	signer := events.Signer{
		Schemas:       schemas,
		PrimarySchema: &Schema,
	}
	var testMessage = ChannelDefinition{
		ID:   "my-awesome-stream",
		Time: int64(1681403259137),
		MultistreamTargets: []MultistreamTarget{{
			URL: "rtmp://localhost/foo/bar",
		}},
	}
	signedEvent, err := signer.Sign(testMessage)
	require.NoError(t, err)
	_, err = json.Marshal(signedEvent.UnverifiedEvent())
	require.NoError(t, err)
}

func TestVerify(t *testing.T) {
	schemas := []*events.Schema{&Schema}
	signer := events.Signer{
		Schemas:       schemas,
		PrimarySchema: &Schema,
	}
	var unverified events.UnverifiedEvent
	err := json.Unmarshal(validBody, &unverified)
	require.NoError(t, err)
	signed, err := signer.Verify(unverified)
	require.NoError(t, err)
	act, ok := signed.Action.(*ChannelDefinition)
	require.True(t, ok)
	require.Equal(t, act.ID, "my-awesome-stream")
	require.Equal(t, act.Time, int64(1681403259137))
	require.Len(t, act.MultistreamTargets, 1)
	require.Equal(t, act.MultistreamTargets[0].URL, "rtmp://localhost/foo/bar")
}

var validBody = []byte(`
	{
		"primaryType": "ChannelDefinition",
		"domain": {
			"name": "Livepeer Decentralized Video Protocol",
			"version": "0.0.1",
			"salt": "f8b3858ac49ca50b138587d5dace09bd102b9d24d2567d9a5cde2f6122810931"
		},
		"message": {
			"id": "my-awesome-stream",
			"multistreamTargets": [
				{
					"url": "rtmp://localhost/foo/bar"
				}
			],
			"time": 1681403259137
		},
		"signature": "0xa9130d4936a436e57da67112aaa41751cb210bc5766c674a6b931a7263f8f53b13cc67fe809aee8b1b2eecdff9c27cd51bf374e0a725a6df07b86666489a85d11b"
	}
`)

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
