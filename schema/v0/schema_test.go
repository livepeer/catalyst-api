package v0

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/livepeer/catalyst-api/events"
	"github.com/stretchr/testify/require"
)

func getSigner() events.Signer {
	schemas := []*events.Schema{&Schema}
	signer := events.EIP712Signer{
		Schemas:       schemas,
		PrimarySchema: &Schema,
	}
	return &signer
}

func TestSign(t *testing.T) {
	signer := getSigner()
	var testMessage = ChannelDefinition{
		ID:     "my-awesome-stream",
		Signer: "0x1964035e4C3cD05b8Ff839EFBf37063D8d1Ba7ae",
		Time:   int64(1681403259137),
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
	signer := getSigner()
	var unverified events.UnverifiedEvent
	err := json.Unmarshal(validBody, &unverified)
	require.NoError(t, err)
	signed, err := signer.Verify(unverified)
	require.NoError(t, err)
	require.Equal(t, signed.Address.String(), "0x1964035e4C3cD05b8Ff839EFBf37063D8d1Ba7ae")
	act, ok := signed.Action.(*ChannelDefinition)
	require.True(t, ok)
	require.Equal(t, act.ID, "my-awesome-stream")
	require.Equal(t, act.Time, int64(1681403259137))
	require.Len(t, act.MultistreamTargets, 1)
	require.Equal(t, act.MultistreamTargets[0].URL, "rtmp://localhost/foo/bar")
}

func TestModified(t *testing.T) {
	signer := getSigner()
	var unverified events.UnverifiedEvent
	err := json.Unmarshal(modifiedBody, &unverified)
	require.NoError(t, err)
	_, err = signer.Verify(unverified)
	require.Error(t, err)
}

var validBodyStr = `
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
			"signer": "0x1964035e4C3cD05b8Ff839EFBf37063D8d1Ba7ae",
			"time": 1681403259137
		},
		"signature": "0x85027f5d0f266e0f998164f8c854b8faa507c5acd9e2415276cd8cfd3cac5be246494aebe4b489c0ddf15a5e81c2f5edbd905c64aaca991047109e051d174b101c"
	}
`

var validBody = []byte(validBodyStr)
var modifiedBody = []byte(strings.Replace(validBodyStr, "awesome", "terrible", -1))
