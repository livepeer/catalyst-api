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
		"signature": "0xef0d818467991664fa638642aee445e3e054c813a5d58ed33bf1ca6b65af76fa74231dafa740470a7c1a5953598cb6d9b3a6bad840ec19e53c04072024b88dcd1b"
	}
`

var validBody = []byte(validBodyStr)
var modifiedBody = []byte(strings.Replace(validBodyStr, "awesome", "terrible", -1))
