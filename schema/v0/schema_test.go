package v0

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/livepeer/catalyst-api/events"
	"github.com/stretchr/testify/require"
)

func getSigner(t *testing.T) events.Signer {
	schemas := []*events.Schema{&Schema}
	signer, err := events.NewEIP712Signer(&events.EIP712SignerOptions{
		Schemas:             schemas,
		PrimarySchema:       &Schema,
		EthKeystorePassword: "secretpassword",
		EthKeystorePath:     ".",
	})
	require.NoError(t, err)
	return signer
}

func TestSign(t *testing.T) {
	signer := getSigner(t)
	var testMessage = &ChannelDefinition{
		ID:     "my-awesome-stream",
		Signer: "0x1964035e4C3cD05b8Ff839EFBf37063D8d1Ba7ae",
		Time:   int64(1681403259137),
		MultistreamTargets: []MultistreamTarget{{
			URL: "rtmp://localhost/foo/bar",
		}},
	}
	signedEvent, err := signer.Sign(testMessage)
	require.NoError(t, err)
	unverified, err := signedEvent.UnverifiedEvent()
	require.NoError(t, err)
	_, err = json.Marshal(unverified)
	require.NoError(t, err)
}

func TestVerify(t *testing.T) {
	signer := getSigner(t)
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
	signer := getSigner(t)
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
			"version": "0.0.1"
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
		"signature": "0x34ed2b69881f79f153c0a4e6e3313e58b642be227cd91cc2ef1e7e8d04d3c89a272a3cea8da87b0b3b52c91b484d6f6d36ed9921bda89755ff60d1918d6268861c"
	}
`

var validBody = []byte(validBodyStr)
var modifiedBody = []byte(strings.Replace(validBodyStr, "awesome", "terrible", -1))
