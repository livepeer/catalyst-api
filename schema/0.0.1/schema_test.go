package schema

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/livepeer/catalyst-api/events"
)

// const primaryType = "Mail"
//
// var domainStandard = apitypes.TypedDataDomain{
//  Name:    "Livepeer Decentralized Video Protocol",
//  Version: "1",
//  Salt:    "f8b3858ac49ca50b138587d5dace09bd102b9d24d2567d9a5cde2f6122810931",
// }
//
// var messageStandard = map[string]interface{}{
//  "from": map[string]interface{}{
//  "name":   "Cow",
//  "wallet": "0xCD2a3d9F938E13CD947Ec05AbC7FE734Df8DD826",
//  },
//  "to": map[string]interface{}{
//  "name":   "Bob",
//  "wallet": "0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB",
//  },
//  "contents": "Hello, Bob!",
// }
//
// var typedData = apitypes.TypedData{
//  Types:       typesStandard,
//  PrimaryType: primaryType,
//  Domain:      domainStandard,
//  Message:     messageStandard,
// }

func TestEventParseVerify(t *testing.T) {
	var messageStandard = map[string]interface{}{
		"time": big.NewInt(1681403259137),
		"data": map[string]any{
			"id": "my-awesome-stream",
			"targets": []map[string]any{
				{
					"url": "rtmp://localhost/foo/bar",
				},
			},
		},
	}
	var typedData = apitypes.TypedData{
		Types:       Types,
		PrimaryType: "EventChannelDefinitionMeta",
		Domain:      Domain,
		Message:     messageStandard,
	}
	out := events.Sign(typedData)
	panic(out)
}
