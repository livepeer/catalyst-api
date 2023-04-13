package events

import (
	"encoding/json"
	"fmt"
	"math/big"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/livepeer/go-livepeer/eth"
)

var typesStandard = apitypes.Types{
	"EIP712Domain": {
		{
			Name: "name",
			Type: "string",
		},
		{
			Name: "version",
			Type: "string",
		},
		{
			Name: "salt",
			Type: "string",
		},
	},
	"Person": {
		{
			Name: "name",
			Type: "string",
		},
		{
			Name: "wallet",
			Type: "address",
		},
	},
	"Mail": {
		{
			Name: "from",
			Type: "Person",
		},
		{
			Name: "to",
			Type: "Person",
		},
		{
			Name: "contents",
			Type: "string",
		},
	},
}

const primaryType = "Mail"

var domainStandard = apitypes.TypedDataDomain{
	Name:    "Livepeer Decentralized Video Protocol",
	Version: "1",
	Salt:    "f8b3858ac49ca50b138587d5dace09bd102b9d24d2567d9a5cde2f6122810931",
}

var messageStandard = map[string]interface{}{
	"from": map[string]interface{}{
		"name":   "Cow",
		"wallet": "0xCD2a3d9F938E13CD947Ec05AbC7FE734Df8DD826",
	},
	"to": map[string]interface{}{
		"name":   "Bob",
		"wallet": "0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB",
	},
	"contents": "Hello, Bob!",
}

var typedData = apitypes.TypedData{
	Types:       typesStandard,
	PrimaryType: primaryType,
	Domain:      domainStandard,
	Message:     messageStandard,
}

type fullMessage struct {
	Data      apitypes.TypedData `json:"data"`
	Signature string             `json:"sig"`
}

func Sign() string {
	id := new(big.Int).SetInt64(int64(80001))
	am, err := eth.NewAccountManager(ethcommon.HexToAddress(""), ".", id, "secretpassword")
	if err != nil {
		panic(err)
	}
	err = am.Unlock("secretpassword")
	if err != nil {
		panic(err)
	}
	b, err := am.SignTypedData(typedData)
	if err != nil {
		panic(err)
	}
	message := fullMessage{
		Data:      typedData,
		Signature: fmt.Sprintf("%s", hexutil.Bytes(b)),
	}
	b2, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	return string(b2)
}
