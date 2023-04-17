package v0

import (
	"math/big"

	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/livepeer/catalyst-api/events"
)

// TODO: Not sure what we need here yet exactly... salt good? Definitely don't want the chainID for chain-agnostic reasons
var Domain = events.Domain{
	Name:    "Livepeer Decentralized Video Protocol",
	Version: "0.0.1",
	Salt:    "f8b3858ac49ca50b138587d5dace09bd102b9d24d2567d9a5cde2f6122810931",
}

var Types = apitypes.Types{
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
	"ChannelDefinition": {
		{
			Name: "id",
			Type: "string",
		},
		{
			Name: "time",
			Type: "int64",
		},
		{
			Name: "multistreamTargets",
			Type: "MultistreamTarget[]",
		},
	},
	"MultistreamTarget": {
		{
			Name: "url",
			Type: "string",
		},
	},
}

type ChannelDefinition struct {
	events.ActionBase
	ID                 string              `json:"id"`
	Time               big.Int             `json:"time"`
	MultistreamTargets []MultistreamTarget `json:"multistreamTargets"`
}

type MultistreamTarget struct {
	URL string
}
