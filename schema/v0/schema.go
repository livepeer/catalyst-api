package v0

import (
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
			Name: "signer",
			Type: "address",
		},
		{
			Name: "time",
			Type: "int64",
		},
		{
			Name: "id",
			Type: "string",
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

var Schema = events.Schema{
	Types:  Types,
	Domain: Domain,
	Actions: map[string]func() events.Action{
		"ChannelDefinition": func() events.Action {
			return &ChannelDefinition{}
		},
	},
}
