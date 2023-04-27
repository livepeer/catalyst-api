package domain

import "github.com/ethereum/go-ethereum/signer/core/apitypes"

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
}
