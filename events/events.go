package events

import (
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

// an event, with an address, for which we have verified the signature.
type SignedEvent struct {
	PrimaryType string
	Domain      Domain
	Action      Action
	Signature   string
	Address     ethcommon.Address
}

// an unverified event. Don't trust it without first producing a SignedEvent with .Verify()
type UnverifiedEvent struct {
	PrimaryType string         `json:"primaryType"`
	Domain      Domain         `json:"domain"`
	Message     map[string]any `json:"message"`
	Signature   string         `json:"signature"`
}

// convert to apitypes.TypedData, suitable for signing
func (e *UnverifiedEvent) TypedData(types apitypes.Types) apitypes.TypedData {
	return apitypes.TypedData{
		Types:       types,
		PrimaryType: e.PrimaryType,
		Domain:      e.Domain.TypedDataDomain(),
		Message:     e.Message,
	}
}

// using apitypes.TypedDataDomain directly causes us to get "chainId": null in the JSON
type Domain struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Salt    string `json:"salt"`
}

// convert to a TypedDataDomain suitable for signing by eth tooling
func (t *Domain) TypedDataDomain() apitypes.TypedDataDomain {
	return apitypes.TypedDataDomain{
		Name:    t.Name,
		Version: t.Version,
		Salt:    t.Salt,
	}
}
