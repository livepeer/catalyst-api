package events

import (
	"bytes"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/livepeer/catalyst-api/schema/domain"
)

// an event, with an address, for which we have verified the signature.
type SignedEvent struct {
	Domain    Domain
	Action    Action
	Signature string
	Address   ethcommon.Address
}

// convert to UnverifiedEvent suitable for JSON serialization
func (s *SignedEvent) UnverifiedEvent() UnverifiedEvent {
	return UnverifiedEvent{
		PrimaryType: s.Action.Type(),
		Domain:      s.Domain,
		Message:     ActionToMap(s.Action),
		Signature:   s.Signature,
	}
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
}

// convert to a TypedDataDomain suitable for signing by eth tooling
func (d *Domain) TypedDataDomain() apitypes.TypedDataDomain {
	return apitypes.TypedDataDomain{
		Name:    d.Name,
		Version: d.Version,
	}
}

// produce a hash of this typed data domain, suitable for comparing with another
func (d *Domain) Hash() ([]byte, error) {
	typedDataDomain := d.TypedDataDomain()
	typedData := apitypes.TypedData{
		Types:       domain.Types,
		PrimaryType: "EIP712Domain",
		Domain:      typedDataDomain,
	}
	domainSeparator, err := typedData.HashStruct("EIP712Domain", typedData.Domain.Map())
	if err != nil {
		return []byte{}, err
	}
	return domainSeparator, nil
}

func (d *Domain) Equal(d2 *Domain) (bool, error) {
	hash1, err := d.Hash()
	if err != nil {
		return false, err
	}
	hash2, err := d.Hash()
	if err != nil {
		return false, err
	}
	return bytes.Equal(hash1, hash2), nil
}
