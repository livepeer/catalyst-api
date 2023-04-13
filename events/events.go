package events

import (
	"fmt"
	"math/big"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/livepeer/go-livepeer/eth"
)

// using apitypes.TypedDataDomain directly causes us to get "chainId": null
type Domain struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Salt    string `json:"salt"`
}

func (t *Domain) TypedDataDomain() apitypes.TypedDataDomain {
	return apitypes.TypedDataDomain{
		Name:    t.Name,
		Version: t.Version,
		Salt:    t.Salt,
	}
}

type Event struct {
	PrimaryType string         `json:"primaryType"`
	Domain      Domain         `json:"domain"`
	Message     map[string]any `json:"message"`
}

type SignedEvent struct {
	Signature string `json:"sig"`
	Event     Event  `json:"event"`
}

type Signer struct {
	Types apitypes.Types
}

func (s *Signer) Sign(event Event) SignedEvent {
	id := new(big.Int).SetInt64(int64(80001))
	am, err := eth.NewAccountManager(ethcommon.HexToAddress(""), ".", id, "secretpassword")
	if err != nil {
		panic(err)
	}
	err = am.Unlock("secretpassword")
	if err != nil {
		panic(err)
	}
	typedData := apitypes.TypedData{
		Types:       s.Types,
		PrimaryType: event.PrimaryType,
		Domain:      event.Domain.TypedDataDomain(),
		Message:     event.Message,
	}
	b, err := am.SignTypedData(typedData)
	if err != nil {
		panic(fmt.Errorf("error signing typed data: %w", err))
	}
	// golint wants string(b) but that gives /x1234 encoded output
	sig := fmt.Sprintf("%s", hexutil.Bytes(b)) //nolint:gosimple
	return SignedEvent{
		Event:     event,
		Signature: sig,
	}
}
