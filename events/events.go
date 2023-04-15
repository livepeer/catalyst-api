package events

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
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
	PrimaryType string `json:"primaryType"`
	Domain      Domain `json:"domain"`
	Message     Action `json:"message"`
}

func (e *Event) TypedData(types apitypes.Types) apitypes.TypedData {
	return apitypes.TypedData{
		Types:       types,
		PrimaryType: e.PrimaryType,
		Domain:      e.Domain.TypedDataDomain(),
		Message:     e.Message.Map(),
	}
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
	typedData := event.TypedData(s.Types)
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

func (s *Signer) Verify(signedEvent SignedEvent) (common.Address, error) {
	sig, err := hexutil.Decode(signedEvent.Signature)
	sig[64] -= 27
	if err != nil {
		return common.Address{}, err
	}
	typedData := signedEvent.Event.TypedData(s.Types)
	hash, _, err := apitypes.TypedDataAndHash(typedData)
	if err != nil {
		return common.Address{}, err
	}
	rpk, err := crypto.SigToPub(hash, sig)
	if err != nil {
		return common.Address{}, err
	}
	return crypto.PubkeyToAddress(*rpk), nil
}

type Action interface {
	Map() map[string]any
}

// Base action suitable for inheriting by every other action
type ActionBase struct{}

// Returns a map version of this event suitable for
func (a ActionBase) Map() map[string]any {
	// lol very hacky implementation obviously
	data, err := json.Marshal(a)

	if err != nil {
		panic(err)
	}

	newMap := map[string]any{}
	err = json.Unmarshal(data, &newMap)
	if err != nil {
		panic(err)
	}
	return newMap
}
