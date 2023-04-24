package events

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/livepeer/go-livepeer/eth"
)

// schema-aware signer for signing actions and verifying untrusted payloads
type Signer struct {
	Types apitypes.Types
}

func (s *Signer) Sign(unverified UnverifiedEvent) SignedEvent {
	id := new(big.Int).SetInt64(int64(80001))
	am, err := eth.NewAccountManager(ethcommon.HexToAddress(""), ".", id, "secretpassword")
	if err != nil {
		panic(err)
	}
	err = am.Unlock("secretpassword")
	if err != nil {
		panic(err)
	}
	typedData := unverified.TypedData(s.Types)
	b, err := am.SignTypedData(typedData)
	if err != nil {
		panic(fmt.Errorf("error signing typed data: %w", err))
	}
	// golint wants string(b) but that gives /x1234 encoded output
	sig := fmt.Sprintf("%s", hexutil.Bytes(b)) //nolint:gosimple
	return SignedEvent{
		PrimaryType: unverified.PrimaryType,
		Domain:      unverified.Domain,
		Signature:   sig,
		Address:     am.Account().Address,
	}
}

func (s *Signer) Verify(unverified UnverifiedEvent) (SignedEvent, error) {
	sig, err := hexutil.Decode(unverified.Signature)
	sig[64] -= 27
	if err != nil {
		return SignedEvent{}, err
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
