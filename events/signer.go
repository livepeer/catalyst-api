package events

import (
	"fmt"
	"math/big"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/livepeer/go-livepeer/eth"
)

// schema-aware signer for signing actions and verifying untrusted payloads
type Signer interface {
	Sign(action Action) (*SignedEvent, error)
	Verify(unverified UnverifiedEvent) (*SignedEvent, error)
}

// Signer implemented with EIP712
type EIP712Signer struct {
	// When I sign an action, which schema should I use?
	PrimarySchema *Schema
	// All supported schemas for verification purposes
	Schemas []*Schema
	// Eth Account Manager
	AccountManager eth.AccountManager
}

type EIP712SignerOptions struct {
	PrimarySchema       *Schema
	Schemas             []*Schema
	EthKeystorePassword string
	EthKeystorePath     string
	EthAccountAddr      string
}

func NewEIP712Signer(opts *EIP712SignerOptions) (Signer, error) {
	// We don't use this parameter so let's use one that doesn't exist
	id := new(big.Int).SetInt64(int64(9999999999))
	am, err := eth.NewAccountManager(ethcommon.HexToAddress(opts.EthAccountAddr), opts.EthKeystorePath, id, opts.EthKeystorePassword)
	if err != nil {
		return nil, fmt.Errorf("error initalizing eth.AccountManager: %w", err)
	}
	err = am.Unlock(opts.EthKeystorePassword)
	if err != nil {
		return nil, fmt.Errorf("error unlcoking eth.AccountManager: %w", err)
	}
	return &EIP712Signer{
		PrimarySchema:  opts.PrimarySchema,
		Schemas:        opts.Schemas,
		AccountManager: am,
	}, nil
}

func (s *EIP712Signer) Sign(action Action) (*SignedEvent, error) {
	actionMap, err := ActionToMap(action)
	if err != nil {
		return nil, err
	}
	addrStr := fmt.Sprintf("%s", s.AccountManager.Account().Address)
	if actionMap["signer"] != addrStr {
		return nil, fmt.Errorf("address mismatch signing action, signer.address=%s, action.singer=%s", addrStr, actionMap["signer"])
	}
	actionMap["signer"] = addrStr
	typedData := apitypes.TypedData{
		Types:       s.PrimarySchema.Types,
		Domain:      s.PrimarySchema.Domain.TypedDataDomain(),
		PrimaryType: action.Type(),
		Message:     actionMap,
	}
	_, err = typedData.HashStruct("EIP712Domain", typedData.Domain.Map())
	if err != nil {
		return nil, fmt.Errorf("error signing EIP712Domain: %w", err)
	}
	_, err = typedData.HashStruct(typedData.PrimaryType, typedData.Message)
	if err != nil {
		return nil, fmt.Errorf("error signing struct: %w", err)
	}

	b, err := s.AccountManager.SignTypedData(typedData)
	if err != nil {
		return nil, fmt.Errorf("error signing typed data: %w", err)
	}
	// golint wants string(b) but that gives /x1234 encoded output
	sig := fmt.Sprintf("%s", hexutil.Bytes(b)) //nolint:gosimple
	return &SignedEvent{
		Domain:    s.PrimarySchema.Domain,
		Signature: sig,
		Address:   s.AccountManager.Account().Address,
		Action:    action,
	}, nil
}

// given an unverified event from an untrusted source, verify its signature
func (s *EIP712Signer) Verify(unverified UnverifiedEvent) (*SignedEvent, error) {
	// find the correct schema for this action
	var schema *Schema
	for _, s := range s.Schemas {
		eq, err := s.Domain.Equal(&unverified.Domain)
		if eq && err == nil {
			schema = s
			break
		}
	}
	if schema == nil {
		return nil, fmt.Errorf("unknown event domain: %s", unverified.Domain)
	}
	sig, err := hexutil.Decode(unverified.Signature)
	sig[64] -= 27
	if err != nil {
		return nil, err
	}
	typedData := apitypes.TypedData{
		Types:       schema.Types,
		Domain:      schema.Domain.TypedDataDomain(),
		PrimaryType: unverified.PrimaryType,
		Message:     unverified.Message,
	}
	hash, _, err := apitypes.TypedDataAndHash(typedData)
	if err != nil {
		return nil, err
	}
	rpk, err := crypto.SigToPub(hash, sig)
	if err != nil {
		return nil, err
	}
	addr := crypto.PubkeyToAddress(*rpk)
	actionGenerator, ok := schema.Actions[unverified.PrimaryType]
	if !ok {
		return nil, fmt.Errorf("unknown action domain: %s", unverified.Domain)
	}
	action := actionGenerator()
	err = LoadMap(action, unverified.Message)
	if err != nil {
		return nil, err
	}
	addrString := fmt.Sprintf("%s", addr)
	if addrString != action.SignerAddress() {
		return nil, fmt.Errorf("incorrect signer for action! signer=%s action.signer=%s", addrString, action.SignerAddress())
	}
	return &SignedEvent{
		Domain:    schema.Domain,
		Signature: unverified.Signature,
		Address:   addr,
		Action:    action,
	}, nil
}
