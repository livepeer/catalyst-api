package events

import (
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

// Defines interfaces for schema
type Schema struct {
	Domain  Domain
	Types   apitypes.Types
	Actions map[string]func() Action
}
