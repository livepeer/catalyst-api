package events

import (
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

// Type to be utilized by all supported schemas
type Schema struct {
	Domain  Domain
	Types   apitypes.Types
	Actions map[string]func() Action
}
