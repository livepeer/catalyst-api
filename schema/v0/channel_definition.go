package v0

import (
	"math/big"

	"github.com/livepeer/catalyst-api/events"
)

type ChannelDefinition struct {
	events.ActionBase
	ID                 string              `json:"id"`
	Time               big.Int             `json:"time"`
	MultistreamTargets []MultistreamTarget `json:"multistreamTargets"`
}

type MultistreamTarget struct {
	URL string
}
