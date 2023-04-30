package v0

import (
	"github.com/livepeer/catalyst-api/events"
)

type ChannelDefinition struct {
	events.ActionBase
	ID                 string              `json:"id"`
	Signer             string              `json:"signer"`
	Time               int64               `json:"time"`
	MultistreamTargets []MultistreamTarget `json:"multistreamTargets"`
}

func (c ChannelDefinition) Type() string {
	return "ChannelDefinition"
}

func (c ChannelDefinition) SignerAddress() string {
	return c.Signer
}

type MultistreamTarget struct {
	URL string `json:"url"`
}
