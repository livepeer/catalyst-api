package v0

import (
	"github.com/livepeer/catalyst-api/events"
)

type ChannelDefinition struct {
	events.ActionBase
	ID                 string              `json:"id"`
	Time               int64               `json:"time"`
	MultistreamTargets []MultistreamTarget `json:"multistreamTargets"`
}

func (c ChannelDefinition) Type() string {
	return "ChannelDefinition"
}

type MultistreamTarget struct {
	URL string `json:"url"`
}
