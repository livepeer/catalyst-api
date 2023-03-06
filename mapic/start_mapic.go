package mistapiconnector

import (
	"github.com/livepeer/catalyst-api/config"
)

func NewMapic(cli *config.Cli) IMac {
	mc := &mac{
		config:                    cli,
		nodeID:                    cli.NodeName,
		mistHot:                   cli.MistHost,
		checkBandwidth:            false,
		streamInfo:                make(map[string]*streamInfo),
		baseStreamName:            cli.MistBaseStreamName,
		ownRegion:                 cli.OwnRegion,
		mistStreamSource:          cli.MistStreamSource,
		mistHardcodedBroadcasters: cli.MistHardcodedBroadcasters,
	}
	return mc
}
