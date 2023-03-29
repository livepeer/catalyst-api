package mistapiconnector

import (
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/mapic/metrics"
	"github.com/livepeer/catalyst-api/mapic/model"
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
	metrics.InitCensus(mc.config.NodeName, model.Version, "mistconnector")
	return mc
}