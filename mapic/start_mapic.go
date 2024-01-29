package mistapiconnector

import (
	"fmt"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/mapic/metrics"
	"github.com/livepeer/catalyst-api/mapic/model"
	"regexp"
)

func NewMapic(cli *config.Cli, broker misttriggers.TriggerBroker, mist clients.MistAPIClient) IMac {
	streamMetricsRe := regexp.MustCompile(fmt.Sprintf(`stream="%s\+(.*?)"`, cli.MistBaseStreamName))
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
		broker:                    broker,
		mist:                      mist,
		streamMetricsRe:           streamMetricsRe,
	}
	metrics.InitCensus(mc.config.NodeName, model.Version, "mistconnector")
	return mc
}
