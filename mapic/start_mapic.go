package mistapiconnector

import (
	"errors"
	"net"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/config"
	mistapi "github.com/livepeer/catalyst-api/mapic/apis/mist"
	"github.com/livepeer/catalyst-api/mapic/metrics"
	"github.com/livepeer/catalyst-api/mapic/model"
	"github.com/livepeer/go-api-client"
)

func StartMapic(cli *config.Cli) IMac {
	lapi, _ := api.NewAPIClientGeolocated(api.ClientOptions{
		Server:      cli.APIServer,
		AccessToken: cli.APIToken,
	})

	mapi := mistapi.NewMist(cli.MistHost, cli.MistUser, cli.MistPassword, cli.APIToken, uint(cli.MistPort))
	ensureLoggedIn(mapi, cli.MistConnectTimeout)
	metrics.InitCensus(cli.Node, model.Version, "mistconnector")

	opts := MacOptions{
		NodeID:                    cli.Node,
		MistHost:                  cli.MistHost,
		MistAPI:                   mapi,
		LivepeerAPI:               lapi,
		BaseStreamName:            cli.MistBaseStreamName,
		CheckBandwidth:            false,
		SendAudio:                 cli.MistSendAudio,
		AMQPUrl:                   cli.AMQPURL,
		OwnRegion:                 cli.OwnRegion,
		MistStreamSource:          cli.MistStreamSource,
		MistHardcodedBroadcasters: cli.MistHardcodedBroadcasters,
		NoMistScrapeMetrics:       !cli.MistScrapeMetrics,
	}
	mc, err := NewMac(opts)
	if err != nil {
		glog.Fatalf("Error creating mist-api-connector %v", err)
	}
	if err := mc.SetupTriggers(cli.OwnUri); err != nil {
		glog.Fatal(err)
	}
	return mc
}

func ensureLoggedIn(mapi *mistapi.API, timeout time.Duration) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		err := mapi.Login()
		if err == nil {
			return
		}

		var netErr net.Error
		if !errors.As(err, &netErr) {
			glog.Fatalf("Fatal non-network error logging to mist. err=%q", err)
		}
		select {
		case <-deadline.C:
			glog.Fatalf("Failed to login to mist after %s. err=%q", timeout, netErr)
		case <-time.After(1 * time.Second):
			glog.Errorf("Retrying after network error logging to mist. err=%q", netErr)
		}
	}
}
