package balancer

//go:generate mockgen -source=./balancer.go -destination=../mocks/balancer/balancer.go

import (
	"context"

	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/log"
)

type Balancer interface {
	Start(ctx context.Context) error
	UpdateMembers(ctx context.Context, members []cluster.Member) error
	GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string, isStudioReq bool) (string, string, error)
	MistUtilLoadSource(ctx context.Context, streamID, lat, lon string) (string, error)
}

// CombinedBalancerEnabled checks if catabalancer is enabled in any way
// enabled - catabalancer fully enabled
// background - only run in background, no results are used
// playback - use catabalancer for playback requests only
// ingest - use catabalancer for ingest requests only
func CombinedBalancerEnabled(cata string) bool {
	return cata == "enabled" || cata == "background" || cata == "playback" || cata == "ingest"
}

func NewCombinedBalancer(cataBalancer, mistBalancer Balancer, catabalancerEnabled string) CombinedBalancer {
	playback := catabalancerEnabled == "enabled" || catabalancerEnabled == "playback"
	ingest := catabalancerEnabled == "enabled" || catabalancerEnabled == "ingest"
	log.LogNoRequestID("catabalancer modes enabled", "playback", playback, "ingest", ingest)
	return CombinedBalancer{
		Catabalancer:                cataBalancer,
		MistBalancer:                mistBalancer,
		CatabalancerPlaybackEnabled: playback,
		CatabalancerIngestEnabled:   ingest,
	}
}

type CombinedBalancer struct {
	Catabalancer                Balancer
	MistBalancer                Balancer
	CatabalancerPlaybackEnabled bool
	CatabalancerIngestEnabled   bool
}

func (c CombinedBalancer) Start(ctx context.Context) error {
	if c.CatabalancerPlaybackEnabled && c.CatabalancerIngestEnabled {
		return c.Catabalancer.Start(ctx)
	}

	if err := c.Catabalancer.Start(ctx); err != nil {
		log.LogNoRequestID("catabalancer Start failed", "err", err)
	}
	return c.MistBalancer.Start(ctx)
}

func (c CombinedBalancer) UpdateMembers(ctx context.Context, members []cluster.Member) error {
	if c.CatabalancerPlaybackEnabled && c.CatabalancerIngestEnabled {
		return c.Catabalancer.UpdateMembers(ctx, members)
	}

	if err := c.Catabalancer.UpdateMembers(ctx, members); err != nil {
		log.LogNoRequestID("catabalancer UpdateMembers failed", "err", err)
	}
	return c.MistBalancer.UpdateMembers(ctx, members)
}

func (c CombinedBalancer) GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string, isStudioReq bool) (string, string, error) {
	cataBestNode, cataFullPlaybackID, cataErr := c.Catabalancer.GetBestNode(ctx, redirectPrefixes, playbackID, lat, lon, fallbackPrefix, isStudioReq)
	if c.CatabalancerPlaybackEnabled {
		return cataBestNode, cataFullPlaybackID, cataErr
	}

	bestNode, fullPlaybackID, err := c.MistBalancer.GetBestNode(ctx, redirectPrefixes, playbackID, lat, lon, fallbackPrefix, isStudioReq)
	log.LogNoRequestID("catabalancer GetBestNode",
		"bestNode", bestNode,
		"fullPlaybackID", fullPlaybackID,
		"cataBestNode", cataBestNode,
		"cataFullPlaybackID", cataFullPlaybackID,
		"cataErr", cataErr,
		"nodeMatch", cataBestNode == bestNode,
		"playbackIDMatch", cataFullPlaybackID == fullPlaybackID,
		"playbackID", playbackID,
		"isStudioReq", isStudioReq,
	)
	return bestNode, fullPlaybackID, err
}

func (c CombinedBalancer) MistUtilLoadSource(ctx context.Context, stream, lat, lon string) (string, error) {
	cataDtscURL, cataErr := c.Catabalancer.MistUtilLoadSource(ctx, stream, lat, lon)
	if c.CatabalancerIngestEnabled {
		return cataDtscURL, cataErr
	}

	dtscURL, err := c.MistBalancer.MistUtilLoadSource(ctx, stream, lat, lon)
	log.LogNoRequestID("catabalancer MistUtilLoadSource",
		"dtscURL", dtscURL,
		"cataDtscURL", cataDtscURL,
		"cataErr", cataErr,
		"urlMatch", dtscURL == cataDtscURL,
		"stream", stream,
	)
	return dtscURL, err
}

type Config struct {
	Args                     []string
	MistUtilLoadPort         uint32
	MistLoadBalancerTemplate string
	NodeName                 string
	MistPort                 int
	MistHost                 string
	OwnRegion                string
	OwnRegionTagAdjust       int

	ReplaceHostMatch   string
	ReplaceHostList    []string
	ReplaceHostPercent int
}
