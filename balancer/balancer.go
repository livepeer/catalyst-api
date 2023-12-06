package balancer

//go:generate mockgen -source=./balancer.go -destination=../mocks/balancer/balancer.go

import (
	"context"

	"github.com/livepeer/catalyst-api/balancer/catabalancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/log"
)

type Balancer interface {
	Start(ctx context.Context) error
	UpdateMembers(ctx context.Context, members []cluster.Member) error
	GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string) (string, string, error)
	MistUtilLoadSource(ctx context.Context, stream, lat, lon string) (string, error)
	UpdateNodes(id string, nodeMetrics catabalancer.NodeMetrics)
	UpdateStreams(id string, stream string, isIngest bool)
}

type CombinedBalancer struct {
	Catabalancer        Balancer
	MistBalancer        Balancer
	CatabalancerEnabled bool
}

func (c CombinedBalancer) Start(ctx context.Context) error {
	if c.CatabalancerEnabled {
		return c.Catabalancer.Start(ctx)
	}

	if err := c.Catabalancer.Start(ctx); err != nil {
		log.LogNoRequestID("catabalancer Start failed", "err", err)
	}
	return c.MistBalancer.Start(ctx)
}

func (c CombinedBalancer) UpdateMembers(ctx context.Context, members []cluster.Member) error {
	if c.CatabalancerEnabled {
		return c.Catabalancer.UpdateMembers(ctx, members)
	}

	if err := c.Catabalancer.UpdateMembers(ctx, members); err != nil {
		log.LogNoRequestID("catabalancer UpdateMembers failed", "err", err)
	}
	return c.MistBalancer.UpdateMembers(ctx, members)
}

func (c CombinedBalancer) GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string) (string, string, error) {
	cataBestNode, cataFullPlaybackID, cataErr := c.Catabalancer.GetBestNode(ctx, redirectPrefixes, playbackID, lat, lon, fallbackPrefix)
	if c.CatabalancerEnabled {
		return cataBestNode, cataFullPlaybackID, cataErr
	}

	bestNode, fullPlaybackID, err := c.MistBalancer.GetBestNode(ctx, redirectPrefixes, playbackID, lat, lon, fallbackPrefix)
	log.LogNoRequestID("catabalancer GetBestNode",
		"bestNode", bestNode,
		"fullPlaybackID", fullPlaybackID,
		"cataBestNode", cataBestNode,
		"cataFullPlaybackID", cataFullPlaybackID,
		"cataErr", cataErr,
		"nodeMatch", cataBestNode == bestNode,
		"playbackIDMatch", cataFullPlaybackID == fullPlaybackID,
	)
	return bestNode, fullPlaybackID, err
}

func (c CombinedBalancer) MistUtilLoadSource(ctx context.Context, stream, lat, lon string) (string, error) {
	cataDtscURL, cataErr := c.Catabalancer.MistUtilLoadSource(ctx, stream, lat, lon)
	if c.CatabalancerEnabled {
		return cataDtscURL, cataErr
	}

	dtscURL, err := c.MistBalancer.MistUtilLoadSource(ctx, stream, lat, lon)
	log.LogNoRequestID("catabalancer MistUtilLoadSource",
		"dtscURL", dtscURL,
		"cataDtscURL", cataDtscURL,
		"cataErr", cataErr,
		"urlMatch", dtscURL == cataDtscURL,
	)
	return dtscURL, err
}

func (c CombinedBalancer) UpdateNodes(id string, nodeMetrics catabalancer.NodeMetrics) {
	c.Catabalancer.UpdateNodes(id, nodeMetrics)
	c.MistBalancer.UpdateNodes(id, nodeMetrics)
}

func (c CombinedBalancer) UpdateStreams(id string, stream string, isIngest bool) {
	c.Catabalancer.UpdateStreams(id, stream, false)
	c.MistBalancer.UpdateStreams(id, stream, false)
}

type Config struct {
	Args                     []string
	MistUtilLoadPort         uint32
	MistLoadBalancerTemplate string
	NodeName                 string
	MistPort                 int
	MistHost                 string
}
