package balancer

//go:generate mockgen -source=./balancer.go -destination=../mocks/balancer/balancer.go

import (
	"context"
	"fmt"
	"github.com/livepeer/catalyst-api/balancer/catabalancer"

	"github.com/livepeer/catalyst-api/cluster"
)

type BalancerStub struct {
	config *Config
}

// create a new load balancer instance
func NewBalancerStub(config *Config) Balancer {
	return &BalancerStub{config: config}
}

// start this load balancer instance, execing MistUtilLoad if necessary
func (b *BalancerStub) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (b *BalancerStub) UpdateMembers(ctx context.Context, members []cluster.Member) error {
	return nil
}

// always returns local node
func (b *BalancerStub) GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string, isStudioReq bool) (string, string, error) {
	return "localhost", playbackID, nil
}

func (b *BalancerStub) QueryMistForClosestNodeSource(ctx context.Context, playbackID, lat, lon, prefix string, source bool) (string, error) {
	return "dtsc://localhost", nil
}

func (b *BalancerStub) MistUtilLoadBalance(ctx context.Context, stream, lat, lon string, isStudioReq bool) (string, error) {
	return "127.0.0.1", nil
}

func (b *BalancerStub) MistUtilLoadSource(ctx context.Context, stream, lat, lon string) (string, error) {
	return "dtsc://127.0.0.1", nil
}

func (b *BalancerStub) MistUtilLoadStreamStats(ctx context.Context, stream string) error {
	return fmt.Errorf("not implemented")
}

func (b *BalancerStub) UpdateNodes(id string, nodeMetrics catabalancer.NodeMetrics) {
}

func (b *BalancerStub) UpdateStreams(id string, stream string, isIngest bool) {
}
