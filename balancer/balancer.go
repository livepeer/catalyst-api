package balancer

//go:generate mockgen -source=./balancer.go -destination=../mocks/balancer/balancer.go

import (
	"context"
	"time"

	"github.com/livepeer/catalyst-api/cluster"
)

var mistUtilLoadSingleRequestTimeout = 15 * time.Second
var mistUtilLoadLoopTimeout = 2 * time.Minute

type Balancer interface {
	Start(ctx context.Context) error
	UpdateMembers(ctx context.Context, members []cluster.Member) error
	GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string) (string, string, error)
	MistUtilLoadBalance(ctx context.Context, stream, lat, lon string) (string, error)
	MistUtilLoadSource(ctx context.Context, stream, lat, lon string) (string, error)
	MistUtilLoadStreamStats(ctx context.Context, stream string) error
}

type Config struct {
	Args                     []string
	MistUtilLoadPort         uint32
	MistLoadBalancerTemplate string
	NodeName                 string
	MistPort                 int
	MistHost                 string
}
