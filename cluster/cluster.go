package cluster

//go:generate mockgen -source=./cluster.go -destination=./mocks/cluster.go

import (
	"context"
	"fmt"

	"github.com/golang/glog"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"github.com/livepeer/catalyst-api/config"
)

type Cluster interface {
	Start(ctx context.Context) error
	MembersFiltered(filter map[string]string, status, name string) ([]Member, error)
	Member(filter map[string]string, status, name string) (Member, error)
	MemberChan() chan []Member
}

type ClusterImpl struct {
	config   *config.Cli
	serf     *serf.Serf
	eventCh  chan serf.Event
	memberCh chan []Member
}

type Member struct {
	Name string
	Tags map[string]string
}

var mediaFilter = map[string]string{"node": "media"}

// Create a connection to a new Cluster that will immediately connect
func NewCluster(config *config.Cli) Cluster {
	c := ClusterImpl{
		config:   config,
		eventCh:  make(chan serf.Event, 64),
		memberCh: make(chan []Member),
	}
	return &c
}

// Start the connection to this cluster
func (c *ClusterImpl) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	encryptBytes, err := c.config.EncryptBytes()
	if err != nil {
		return fmt.Errorf("error decoding encryption key: %w", err)
	}
	memberlistConfig := memberlist.DefaultWANConfig()
	memberlistConfig.BindAddr = c.config.ClusterAddress
	memberlistConfig.AdvertiseAddr = c.config.ClusterAdvertiseAddress
	memberlistConfig.EnableCompression = true
	memberlistConfig.SecretKey = encryptBytes
	serfConfig := serf.DefaultConfig()
	serfConfig.MemberlistConfig = memberlistConfig
	serfConfig.NodeName = c.config.NodeName
	serfConfig.Tags = c.config.Tags
	serfConfig.EventCh = c.eventCh
	serfConfig.ProtocolVersion = 5

	if err != nil {
		return err
	}

	go func() {
		err = c.handleEvents(ctx)
		cancel()
	}()

	<-ctx.Done()

	glog.Infof("Leaving Serf")
	err = c.serf.Leave()
	if err != nil {
		glog.Infof("Error leaving Serf cluster: %s", err)
	}
	err = c.serf.Shutdown()
	if err != nil {
		glog.Infof("Error shutting down Serf cluster: %s", err)
	}

	return err
}

func (c *ClusterImpl) MembersFiltered(filter map[string]string, status, name string) ([]Member, error) {
	all := c.serf.Members()
	nodes := []Member{}
	for _, member := range all {
		for k, v := range filter {
			val, ok := member.Tags[k]
			if !ok || val != v {
				continue
			}
			nodes = append(nodes, Member{
				Name: member.Name,
				Tags: member.Tags,
			})
		}
	}
	return nodes, nil
}

func (c *ClusterImpl) Member(filter map[string]string, status, name string) (Member, error) {
	members, err := c.MembersFiltered(filter, status, name)
	if err != nil {
		return Member{}, err
	}
	if len(members) < 1 {
		return Member{}, fmt.Errorf("could not find serf member name=%s", name)
	}
	if len(members) > 1 {
		glog.Errorf("found multiple serf members with the same name! this shouldn't happen! name=%s count=%d", name, len(members))
	}
	return members[0], nil
}

// Subscribe to changes in the member list. Please only call me once. I only have one channel internally.
func (c *ClusterImpl) MemberChan() chan []Member {
	return c.memberCh
}

func (c *ClusterImpl) handleEvents(ctx context.Context) error {
	inbox := make(chan serf.Event, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-c.eventCh:
				select {
				case <-ctx.Done():
					return
				case inbox <- e:
					// Event is now in the inbox
				default:
					// Overflow event gets dropped
				}
			}
		}
	}()

	for {
		event := <-inbox
		glog.V(5).Infof("got event: %v", event)

		members, err := c.MembersFiltered(mediaFilter, ".*", ".*")

		if err != nil {
			glog.Errorf("Error getting serf, crashing: %v\n", err)
			break
		}

		c.memberCh <- members
		continue
	}

	return nil
}
