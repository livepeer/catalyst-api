package cluster

//go:generate mockgen -source=./cluster.go -destination=../mocks/cluster/cluster.go

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"time"

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
	ResolveNodeURL(streamURL string) (string, error)
}

type ClusterImpl struct {
	config   *config.Cli
	serf     *serf.Serf
	eventCh  chan serf.Event
	memberCh chan []Member
}

type Member struct {
	Name string            `json:"name"`
	Tags map[string]string `json:"tags"`
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
	bhost, portstr, err := net.SplitHostPort(c.config.ClusterAddress)
	if err != nil {
		return fmt.Errorf("error splitting bind address %s: %v", c.config.ClusterAddress, err)
	}
	bport, err := strconv.Atoi(portstr)
	if err != nil {
		return fmt.Errorf("error parsing port %s: %v", portstr, err)
	}
	ahost := ""
	aport := 0
	if c.config.ClusterAdvertiseAddress != "" {
		ahost, portstr, err = net.SplitHostPort(c.config.ClusterAdvertiseAddress)
		if err != nil {
			return fmt.Errorf("error splitting bind address %s: %v", c.config.ClusterAddress, err)
		}
		aport, err = strconv.Atoi(portstr)
		if err != nil {
			return fmt.Errorf("error parsing port %s: %v", portstr, err)
		}
	}
	memberlistConfig := memberlist.DefaultWANConfig()
	memberlistConfig.BindAddr = bhost
	memberlistConfig.BindPort = bport
	memberlistConfig.AdvertiseAddr = ahost
	memberlistConfig.AdvertisePort = aport
	memberlistConfig.EnableCompression = true
	memberlistConfig.SecretKey = encryptBytes
	serfConfig := serf.DefaultConfig()
	serfConfig.MemberlistConfig = memberlistConfig
	serfConfig.NodeName = c.config.NodeName
	serfConfig.Tags = c.config.Tags
	serfConfig.EventCh = c.eventCh
	serfConfig.ProtocolVersion = 5

	c.serf, err = serf.Create(serfConfig)
	if err != nil {
		return err
	}

	go c.retryJoin(ctx)

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

func (c *ClusterImpl) retryJoin(ctx context.Context) {
	if len(c.config.RetryJoin) == 0 {
		glog.Infof("No --retry-join provided, starting a single-node cluster")
		return
	}
	backoff := time.Second

	for {
		n, err := c.serf.Join(c.config.RetryJoin, false)
		if n > 0 {
			glog.Infof("Serf successfully joined %d-node cluster", n)
			return
		}
		if err != nil {
			glog.Errorf("Error attempting to join Serf cluster: %v", err)
		}

		jitter := time.Duration(rand.Int63n(int64(backoff)))
		sleepTime := backoff + jitter

		fmt.Printf("Retrying in %v...\n", sleepTime)

		sleepCtx, cancel := context.WithTimeout(ctx, sleepTime)
		defer cancel()

		select {
		case <-ctx.Done():
			return
		case <-sleepCtx.Done():
			continue
		}
	}
}

func (c *ClusterImpl) MembersFiltered(filter map[string]string, status, name string) ([]Member, error) {
	all := c.serf.Members()
	nodes := []Member{}
	for _, member := range all {
		if status != "" && status != member.Status.String() {
			continue
		}
		if name != "" && name != member.Name {
			continue
		}
		matches := true
		for k, v := range filter {
			val, ok := member.Tags[k]
			if !ok || val != v {
				matches = false
				break
			}
		}
		if matches {
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

// Given a dtsc:// or https:// url, resolve the proper address of the node via serf tags
func (c *ClusterImpl) ResolveNodeURL(streamURL string) (string, error) {
	return ResolveNodeURL(c, streamURL)
}

// Separated here to be more easily fed mocks for testing
func ResolveNodeURL(c Cluster, streamURL string) (string, error) {
	u, err := url.Parse(streamURL)
	if err != nil {
		return "", err
	}
	nodeName := u.Host
	protocol := u.Scheme

	member, err := c.Member(map[string]string{}, "alive", nodeName)
	if err != nil {
		return "", err
	}
	addr, has := member.Tags[protocol]
	if !has {
		glog.V(7).Infof("no tag found, not tag resolving protocol=%s nodeName=%s", protocol, nodeName)
		return streamURL, nil
	}
	u2, err := url.Parse(addr)
	if err != nil {
		err = fmt.Errorf("node has unparsable tag!! nodeName=%s protocol=%s tag=%s", nodeName, protocol, addr)
		glog.Error(err)
		return "", err
	}
	u2.Path = u.Path
	u2.RawQuery = u.RawQuery
	return u2.String(), nil
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
		select {
		case event := <-inbox:
			glog.V(3).Infof("got event: %v", event)
		case <-ctx.Done():
			return nil
		}

		members, err := c.MembersFiltered(mediaFilter, ".*", ".*")

		if err != nil {
			glog.Errorf("Error getting serf, crashing: %v\n", err)
			return err
		}

		c.memberCh <- members
	}
}
