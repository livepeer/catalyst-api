package cluster

//go:generate mockgen -source=./cluster.go -destination=../mocks/cluster/cluster.go

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/metrics"
)

const serfClusterInternalEventBuffer = 100000

type Cluster interface {
	Start(ctx context.Context) error
	MembersFiltered(filter map[string]string, status, name string) []Member
	MemberChan() chan []Member
	EventChan() <-chan serf.UserEvent
	BroadcastEvent(serf.UserEvent) error
}

type ClusterImpl struct {
	config *config.Cli
	serf   *serf.Serf
	// serfCh is an internal channel to receive all events in the Serf cluster.
	// Events from this channel later ends up in either one of the following channels:
	// - `eventCh` (for custom user events)
	// - `memberCh` (for internal membership events)
	serfCh chan serf.Event
	// eventCh is used to receive custom user events
	// This channel is intended to be used by the user of the ClusterImpl struct
	eventCh chan serf.UserEvent
	// membersCh is an internal channel to update the current membership list
	memberCh chan []Member
}

type Member struct {
	Name   string            `json:"name"`
	Tags   map[string]string `json:"tags"`
	Status string            `json:"status"`
}

var MediaFilter = map[string]string{"node": "media"}

// Create a connection to a new Cluster that will immediately connect
func NewCluster(config *config.Cli) Cluster {
	c := ClusterImpl{
		config:   config,
		serfCh:   make(chan serf.Event, serfClusterInternalEventBuffer),
		memberCh: make(chan []Member),
		eventCh:  make(chan serf.UserEvent, config.SerfQueueSize),
	}
	return &c
}

type serfLogger struct{}

func (s serfLogger) Write(p []byte) (int, error) {
	logLine := string(p)
	if strings.Contains(logLine, "[DEBUG]") || strings.Contains(logLine, "[INFO]") {
		return 0, nil
	}

	glog.Info(logLine)
	return len(p), nil
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
	memberlistConfig.LogOutput = serfLogger{}
	serfConfig := serf.DefaultConfig()
	serfConfig.UserEventSizeLimit = 1024
	serfConfig.MemberlistConfig = memberlistConfig
	serfConfig.NodeName = c.config.NodeName
	serfConfig.Tags = c.config.Tags
	serfConfig.EventCh = c.serfCh
	serfConfig.ProtocolVersion = 5
	serfConfig.EventBuffer = c.config.SerfEventBuffer
	serfConfig.MaxQueueDepth = c.config.SerfMaxQueueDepth
	serfConfig.LogOutput = serfLogger{}

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
		n, err := c.serf.Join(c.config.RetryJoin, true)
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

func (c *ClusterImpl) MembersFiltered(filter map[string]string, status, name string) []Member {
	return FilterMembers(toClusterMembers(c.serf.Members()), filter, status, name)
}

func toClusterMembers(members []serf.Member) []Member {
	var nodes []Member
	for _, member := range members {
		nodes = append(nodes, Member{
			Name:   member.Name,
			Tags:   member.Tags,
			Status: member.Status.String(),
		})
	}
	return nodes
}

func FilterMembers(all []Member, filter map[string]string, status string, name string) []Member {
	var nodes []Member
	for _, member := range all {
		if status != "" && status != member.Status {
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
			nodes = append(nodes, member)
		}
	}
	return nodes
}

// Subscribe to changes in the member list. Please only call me once. I only have one channel internally.
func (c *ClusterImpl) MemberChan() chan []Member {
	return c.memberCh
}

// Subscribe to events broadcaster in the serf cluster. Please only call me once. I only have one channel internally.
func (c *ClusterImpl) EventChan() <-chan serf.UserEvent {
	return c.eventCh
}

func (c *ClusterImpl) BroadcastEvent(event serf.UserEvent) error {
	return c.serf.UserEvent(event.Name, event.Payload, event.Coalesce)
}

func (c *ClusterImpl) handleEvents(ctx context.Context) error {
	inbox := make(chan serf.Event, c.config.SerfQueueSize)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-c.serfCh:
				metrics.Metrics.UserEventBufferSize.Set(float64(len(c.eventCh)))
				metrics.Metrics.MemberEventBufferSize.Set(float64(len(inbox)))
				metrics.Metrics.SerfEventBufferSize.Set(float64(len(c.serfCh)))

				switch evt := e.(type) {
				case serf.UserEvent:
					select {
					case <-ctx.Done():
						return
					case c.eventCh <- evt:
						// Event moved to eventCh
					default:
						// Overflow event gets dropped
						glog.Infof("Overflow UserEvent, dropped: %v", evt)
					}
				case serf.MemberEvent:
					select {
					case <-ctx.Done():
						return
					case inbox <- e:
						// Event is now in the inbox
					default:
						// Overflow event gets dropped
						glog.Infof("Overflow MemberEvent, dropped: %v", evt)
					}
				default:
					glog.Infof("Ignoring serf event, dropped: %v", evt.EventType().String())
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

		members := c.MembersFiltered(MediaFilter, "alive", "")

		c.memberCh <- members
	}
}
