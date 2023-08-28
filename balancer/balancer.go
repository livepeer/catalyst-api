package balancer

//go:generate mockgen -source=./balancer.go -destination=../mocks/balancer/balancer.go

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/cluster"
	"golang.org/x/sync/errgroup"
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

type BalancerImpl struct {
	config   *Config
	cmd      *exec.Cmd
	endpoint string
	// Blocks until initial startup
	startupOnce  sync.Once
	startupError error
}

// create a new load balancer instance
func NewBalancer(config *Config) Balancer {
	_, err := exec.LookPath("MistUtilLoad")
	if err != nil {
		glog.Warning("MistUtilLoad not found, not doing meaningful balancing")
		return &BalancerStub{}
	}
	return &BalancerImpl{
		config:   config,
		cmd:      nil,
		endpoint: fmt.Sprintf("http://127.0.0.1:%d", config.MistUtilLoadPort),
	}
}

// start this load balancer instance, execing MistUtilLoad if necessary
func (b *BalancerImpl) Start(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return b.execBalancer(ctx, b.config.Args)
	})
	group.Go(func() error {
		return b.waitForStartup(ctx)
	})
	return group.Wait()
}

// wait for the mist LB to be available. can be called multiple times.
func (b *BalancerImpl) waitForStartup(ctx context.Context) error {
	b.startupOnce.Do(func() {
		i := 0
		for {
			_, err := b.getMistLoadBalancerServers(ctx)
			if err == nil {
				return
			}
			i += 1
			if i > 10 {
				b.startupError = fmt.Errorf("could not contact mist load balancer after %d tries", i)
				return
			}
			select {
			case <-time.After(250 * time.Millisecond):
				continue
			case <-ctx.Done():
				b.startupError = ctx.Err()
				return
			}
		}
	})
	return b.startupError
}

func (b *BalancerImpl) UpdateMembers(ctx context.Context, members []cluster.Member) error {
	ctx, cancel := context.WithTimeout(ctx, mistUtilLoadLoopTimeout)
	defer cancel()
	err := b.waitForStartup(ctx)
	if err != nil {
		return fmt.Errorf("mist load balancer did not start properly: %w", err)
	}
	balancedServers, err := b.getMistLoadBalancerServers(ctx)

	if err != nil {
		glog.Errorf("Error getting mist load balancer servers: %v\n", err)
		return err
	}

	membersMap := make(map[string]bool)

	for _, member := range members {
		memberHost := member.Name

		// commented out as for now the load balancer does not return ports
		//if member.Port != 0 {
		//	memberHost = fmt.Sprintf("%s:%d", memberHost, member.Port)
		//}

		membersMap[memberHost] = true
	}

	glog.V(5).Infof("current members in cluster: %v\n", membersMap)
	glog.V(5).Infof("current members in load balancer: %v\n", balancedServers)

	// compare membersMap and balancedServers
	// del all servers not present in membersMap but present in balancedServers
	// add all servers not present in balancedServers but present in membersMap

	// note: untested as per MistUtilLoad ports
	for k := range balancedServers {
		if _, ok := membersMap[k]; !ok {
			glog.Infof("deleting server %s from load balancer\n", k)
			_, err := b.changeLoadBalancerServers(ctx, k, "del")
			if err != nil {
				glog.Errorf("Error deleting server %s from load balancer: %v\n", k, err)
			}
		}
	}

	for k := range membersMap {
		if _, ok := balancedServers[k]; !ok {
			glog.Infof("adding server %s to load balancer\n", k)
			_, err := b.changeLoadBalancerServers(ctx, k, "add")
			if err != nil {
				glog.Errorf("Error adding server %s to load balancer: %v\n", k, err)
			}
		}
	}
	return nil
}

func (b *BalancerImpl) changeLoadBalancerServers(ctx context.Context, server, action string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, mistUtilLoadSingleRequestTimeout)
	defer cancel()
	var serverURL string
	if server == b.config.NodeName {
		// Special case — make sure the balancer is aware this one is localhost
		serverURL = b.mistAddr()
	} else {
		serverURL = b.formatNodeAddress(server)
	}
	actionURL := b.endpoint + "?" + action + "server=" + url.QueryEscape(serverURL)
	req, err := http.NewRequest("POST", actionURL, nil)
	req = req.WithContext(ctx)
	if err != nil {
		glog.Errorf("Error creating request: %v", err)
		return nil, err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		glog.Errorf("Error making request: %v", err)
		return nil, err
	}

	bytes, err := io.ReadAll(resp.Body)

	if err != nil {
		glog.Errorf("Error reading response: %v", err)
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		glog.Errorf("Error response from load balancer changing servers: %s\n", string(bytes))
		return bytes, errors.New(string(bytes))
	}

	glog.V(6).Infof("requested mist to %s server %s to the load balancer\n", action, server)
	glog.V(6).Info(string(bytes))
	return bytes, nil
}

func (b *BalancerImpl) getMistLoadBalancerServers(ctx context.Context) (map[string]struct{}, error) {
	ctx, cancel := context.WithTimeout(ctx, mistUtilLoadSingleRequestTimeout)
	defer cancel()
	url := b.endpoint + "?lstservers=1"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		glog.Errorf("Error creating request: %v", err)
		return nil, err
	}
	req = req.WithContext(ctx)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		glog.Errorf("Error making request: %v", err)
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		glog.Errorf("Error response from load balancer listing servers: %s\n", string(b))
		return nil, errors.New(string(b))
	}
	bytes, err := io.ReadAll(resp.Body)

	if err != nil {
		glog.Errorf("Error reading response: %v", err)
		return nil, err
	}

	var mistResponse map[string]any

	err = json.Unmarshal([]byte(string(bytes)), &mistResponse)
	if err != nil {
		return nil, err
	}

	output := make(map[string]struct{}, len(mistResponse))

	for k := range mistResponse {
		if k == b.mistAddr() {
			// Special case — recognize 127.0.0.1 and transform it to our node address
			myAddr := b.formatNodeAddress(b.config.NodeName)
			output[myAddr] = struct{}{}
		} else {
			output[k] = struct{}{}
		}
	}

	return output, nil
}

// format a server address for consumption by MistUtilLoad
// commonly this means catalyst-0.example.com --> https://catalyst-0.example.com:443
func (b *BalancerImpl) formatNodeAddress(server string) string {
	return fmt.Sprintf(b.config.MistLoadBalancerTemplate, server)
}

func (b *BalancerImpl) execBalancer(ctx context.Context, balancerArgs []string) error {
	args := append(balancerArgs, "-p", fmt.Sprintf("%d", b.config.MistUtilLoadPort))
	glog.Infof("Running MistUtilLoad with %v", args)
	b.cmd = exec.CommandContext(ctx, "MistUtilLoad", args...)

	b.cmd.Stdout = os.Stdout
	b.cmd.Stderr = os.Stderr

	err := b.cmd.Start()
	if err != nil {
		return err
	}

	err = b.cmd.Wait()
	if err != nil {
		return err
	}
	return fmt.Errorf("MistUtilLoad exited cleanly")
}

func (b *BalancerImpl) queryMistForClosestNode(ctx context.Context, playbackID, lat, lon, prefix string) (string, error) {
	streamName := fmt.Sprintf("%s+%s", prefix, playbackID)
	// First, check to see if any server has this stream
	err1 := b.MistUtilLoadStreamStats(ctx, streamName)
	// Then, check the best playback server
	node, err2 := b.MistUtilLoadBalance(ctx, streamName, lat, lon)
	// If we can't get a playback server, error
	if err2 != nil {
		return "", err2
	}
	// If we didn't find the stream but we did find a node, return it as the 404 handler
	if err1 != nil {
		return node, err1
	}
	// Good path, we found the stream and a playback node!
	return node, nil
}

// return the best node available for a given stream. will return any node if nobody has the stream.
func (b *BalancerImpl) GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string) (string, string, error) {
	var nodeAddr, fullPlaybackID, fallbackAddr string
	var mu sync.Mutex
	var err error
	var waitGroup sync.WaitGroup

	for _, prefix := range redirectPrefixes {
		waitGroup.Add(1)
		go func(prefix string) {
			addr, e := b.queryMistForClosestNode(ctx, playbackID, lat, lon, prefix)
			mu.Lock()
			defer mu.Unlock()
			if e != nil {
				err = e
				glog.V(8).Infof("error finding origin server playbackID=%s prefix=%s error=%s", playbackID, prefix, e)
				// If we didn't find a stream but we did find a server, keep that so we can use it to handle a 404
				if addr != "" {
					fallbackAddr = addr
				}
			} else {
				nodeAddr = addr
				fullPlaybackID = prefix + "+" + playbackID
			}
			waitGroup.Done()
		}(prefix)
	}
	waitGroup.Wait()

	// good path: we found the stream and a good node to play it back, yay!
	if nodeAddr != "" {
		return nodeAddr, fullPlaybackID, nil
	}

	return fallbackNode(fallbackAddr, fallbackPrefix, playbackID, redirectPrefixes[0], err)
}

// `playbackID`s matching the pattern `....-....-.....-....`
var regexpStreamKey = regexp.MustCompile(`^(?:\w{4}-){3}\w{4}$`)

func fallbackNode(fallbackAddr, fallbackPrefix, playbackID, defaultPrefix string, err error) (string, string, error) {
	// Check for `playbackID`s matching the pattern `....-....-.....-....`
	if regexpStreamKey.MatchString(playbackID) {
		return fallbackAddr, playbackID, nil
	}

	// bad path: nobody has the stream, but we did find a server which can handle the 404 for us.
	if fallbackAddr != "" {
		if fallbackPrefix == "" {
			fallbackPrefix = defaultPrefix
		}
		return fallbackAddr, fallbackPrefix + "+" + playbackID, nil
	}

	// ugly path: we couldn't find ANY servers. yikes.
	return "", "", err
}

// make a balancing request to MistUtilLoad, returns a server suitable for playback
func (b *BalancerImpl) MistUtilLoadBalance(ctx context.Context, stream, lat, lon string) (string, error) {
	str, err := b.mistUtilLoadRequest(ctx, "/", stream, lat, lon)
	if err != nil {
		return "", err
	}
	// Special case: rewrite our local node to our public node url
	if str == b.config.MistHost {
		str = b.config.NodeName
	}
	return str, nil
}

// make a source request to MistUtilLoad, returns the DTSC url of the origin server
func (b *BalancerImpl) MistUtilLoadSource(ctx context.Context, stream, lat, lon string) (string, error) {
	str, err := b.mistUtilLoadRequest(ctx, "?source=", stream, lat, lon)
	if err != nil {
		return "", err
	}
	// Special case: rewrite our local node to our public node url
	u, err := url.Parse(str)
	if err != nil {
		return "", err
	}
	if u.Hostname() == b.config.MistHost {
		u.Host = b.config.NodeName
		str = u.String()
	}
	return str, nil
}

// make a streamStats request to MistUtilLoad; response is opaque but a
// successful call means the stream is active somewhere in the world
func (b *BalancerImpl) MistUtilLoadStreamStats(ctx context.Context, stream string) error {
	_, err := b.mistUtilLoadRequest(ctx, "?streamstats=", stream, "0", "0")
	return err
}

// Internal method to make a request to MistUtilLoad
func (b *BalancerImpl) mistUtilLoadRequest(ctx context.Context, route, stream, lat, lon string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, mistUtilLoadSingleRequestTimeout)
	defer cancel()
	enc := url.QueryEscape(stream)
	murl := fmt.Sprintf("%s%s%s", b.endpoint, route, enc)
	glog.V(8).Infof("MistUtilLoad started request=%s", murl)
	req, err := http.NewRequest("GET", murl, nil)
	if err != nil {
		return "", err
	}
	req = req.WithContext(ctx)
	if lat != "" && lon != "" {
		req.Header.Set("X-Latitude", lat)
		req.Header.Set("X-Longitude", lon)
	} else {
		glog.Warningf("Incoming request missing X-Latitude/X-Longitude, response will not be geolocated")
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET request '%s' failed with http status code %d", murl, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("GET request '%s' failed while reading response body", murl)
	}
	glog.V(8).Infof("MistUtilLoad responded request=%s response=%s", murl, body)
	str := string(body)
	if str == "FULL" || str == "null" {
		return "", fmt.Errorf("GET request '%s' returned '%s'", murl, str)
	}

	return str, nil
}

func (b *BalancerImpl) mistAddr() string {
	return fmt.Sprintf("http://%s:%d", b.config.MistHost, b.config.MistPort)
}
