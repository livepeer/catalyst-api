package mist_balancer

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

	"github.com/livepeer/catalyst-api/balancer/catabalancer"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
)

var mistUtilLoadSingleRequestTimeout = 15 * time.Second
var mistUtilLoadLoopTimeout = 2 * time.Minute

type MistBalancer struct {
	config   *balancer.Config
	endpoint string
	// Blocks until initial startup
	startupOnce  sync.Once
	startupError error
}

// create a new load balancer instance
func NewLocalBalancer(config *balancer.Config) balancer.Balancer {
	_, err := exec.LookPath("MistUtilLoad")
	if err != nil {
		glog.Warning("MistUtilLoad not found, not doing meaningful balancing")
		return &balancer.BalancerStub{}
	}
	return &MistBalancer{
		config:   config,
		endpoint: fmt.Sprintf("http://127.0.0.1:%d", config.MistUtilLoadPort),
	}
}

func NewRemoteBalancer(config *balancer.Config) balancer.Balancer {
	return &MistBalancer{
		config:   config,
		endpoint: fmt.Sprintf("http://%s:%d", config.MistHost, config.MistUtilLoadPort),
	}
}

// start this load balancer instance, execing MistUtilLoad if necessary
func (b *MistBalancer) Start(ctx context.Context) error {
	b.killPreviousBalancer(ctx)

	go func() {
		b.reconcileBalancerLoop(ctx, b.config.Args)
	}()
	return b.waitForStartup(ctx)
}

// wait for the mist LB to be available. can be called multiple times.
func (b *MistBalancer) waitForStartup(ctx context.Context) error {
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

func (b *MistBalancer) UpdateMembers(ctx context.Context, members []cluster.Member) error {
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

func (b *MistBalancer) changeLoadBalancerServers(ctx context.Context, server, action string) ([]byte, error) {
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
	defer resp.Body.Close()

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

func (b *MistBalancer) getMistLoadBalancerServers(ctx context.Context) (map[string]struct{}, error) {
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
	defer resp.Body.Close()

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
func (b *MistBalancer) formatNodeAddress(server string) string {
	return fmt.Sprintf(b.config.MistLoadBalancerTemplate, server)
}

// killPreviousBalancer cleans up the previous MistUtilLoad process if it exists.
// It uses pkill to kill the process.
func (b *MistBalancer) killPreviousBalancer(ctx context.Context) {
	cmd := exec.CommandContext(ctx, "pkill", "-9", "-f", "MistUtilLoad")
	err := cmd.Run()
	if err != nil {
		glog.V(6).Infof("Killing MistUtilLoad failed, most probably it was not running, err=%v", err)
	}
}

// reconcileBalancerLoop makes sure that MistUtilLoad is up and running all the time.
func (b *MistBalancer) reconcileBalancerLoop(ctx context.Context, balancerArgs []string) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		b.reconcileBalancer(ctx, balancerArgs)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// reconcileBalancer makes sure that MistUtilLoad is up and running.
func (b *MistBalancer) reconcileBalancer(ctx context.Context, balancerArgs []string) {
	if !b.isBalancerRunning(ctx) {
		glog.Info("Starting MistUtilLoad")
		err := b.execBalancer(ctx, balancerArgs)
		if err != nil {
			glog.Warningf("Error starting MistUtilLoad: %v", err)
		}
	}
}

// isBalancerRunning checks if MistUtilLoad is running.
func (b *MistBalancer) isBalancerRunning(ctx context.Context) bool {
	cmd := exec.CommandContext(ctx, "pgrep", "-f", "MistUtilLoad")
	err := cmd.Run()
	return err == nil
}

func (b *MistBalancer) execBalancer(ctx context.Context, balancerArgs []string) error {
	args := append(balancerArgs, "-p", fmt.Sprintf("%d", b.config.MistUtilLoadPort), "-g", "4")
	glog.Infof("Running MistUtilLoad with %v", args)
	cmd := exec.CommandContext(ctx, "MistUtilLoad", args...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil {
		return err
	}
	return fmt.Errorf("MistUtilLoad exited cleanly")
}

func (b *MistBalancer) queryMistForClosestNode(ctx context.Context, playbackID, lat, lon, prefix string, isStudioReq bool) (string, error) {
	streamName := fmt.Sprintf("%s+%s", prefix, playbackID)
	// First, check to see if any server has this stream
	err1 := b.MistUtilLoadStreamStats(ctx, streamName)
	// Then, check the best playback server
	node, err2 := b.MistUtilLoadBalance(ctx, streamName, lat, lon, isStudioReq)
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
func (b *MistBalancer) GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string, isStudioReq bool) (string, string, error) {
	var nodeAddr, fullPlaybackID, fallbackAddr string
	var mu sync.Mutex
	var err error
	var waitGroup sync.WaitGroup

	for _, prefix := range redirectPrefixes {
		waitGroup.Add(1)
		go func(prefix string) {
			defer waitGroup.Done()
			addr, e := b.queryMistForClosestNode(ctx, playbackID, lat, lon, prefix, isStudioReq)
			mu.Lock()
			defer mu.Unlock()
			if e != nil {
				err = e
				glog.V(9).Infof("error finding origin server playbackID=%s prefix=%s error=%s", playbackID, prefix, e)
				// If we didn't find a stream but we did find a server, keep that so we can use it to handle a 404
				if addr != "" {
					fallbackAddr = addr
				}
			} else {
				nodeAddr = addr
				fullPlaybackID = prefix + "+" + playbackID
			}
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
func (b *MistBalancer) MistUtilLoadBalance(ctx context.Context, stream, lat, lon string, isStudioReq bool) (string, error) {
	// add `tag_adjust={"region_name":1000}` to bump current region weight so it's more important that other params like
	// cpu, memory, bandwidth or distance. It's meant to minimise redirects to other regions after current region is "selected"
	// by the DNS rules.
	// However, if the current request is a Studio request (e.g. to start a pull ingest), then don't bump the current region weight at all
	// since DNS rules might select a wrong node where this code runs. In this case, the lat/lon specified in the Studio request should be
	// used to geolocate for which a higher global geo weight is applied (in livepeer-infra).
	tagAdjustVal := b.config.OwnRegionTagAdjust
	if isStudioReq {
		tagAdjustVal = 0
	}
	str, err := b.mistUtilLoadRequest(ctx, "/", stream, lat, lon, fmt.Sprintf("?tag_adjust={\"%s\":%d}", b.config.OwnRegion, tagAdjustVal))
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
func (b *MistBalancer) MistUtilLoadSource(ctx context.Context, stream, lat, lon string) (string, error) {
	str, err := b.mistUtilLoadRequest(ctx, "?source=", stream, lat, lon, "")
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
func (b *MistBalancer) MistUtilLoadStreamStats(ctx context.Context, stream string) error {
	_, err := b.mistUtilLoadRequest(ctx, "?streamstats=", stream, "0", "0", "")
	return err
}

// Internal method to make a request to MistUtilLoad
func (b *MistBalancer) mistUtilLoadRequest(ctx context.Context, route, stream, lat, lon, urlSuffix string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, mistUtilLoadSingleRequestTimeout)
	defer cancel()
	streamEscaped := url.QueryEscape(stream)
	murl := fmt.Sprintf("%s%s%s%s", b.endpoint, route, streamEscaped, urlSuffix)
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

func (b *MistBalancer) mistAddr() string {
	return fmt.Sprintf("http://%s:%d", b.config.MistHost, b.config.MistPort)
}

func (b *MistBalancer) UpdateNodes(id string, nodeMetrics catabalancer.NodeMetrics) {
	//noop
}

func (b *MistBalancer) UpdateStreams(id string, stream string, isIngest bool) {
	//noop
}
