package config

import (
	"encoding/base64"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
)

type Cli struct {
	HTTPAddress               string
	HTTPInternalAddress       string
	ClusterAddress            string
	ClusterAdvertiseAddress   string
	MistHost                  string
	MistUser                  string
	MistPassword              string
	MistPort                  int
	MistConnectTimeout        time.Duration
	MistStreamSource          string
	MistHardcodedBroadcasters string
	MistScrapeMetrics         bool
	MistSendAudio             string
	MistBaseStreamName        string
	MistLoadBalancerPort      int
	MistLoadBalancerTemplate  string
	MistCleanup               bool
	AMQPURL                   string
	OwnRegion                 string
	APIToken                  string
	APIServer                 string
	SourceOutput              string
	PrivateBucketURL          *url.URL
	ExternalTranscoder        string
	VodPipelineStrategy       string
	MetricsDBConnectionString string
	ImportIPFSGatewayURLs     []*url.URL
	ImportArweaveGatewayURLs  []*url.URL
	NodeName                  string
	BalancerArgs              []string
	NodeHost                  string
	NodeLatitude              float64
	NodeLongitude             float64
	RedirectPrefixes          []string
	Tags                      map[string]string
	RetryJoin                 []string
	EncryptKey                string
	VodDecryptPublicKey       string
	VodDecryptPrivateKey      string
	GateURL                   string
	StreamHealthHookURL       string
}

// Return our own URL for callback trigger purposes
func (cli *Cli) OwnInternalURL() string {
	//  No errors because we know it's valid from AddrFlag
	host, port, _ := net.SplitHostPort(cli.HTTPInternalAddress)
	ip := net.ParseIP(host)
	if ip.IsUnspecified() {
		host = "127.0.0.1"
	}
	addr := net.JoinHostPort(host, port)
	return fmt.Sprintf("http://%s", addr)
}

// EncryptBytes returns the encryption key configured.
func (cli *Cli) EncryptBytes() ([]byte, error) {
	return base64.StdEncoding.DecodeString(cli.EncryptKey)
}

// Should we enable mapic?
func (cli *Cli) ShouldMapic() bool {
	return cli.APIServer != ""
}

// Should we enable mist-cleanup script to run periodically and delete leaky shm?
func (cli *Cli) ShouldMistCleanup() bool {
	return cli.MistCleanup
}

// Handle some legacy environment variables for zero-downtime catalyst-node migration
func (cli *Cli) ParseLegacyEnv() {
	node := os.Getenv("CATALYST_NODE_NODE")
	if node != "" {
		cli.NodeName = node
		glog.Warning("Detected legacy env CATALYST_NODE_NODE, please migrate to CATALYST_API_NODE")
	}

	bind := os.Getenv("CATALYST_NODE_BIND")
	if bind != "" {
		cli.ClusterAddress = bind
		glog.Warning("Detected legacy env CATALYST_NODE_BIND, please migrate to CATALYST_API_CLUSTER_ADDR")
	}

	advertise := os.Getenv("CATALYST_NODE_ADVERTISE")
	if advertise != "" {
		cli.ClusterAdvertiseAddress = advertise
		glog.Warning("Detected legacy env CATALYST_NODE_ADVERTISE, please migrate to CATALYST_API_CLUSTER_ADVERTISE_ADDR")
	}

	tags := os.Getenv("CATALYST_NODE_SERF_TAGS")
	if tags != "" {
		parsed, err := parseCommaMap(tags)
		if err != nil {
			panic(fmt.Errorf("error parsing CATALYST_NODE_SERF_TAGS: %w", err))
		}
		cli.Tags = parsed
		glog.Warning("Detected legacy env CATALYST_NODE_SERF_TAGS, please migrate to CATALYST_API_TAGS")
	}
}

// still a string, but validates the provided value is some kind of coherent host:port
func AddrFlag(fs *flag.FlagSet, dest *string, name, value, usage string) {
	*dest = value
	fs.Func(name, usage, func(s string) error {
		host, _, err := net.SplitHostPort(s)
		if err != nil {
			return err
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("invalid address: %s", s)
		}
		*dest = s
		return nil
	})
}

func parseURL(s string, dest **url.URL) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if _, err = url.ParseQuery(u.RawQuery); err != nil {
		return err
	}
	*dest = u
	return nil
}

func URLVarFlag(fs *flag.FlagSet, dest **url.URL, name, value, usage string) {
	fs.Func(name, usage, func(s string) error {
		return parseURL(s, dest)
	})
}

func URLSliceVarFlag(fs *flag.FlagSet, dest *[]*url.URL, name, value, usage string) {
	fs.Func(name, usage, func(s string) error {
		return parseURLs(s, dest)
	})
}

func parseURLs(s string, dest *[]*url.URL) error {
	strs := strings.Split(s, ",")
	urls := make([]*url.URL, len(strs))
	for i, str := range strs {
		if err := parseURL(str, &urls[i]); err != nil {
			return err
		}
	}
	*dest = urls
	return nil
}

// handles -foo "value1 value2 value3"
func SpaceSliceFlag(fs *flag.FlagSet, dest *[]string, name string, value []string, usage string) {
	*dest = value
	fs.Func(name, usage, func(s string) error {
		split := strings.Split(s, " ")
		if len(split) == 1 && split[0] == "" {
			*dest = []string{}
			return nil
		}
		*dest = split
		return nil
	})
}

// handles -foo=value1,value2,value3
func CommaSliceFlag(fs *flag.FlagSet, dest *[]string, name string, value []string, usage string) {
	*dest = value
	fs.Func(name, usage, func(s string) error {
		split := strings.Split(s, ",")
		if len(split) == 1 && split[0] == "" {
			*dest = []string{}
			return nil
		}
		*dest = split
		return nil
	})
}

func parseCommaMap(s string) (map[string]string, error) {
	output := map[string]string{}
	if s == "" {
		return output, nil
	}
	for _, pair := range strings.Split(s, ",") {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			return map[string]string{}, fmt.Errorf("failed to parse keypairs, -option=k1=v1,k2=v2 format required, got %s", s)
		}
		k, v := kv[0], kv[1]
		output[k] = v
	}
	return output, nil
}

// handles -foo=key1=value1,key2=value2
func CommaMapFlag(fs *flag.FlagSet, dest *map[string]string, name string, value map[string]string, usage string) {
	*dest = value
	fs.Func(name, usage, func(s string) error {
		var err error
		*dest, err = parseCommaMap(s)
		return err
	})
}

type InvertedBool struct {
	Value *bool
}

func (f *InvertedBool) String() string {
	return fmt.Sprint(*f.Value)
}

func (f *InvertedBool) IsBoolFlag() bool {
	return true
}

func (f *InvertedBool) Set(value string) error {
	if value == "true" {
		*f.Value = false
	} else if value == "false" {
		*f.Value = true
	} else {
		return fmt.Errorf("only true and false values allowed")
	}
	return nil
}

// MistController has trouble giving us `-booleanFlag=false` values, so we use `-noBooleanFlag=true` instead 🤷‍♂️
func InvertedBoolFlag(fs *flag.FlagSet, dest *bool, name string, value bool, usage string) {
	*dest = value
	fs.Var(&InvertedBool{dest}, fmt.Sprintf("no-%s", name), usage)
}
