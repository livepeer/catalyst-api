package config

import (
	"flag"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

type Cli struct {
	HTTPAddress               string
	HTTPInternalAddress       string
	ClusterAddress            string
	ClusterAdvertiseAddress   string
	RPCAddr                   string
	MistHost                  string
	MistUser                  string
	MistPassword              string
	MistPort                  int
	MistHttpPort              int
	MistConnectTimeout        time.Duration
	MistStreamSource          string
	MistHardcodedBroadcasters string
	MistScrapeMetrics         bool
	MistSendAudio             string
	MistBaseStreamName        string
	MistLoadBalancerPort      int
	MistLoadBalancerTemplate  string
	AMQPURL                   string
	OwnRegion                 string
	PromPort                  int
	APIToken                  string
	APIServer                 string
	SourceOutput              string
	ExternalTranscoder        string
	VodPipelineStrategy       string
	RecordingCallback         string
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
	GateURL                   string
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
	if err := parseURL(value, dest); err != nil {
		panic(err)
	}
	fs.Func(name, usage, func(s string) error {
		return parseURL(s, dest)
	})
}

func URLSliceVarFlag(fs *flag.FlagSet, dest *[]*url.URL, name, value, usage string) {
	if err := parseURLs(value, dest); err != nil {
		panic(err)
	}
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

// handles -foo=key1=value1,key2=value2
func CommaMapFlag(fs *flag.FlagSet, dest *map[string]string, name string, value map[string]string, usage string) {
	*dest = value
	fs.Func(name, usage, func(s string) error {
		output := map[string]string{}
		if s == "" {
			*dest = output
			return nil
		}
		for _, pair := range strings.Split(s, ",") {
			kv := strings.Split(pair, "=")
			if len(kv) != 2 {
				return fmt.Errorf("failed to parse keypairs, -%s=k1=v1,k2=v2 format required, got %s", name, SEGMENTING_PREFIX)
			}
			k, v := kv[0], kv[1]
			output[k] = v
		}
		*dest = output
		return nil
	})
}

// handles -balancer-args="-foo six -bar=seven"
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
