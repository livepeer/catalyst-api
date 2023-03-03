package config

import (
	"flag"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type Cli struct {
	Port                      int
	Host                      string
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
	AMQPURL                   string
	OwnRegion                 string
	PromPort                  int
	APIToken                  string
	APIServer                 string
	SourceOutput              string
	PrivateBucketURL          *url.URL
	ExternalTranscoder        string
	VodPipelineStrategy       string
	RecordingCallback         string
	MetricsDBConnectionString string
	ImportIPFSGatewayURLs     []*url.URL
	ImportArweaveGatewayURLs  []*url.URL
	Node                      string
}

// Return our own URL for callback purposes given our address and port
func (cli *Cli) OwnUrl() string {
	return fmt.Sprintf("http://127.0.0.1:%d", cli.Port)
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
