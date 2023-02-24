package config

import (
	"flag"
	"net/url"
	"strings"
)

type Cli struct {
	Port                      int
	MistPort                  int
	MistHttpPort              int
	PromPort                  int
	APIToken                  string
	SourceOutput              string
	ExternalTranscoder        string
	VodPipelineStrategy       string
	RecordingCallback         string
	MetricsDBConnectionString string
	ImportIPFSGatewayURLs     []*url.URL
	ImportArweaveGatewayURLs  []*url.URL
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
