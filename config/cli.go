package config

import "net/url"

type Cli struct {
	Port                      int
	MistPort                  int
	MistHttpPort              int
	PromPort                  int
	APIToken                  string
	SourceOutput              string
	PrivateBucketURL          *url.URL
	ExternalTranscoder        string
	VodPipelineStrategy       string
	RecordingCallback         string
	MetricsDBConnectionString string
	ImportIPFSGatewayURLs     []*url.URL
	ImportArweaveGatewayURLs  []*url.URL
}
