package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/livepeer/catalyst-api/api"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
	"github.com/peterbourgon/ff"
)

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")
	fs := flag.NewFlagSet("catalyst-api", flag.ExitOnError)
	cli := config.Cli{}

	// catalyst-api parameters
	fs.IntVar(&cli.Port, "port", 4949, "Port to listen on")
	fs.IntVar(&cli.MistPort, "mist-port", 4242, "Port to connect to Mist")
	fs.IntVar(&cli.MistHttpPort, "mist-http-port", 8080, "Port Mist is listening for HTTP connections")
	fs.StringVar(&cli.APIToken, "api-token", "IAmAuthorized", "Auth header value for API access")
	fs.IntVar(&cli.PromPort, "prom-port", 2112, "Prometheus metrics port")
	fs.StringVar(&cli.SourceOutput, "source-output", "", "URL for the video source segments used if source_segments is not defined in the upload request")
	URLVarFlag(fs, &cli.PrivateBucketURL, "private-bucket", "", "URL for the private media bucket")
	fs.StringVar(&cli.ExternalTranscoder, "external-transcoder", "", "URL for the external transcoder to be used by the pipeline coordinator. Only 1 implementation today for AWS MediaConvert which should be in the format: mediaconvert://key-id:key-secret@endpoint-host?region=aws-region&role=iam-role&s3_aux_bucket=s3://bucket")
	fs.StringVar(&cli.VodPipelineStrategy, "vod-pipeline-strategy", string(pipeline.StrategyCatalystDominance), "Which strategy to use for the VOD pipeline")
	fs.StringVar(&cli.RecordingCallback, "recording", "http://recording.livepeer.com/recording/status", "Callback URL for recording start&stop events")
	fs.StringVar(&cli.MetricsDBConnectionString, "metrics-db-connection-string", "", "Connection string to use for the metrics Postgres DB. Takes the form: host=X port=X user=X password=X dbname=X")
	config.URLSliceVarFlag(fs, &cli.ImportIPFSGatewayURLs, "import-ipfs-gateway-urls", "https://vod-import-gtw.mypinata.cloud/ipfs/?pinataGatewayToken={{secrets.LP_PINATA_GATEWAY_TOKEN}},https://w3s.link/ipfs/,https://ipfs.io/ipfs/,https://cloudflare-ipfs.com/ipfs/", "Comma delimited ordered list of IPFS gateways (includes /ipfs/ suffix) to import assets from")
	config.URLSliceVarFlag(fs, &cli.ImportArweaveGatewayURLs, "import-arweave-gateway-urls", "https://arweave.net/", "Comma delimited ordered list of arweave gateways")

	// special parameters
	mistJson := fs.Bool("j", false, "Print application info as JSON. Used by Mist to present flags in its UI.")
	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("CATALYST_API"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)

	if *mistJson {
		mistconnector.PrintMistConfigJson("catalyst-api", "HTTP API server for translating Catalyst API requests into Mist calls", "Catalyst API", config.Version, flag.CommandLine)
		return
	}

	// TODO: I don't love the global variables for these
	config.ImportIPFSGatewayURLs = cli.ImportIPFSGatewayURLs
	config.ImportArweaveGatewayURLs = cli.ImportArweaveGatewayURLs
	config.RecordingCallback = cli.RecordingCallback
	config.PrivateBucketURL = cli.PrivateBucketURL

	var (
		metricsDB *sql.DB
		err       error
	)

	go func() {
		log.Fatal(metrics.ListenAndServe(cli.PromPort))
	}()

	mist := &clients.MistClient{
		ApiUrl:          fmt.Sprintf("http://localhost:%d/api2", cli.MistPort),
		HttpReqUrl:      fmt.Sprintf("http://localhost:%d", cli.MistHttpPort),
		TriggerCallback: fmt.Sprintf("http://localhost:%d/api/mist/trigger", cli.Port),
	}

	// Kick off the callback client, to send job update messages on a regular interval
	statusClient := clients.NewPeriodicCallbackClient(15 * time.Second).Start()

	// Emit high-cardinality metrics to a Postrgres database if configured
	if cli.MetricsDBConnectionString != "" {
		metricsDB, err = sql.Open("postgres", cli.MetricsDBConnectionString)
		if err != nil {
			log.Fatalf("Error creating postgres metrics connection: %v", err)
		}
	} else {
		log.Println("Postgres metrics connection string was not set, postgres metrics are disabled.")
	}

	// Start the "co-ordinator" that determines whether to send jobs to the Catalyst transcoding pipeline
	// or an external one
	vodEngine, err := pipeline.NewCoordinator(pipeline.Strategy(cli.VodPipelineStrategy), mist, cli.SourceOutput, cli.ExternalTranscoder, statusClient, metricsDB)
	if err != nil {
		log.Fatalf("Error creating VOD pipeline coordinator: %v", err)
	}

	// Start the HTTP API server
	if err := api.ListenAndServe(cli.Port, cli.APIToken, vodEngine); err != nil {
		log.Fatal(err)
	}
}
