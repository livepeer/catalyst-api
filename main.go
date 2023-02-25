package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	_ "github.com/lib/pq"
	"github.com/livepeer/catalyst-api/api"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/pipeline"
	lpapi "github.com/livepeer/go-api-client"
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
	fs.StringVar(&cli.Host, "host", "0.0.0.0", "Hostname to bind to")
	fs.IntVar(&cli.MistPort, "mist-port", 4242, "Port to connect to Mist")
	fs.IntVar(&cli.MistHttpPort, "mist-http-port", 8080, "Port Mist is listening for HTTP connections")
	fs.StringVar(&cli.APIToken, "api-token", "IAmAuthorized", "Auth header value for API access")
	fs.IntVar(&cli.PromPort, "prom-port", 2112, "Prometheus metrics port")
	fs.StringVar(&cli.SourceOutput, "source-output", "", "URL for the video source segments used if source_segments is not defined in the upload request")
	fs.StringVar(&cli.ExternalTranscoder, "external-transcoder", "", "URL for the external transcoder to be used by the pipeline coordinator. Only 1 implementation today for AWS MediaConvert which should be in the format: mediaconvert://key-id:key-secret@endpoint-host?region=aws-region&role=iam-role&s3_aux_bucket=s3://bucket")
	fs.StringVar(&cli.VodPipelineStrategy, "vod-pipeline-strategy", string(pipeline.StrategyCatalystDominance), "Which strategy to use for the VOD pipeline")
	fs.StringVar(&cli.RecordingCallback, "recording", "http://recording.livepeer.com/recording/status", "Callback URL for recording start&stop events")
	fs.StringVar(&cli.MetricsDBConnectionString, "metrics-db-connection-string", "", "Connection string to use for the metrics Postgres DB. Takes the form: host=X port=X user=X password=X dbname=X")
	config.URLSliceVarFlag(fs, &cli.ImportIPFSGatewayURLs, "import-ipfs-gateway-urls", "https://vod-import-gtw.mypinata.cloud/ipfs/?pinataGatewayToken={{secrets.LP_PINATA_GATEWAY_TOKEN}},https://w3s.link/ipfs/,https://ipfs.io/ipfs/,https://cloudflare-ipfs.com/ipfs/", "Comma delimited ordered list of IPFS gateways (includes /ipfs/ suffix) to import assets from")
	config.URLSliceVarFlag(fs, &cli.ImportArweaveGatewayURLs, "import-arweave-gateway-urls", "https://arweave.net/", "Comma delimited ordered list of arweave gateways")

	// mist-api-connector parameters
	fs.StringVar(&cli.OwnUri, "own-uri", "http://localhost:4949", "URL at wich service will be accessible by MistServer for callbacks")
	fs.StringVar(&cli.MistHost, "mist-host", "127.0.0.1", "Hostname of the Mist server")
	fs.StringVar(&cli.MistUser, "mist-user", "", "username of MistServer")
	fs.StringVar(&cli.MistPassword, "mist-password", "", "password of MistServer")
	fs.DurationVar(&cli.MistConnectTimeout, "mist-connect-timeout", 5*time.Minute, "Max time to wait attempting to connect to Mist server")
	fs.StringVar(&cli.MistStreamSource, "mist-stream-source", "push://", "Stream source we should use for created Mist stream")
	fs.StringVar(&cli.MistHardcodedBroadcasters, "mist-hardcoded-broadcasters", "", "Hardcoded broadcasters for use by MistProcLivepeer")
	config.InvertedBoolFlag(fs, &cli.MistScrapeMetrics, "mist-scrape-metrics", true, "Scrape statistics from MistServer and publish to RabbitMQ")
	fs.StringVar(&cli.MistSendAudio, "send-audio", "record", "when should we send audio?  {always|never|record}")
	fs.StringVar(&cli.MistBaseStreamName, "mist-base-stream-name", "", "Base stream name to be used in wildcard-based routing scheme")
	fs.StringVar(&cli.APIServer, "api-server", lpapi.ProdServer, "Livepeer API server to use")
	fs.StringVar(&cli.AMQPURL, "amqp-url", "", "RabbitMQ url")
	fs.StringVar(&cli.OwnRegion, "own-region", "", "Identifier of the region where the service is running, used for mapping external data back to current region")

	// catalyst-node parameters
	fs.StringVar(&cli.Node, "node", "", "Name of this node within the cluster")

	// special parameters
	mistJson := fs.Bool("j", false, "Print application info as JSON. Used by Mist to present flags in its UI.")
	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	_ = fs.String("config", "", "config file (optional)")

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

	var (
		metricsDB *sql.DB
		err       error
	)

	go func() {
		glog.Fatal(metrics.ListenAndServe(cli.PromPort))
	}()

	mist := &clients.MistClient{
		ApiUrl:          fmt.Sprintf("http://%s:%d/api2", cli.MistHost, cli.MistPort),
		HttpReqUrl:      fmt.Sprintf("http://%s:%d", cli.MistHost, cli.MistHttpPort),
		TriggerCallback: fmt.Sprintf("%s/api/mist/trigger", cli.OwnUri),
	}

	// Kick off the callback client, to send job update messages on a regular interval
	statusClient := clients.NewPeriodicCallbackClient(15 * time.Second).Start()

	// Emit high-cardinality metrics to a Postrgres database if configured
	if cli.MetricsDBConnectionString != "" {
		metricsDB, err = sql.Open("postgres", cli.MetricsDBConnectionString)
		if err != nil {
			glog.Fatalf("Error creating postgres metrics connection: %v", err)
		}
	} else {
		glog.Info("Postgres metrics connection string was not set, postgres metrics are disabled.")
	}

	// Start the "co-ordinator" that determines whether to send jobs to the Catalyst transcoding pipeline
	// or an external one
	vodEngine, err := pipeline.NewCoordinator(pipeline.Strategy(cli.VodPipelineStrategy), mist, cli.SourceOutput, cli.ExternalTranscoder, statusClient, metricsDB)
	if err != nil {
		glog.Fatalf("Error creating VOD pipeline coordinator: %v", err)
	}

	router := httprouter.New()

	mapic := mistapiconnector.StartMapic(&cli)
	api.AddRoutes(router, vodEngine, cli.APIToken)
	mapic.AddRoutes(router)

	listen := fmt.Sprintf("%s:%d", cli.Host, cli.Port)

	log.LogNoRequestID(
		"Starting Catalyst API!",
		"version", config.Version,
		"host", listen,
	)

	// Start the HTTP API server
	// todo: add cli.Host
	if err := http.ListenAndServe(listen, router); err != nil {
		glog.Fatal(err)
	}
}
