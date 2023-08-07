package main

import (
	"context"
	"crypto/rsa"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang/glog"
	_ "github.com/lib/pq"
	"github.com/livepeer/catalyst-api/api"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/crypto"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/catalyst-api/pprof"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
	"github.com/peterbourgon/ff/v3"
	"golang.org/x/sync/errgroup"
)

func main() {
	err := flag.Set("logtostderr", "true")
	if err != nil {
		glog.Fatal(err)
	}
	vFlag := flag.Lookup("v")
	fs := flag.NewFlagSet("catalyst-api", flag.ExitOnError)
	cli := config.Cli{}

	version := fs.Bool("version", false, "print application version")

	// listen addresses
	config.AddrFlag(fs, &cli.HTTPAddress, "http-addr", "0.0.0.0:8989", "Address to bind for external-facing Catalyst HTTP handling")
	config.AddrFlag(fs, &cli.HTTPInternalAddress, "http-internal-addr", "127.0.0.1:7979", "Address to bind for internal privileged HTTP commands")
	config.AddrFlag(fs, &cli.ClusterAddress, "cluster-addr", "0.0.0.0:9935", "Address to bind Serf network listeners to. To use an IPv6 address, specify [::1] or [::1]:7946.")
	fs.StringVar(&cli.ClusterAdvertiseAddress, "cluster-advertise-addr", "", "Address to advertise to the other cluster members")

	// catalyst-api parameters
	fs.StringVar(&cli.APIToken, "api-token", "IAmAuthorized", "Auth header value for API access")
	fs.StringVar(&cli.SourceOutput, "source-output", "", "URL for the video source segments used if source_segments is not defined in the upload request")
	config.URLVarFlag(fs, &cli.PrivateBucketURL, "private-bucket", "", "URL for the private media bucket")
	fs.StringVar(&cli.ExternalTranscoder, "external-transcoder", "", "URL for the external transcoder to be used by the pipeline coordinator. Only 1 implementation today for AWS MediaConvert which should be in the format: mediaconvert://key-id:key-secret@endpoint-host?region=aws-region&role=iam-role&s3_aux_bucket=s3://bucket")
	fs.StringVar(&cli.VodPipelineStrategy, "vod-pipeline-strategy", string(pipeline.StrategyCatalystFfmpegDominance), "Which strategy to use for the VOD pipeline")
	fs.StringVar(&cli.MetricsDBConnectionString, "metrics-db-connection-string", "", "Connection string to use for the metrics Postgres DB. Takes the form: host=X port=X user=X password=X dbname=X")
	config.URLSliceVarFlag(fs, &cli.ImportIPFSGatewayURLs, "import-ipfs-gateway-urls", "https://vod-import-gtw.mypinata.cloud/ipfs/?pinataGatewayToken={{secrets.LP_PINATA_GATEWAY_TOKEN}},https://w3s.link/ipfs/,https://ipfs.io/ipfs/,https://cloudflare-ipfs.com/ipfs/", "Comma delimited ordered list of IPFS gateways (includes /ipfs/ suffix) to import assets from")
	config.URLSliceVarFlag(fs, &cli.ImportArweaveGatewayURLs, "import-arweave-gateway-urls", "https://arweave.net/", "Comma delimited ordered list of arweave gateways")
	fs.BoolVar(&cli.MistCleanup, "run-mist-cleanup", true, "Run mist-cleanup.sh to cleanup shm")
	fs.BoolVar(&cli.LogSysUsage, "run-pod-mon", true, "Run pod-mon script to monitor sys usage")
	fs.StringVar(&cli.BroadcasterURL, "broadcaster-url", config.DefaultBroadcasterURL, "URL of local broadcaster")
	config.InvertedBoolFlag(fs, &cli.MistEnabled, "mist", true, "Disable all Mist integrations. Should only be used for development and CI")

	// mist-api-connector parameters
	fs.IntVar(&cli.MistPort, "mist-port", 4242, "Port to connect to Mist")
	fs.StringVar(&cli.MistHost, "mist-host", "127.0.0.1", "Hostname of the Mist server")
	fs.StringVar(&cli.MistUser, "mist-user", "", "username of MistServer")
	fs.StringVar(&cli.MistPassword, "mist-password", "", "password of MistServer")
	fs.DurationVar(&cli.MistConnectTimeout, "mist-connect-timeout", 5*time.Minute, "Max time to wait attempting to connect to Mist server")
	fs.StringVar(&cli.MistStreamSource, "mist-stream-source", "push://", "Stream source we should use for created Mist stream")
	fs.StringVar(&cli.MistHardcodedBroadcasters, "mist-hardcoded-broadcasters", "", "Hardcoded broadcasters for use by MistProcLivepeer")
	config.InvertedBoolFlag(fs, &cli.MistScrapeMetrics, "mist-scrape-metrics", true, "Scrape statistics from MistServer and publish to RabbitMQ")
	fs.StringVar(&cli.MistSendAudio, "send-audio", "record", "when should we send audio?  {always|never|record}")
	fs.StringVar(&cli.MistBaseStreamName, "mist-base-stream-name", "video", "Base stream name to be used in wildcard-based routing scheme")
	fs.StringVar(&cli.APIServer, "api-server", "", "Livepeer API server to use")
	fs.StringVar(&cli.AMQPURL, "amqp-url", "", "RabbitMQ url")
	fs.StringVar(&cli.OwnRegion, "own-region", "", "Identifier of the region where the service is running, used for mapping external data back to current region")
	fs.StringVar(&cli.StreamHealthHookURL, "stream-health-hook-url", "http://localhost:3004/api/stream/hook/health", "Address to POST stream health payloads to (response is ignored)")

	// catalyst-node parameters
	hostname, _ := os.Hostname()
	fs.StringVar(&cli.NodeName, "node", hostname, "Name of this node within the cluster")
	config.SpaceSliceFlag(fs, &cli.BalancerArgs, "balancer-args", []string{}, "arguments passed to MistUtilLoad")
	fs.StringVar(&cli.NodeHost, "node-host", "", "Hostname this node should handle requests for. Requests on any other domain will trigger a redirect. Useful as a 404 handler to send users to another node.")
	fs.Float64Var(&cli.NodeLatitude, "node-latitude", 0, "Latitude of this Catalyst node. Used for load balancing.")
	fs.Float64Var(&cli.NodeLongitude, "node-longitude", 0, "Longitude of this Catalyst node. Used for load balancing.")
	config.CommaSliceFlag(fs, &cli.RedirectPrefixes, "redirect-prefixes", []string{}, "Set of valid prefixes of playback id which are handled by mistserver")
	config.CommaMapFlag(fs, &cli.Tags, "tags", map[string]string{"node": "media"}, "Serf tags for Catalyst nodes")
	fs.IntVar(&cli.MistLoadBalancerPort, "mist-load-balancer-port", rand.Intn(10000)+40000, "MistUtilLoad port (default random)")
	fs.StringVar(&cli.MistLoadBalancerTemplate, "mist-load-balancer-template", "http://%s:4242", "template for specifying the host that should be queried for Prometheus stat output for this node")
	config.CommaSliceFlag(fs, &cli.RetryJoin, "retry-join", []string{}, "An agent to join with. This flag be specified multiple times. Does not exit on failure like -join, used to retry until success.")
	fs.StringVar(&cli.EncryptKey, "encrypt", "", "Key for encrypting network traffic within Serf. Must be a base64-encoded 32-byte key.")
	fs.StringVar(&cli.VodDecryptPublicKey, "catalyst-public-key", "", "Public key of the catalyst node for encryption")
	fs.StringVar(&cli.VodDecryptPrivateKey, "catalyst-private-key", "", "Private key of the catalyst node for encryption")
	fs.StringVar(&cli.GateURL, "gate-url", "http://localhost:3004/api/access-control/gate", "Address to contact playback gating API for access control verification")
	config.InvertedBoolFlag(fs, &cli.MistTriggerSetup, "mist-trigger-setup", true, "Overwrite Mist triggers with the ones built into catalyst-api")
	pprofPort := fs.Int("pprof-port", 6061, "Pprof listen port")

	// special parameters
	mistJson := fs.Bool("j", false, "Print application info as JSON. Used by Mist to present flags in its UI.")
	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	_ = fs.String("config", "", "config file (optional)")

	err = ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("CATALYST_API"),
	)
	if err != nil {
		glog.Fatalf("error parsing cli: %s", err)
	}
	cli.ParseLegacyEnv()
	if len(fs.Args()) > 0 {
		glog.Fatalf("unexpected extra arguments on command line: %v", fs.Args())
	}
	err = flag.CommandLine.Parse(nil)
	if err != nil {
		glog.Fatal(err)
	}

	if *version {
		fmt.Printf("catalyst-api version: %s", config.Version)
		return
	}

	go func() {
		log.Println(pprof.ListenAndServe(*pprofPort))
	}()

	if *verbosity != "" {
		err = vFlag.Value.Set(*verbosity)
		if err != nil {
			glog.Fatal(err)
		}
	}

	if *mistJson {
		mistconnector.PrintMistConfigJson("catalyst-api", "HTTP API server for translating Catalyst API requests into Mist calls", "Catalyst API", config.Version, fs)
		return
	}

	if cli.MistUser != "" || cli.MistPassword != "" {
		glog.Warning("DEPRECATION NOTICE: mist-user and mist-password are no longer used and will be removed in a later version")
	}

	// TODO: I don't love the global variables for these
	config.ImportIPFSGatewayURLs = cli.ImportIPFSGatewayURLs
	config.ImportArweaveGatewayURLs = cli.ImportArweaveGatewayURLs
	config.PrivateBucketURL = cli.PrivateBucketURL
	config.HTTPInternalAddress = cli.HTTPInternalAddress

	var (
		metricsDB *sql.DB
	)

	// Kick off the callback client, to send job update messages on a regular interval
	headers := map[string]string{"Authorization": fmt.Sprintf("Bearer %s", cli.APIToken)}
	statusClient := clients.NewPeriodicCallbackClient(15*time.Second, headers).Start()

	// Emit high-cardinality metrics to a Postrgres database if configured
	if cli.MetricsDBConnectionString != "" {
		metricsDB, err = sql.Open("postgres", cli.MetricsDBConnectionString)
		if err != nil {
			glog.Fatalf("Error creating postgres metrics connection: %v", err)
		}
	} else {
		glog.Info("Postgres metrics connection string was not set, postgres metrics are disabled.")
	}

	var vodDecryptPrivateKey *rsa.PrivateKey

	if cli.VodDecryptPrivateKey != "" && cli.VodDecryptPublicKey != "" {
		vodDecryptPrivateKey, err = crypto.LoadPrivateKey(cli.VodDecryptPrivateKey)
		if err != nil {
			glog.Fatalf("Error loading vod decrypt private key: %v", err)
		}
		isValidKeyPair, err := crypto.ValidateKeyPair(cli.VodDecryptPublicKey, *vodDecryptPrivateKey)
		if !isValidKeyPair || err != nil {
			glog.Fatalf("Invalid vod decrypt key pair")
		}
	}

	// Start the "co-ordinator" that determines whether to send jobs to the Catalyst transcoding pipeline
	// or an external one
	vodEngine, err := pipeline.NewCoordinator(pipeline.Strategy(cli.VodPipelineStrategy), cli.SourceOutput, cli.ExternalTranscoder, statusClient, metricsDB, vodDecryptPrivateKey, cli.BroadcasterURL)
	if err != nil {
		glog.Fatalf("Error creating VOD pipeline coordinator: %v", err)
	}

	// Start cron style apps to run periodically
	if cli.ShouldMistCleanup() {
		app := "mist-cleanup.sh"
		// schedule mist-cleanup every 2hrs with a timeout of 15min
		mistCleanup, err := middleware.NewShell(2*60*60*time.Second, 15*60*time.Second, app)
		if err != nil {
			glog.Info("Failed to shell out:", app, err)
		}
		mistCleanupTick := mistCleanup.RunBg()
		defer mistCleanupTick.Stop()
	}
	if cli.ShouldLogSysUsage() {
		app := "pod-mon.sh"
		// schedule pod-mon every 60s with timeout of 15s
		podMon, err := middleware.NewShell(60*time.Second, 15*time.Second, app)
		if err != nil {
			glog.Info("Failed to shell out:", app, err)
		}
		podMonTick := podMon.RunBg()
		defer podMonTick.Stop()
	}

	broker := misttriggers.NewTriggerBroker()

	var mist clients.MistAPIClient
	if cli.MistEnabled {
		ownURL := fmt.Sprintf("%s/api/mist/trigger", cli.OwnInternalURL())
		mist = clients.NewMistAPIClient(cli.MistUser, cli.MistPassword, cli.MistHost, cli.MistPort, ownURL)
		if cli.MistTriggerSetup {
			err := broker.SetupMistTriggers(mist)
			if err != nil {
				glog.Error("catalyst-api was unable to communicate with MistServer to set up its triggers.")
				glog.Error("hint: are you trying to boot catalyst-api without Mist for development purposes? use the flag -no-mist")
				glog.Fatalf("error setting up Mist triggers err=%s", err)
			}
		}
	} else {
		glog.Info("-no-mist flag detected, not initializing Mist stream triggers")
	}

	var mapic mistapiconnector.IMac
	if cli.ShouldMapic() {
		mapic = mistapiconnector.NewMapic(&cli, broker, mist)
	}

	// Start balancer
	bal := balancer.NewBalancer(&balancer.Config{
		Args:                     cli.BalancerArgs,
		MistUtilLoadPort:         uint32(cli.MistLoadBalancerPort),
		MistLoadBalancerTemplate: cli.MistLoadBalancerTemplate,
		MistHost:                 cli.MistHost,
		MistPort:                 cli.MistPort,
		NodeName:                 cli.NodeName,
	})

	c := cluster.NewCluster(&cli)

	// Initialize root context; cancelling this prompts all components to shut down cleanly
	group, ctx := errgroup.WithContext(context.Background())

	group.Go(func() error {
		return handleSignals(ctx)
	})

	group.Go(func() error {
		return api.ListenAndServe(ctx, cli, vodEngine, bal, c)
	})

	group.Go(func() error {
		return api.ListenAndServeInternal(ctx, cli, vodEngine, mapic, bal, c, broker)
	})

	if cli.ShouldMapic() {
		group.Go(func() error {
			return mapic.Start(ctx)
		})
	}

	group.Go(func() error {
		return bal.Start(ctx)
	})

	group.Go(func() error {
		return c.Start(ctx)
	})

	group.Go(func() error {
		return reconcileBalancer(ctx, bal, c)
	})

	err = group.Wait()
	glog.Infof("Shutdown complete. Reason for shutdown: %s", err)
}

// Eventually this will be the main loop of the state machine, but we just have one variable right now.
func reconcileBalancer(ctx context.Context, bal balancer.Balancer, c cluster.Cluster) error {
	memberCh := c.MemberChan()
	for {
		select {
		case <-ctx.Done():
			return nil
		case list := <-memberCh:
			err := bal.UpdateMembers(ctx, list)
			if err != nil {
				return fmt.Errorf("failed to update load balancer from member list: %w", err)
			}
		}
	}
}

func handleSignals(ctx context.Context) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case s := <-c:
			glog.Errorf("caught signal=%v, attempting clean shutdown", s)
			return fmt.Errorf("caught signal=%v", s)
		case <-ctx.Done():
			return nil
		}
	}
}
