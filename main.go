package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"time"

	_ "github.com/lib/pq"
	"github.com/livepeer/catalyst-api/api"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
)

func main() {
	port := flag.Int("port", 4949, "Port to listen on")
	mistPort := flag.Int("mist-port", 4242, "Port to listen on")
	mistHttpPort := flag.Int("mist-http-port", 8080, "Port to listen on")
	apiToken := flag.String("api-token", "IAmAuthorized", "Auth header value for API access")
	mistJson := flag.Bool("j", false, "Print application info as JSON. Used by Mist to present flags in its UI.")
	promPort := flag.Int("prom-port", 2112, "Prometheus metrics port")
	externalTranscoderUrl := flag.String("external-transcoder", "", "URL for the external transcoder to be used by the pipeline coordinator. Only 1 implementation today for AWS MediaConvert which should be in the format: mediaconvert://key-id:key-secret@endpoint-host?region=aws-region&role=iam-role&s3_aux_bucket=s3://bucket")
	vodPipelineStrategy := flag.String("vod-pipeline-strategy", string(pipeline.StrategyCatalystDominance), "Which strategy to use for the VOD pipeline")
	flag.StringVar(&config.RecordingCallback, "recording", "http://recording.livepeer.com/recording/status", "Callback URL for recording start&stop events")
	metricsDBConnectionString := flag.String("metrics-db-connection-string", "", "Connection string to use for the metrics Postgres DB. Takes the form: host=X port=X user=X password=X dbname=X")
	flag.Parse()

	if *mistJson {
		mistconnector.PrintMistConfigJson("catalyst-api", "HTTP API server for translating Catalyst API requests into Mist calls", "Catalyst API", config.Version, flag.CommandLine)
		return
	}

	var (
		metricsDB *sql.DB
		err       error
	)

	go func() {
		log.Fatal(metrics.ListenAndServe(*promPort))
	}()

	mist := &clients.MistClient{
		ApiUrl:          fmt.Sprintf("http://localhost:%d/api2", *mistPort),
		HttpReqUrl:      fmt.Sprintf("http://localhost:%d", *mistHttpPort),
		TriggerCallback: fmt.Sprintf("http://localhost:%d/api/mist/trigger", *port),
	}
	// Kick off the callback client, to send job update messages on a regular interval
	statusClient := clients.NewPeriodicCallbackClient(15 * time.Second).Start()
	if metricsDBConnectionString != nil && *metricsDBConnectionString != "" {
		metricsDB, err = sql.Open("postgres", *metricsDBConnectionString)
		if err != nil {
			log.Fatalf("Error creating postgres metrics connection: %v", err)
		}
	} else {
		log.Println("Postgres metrics connection string was not set, postgres metrics are disabled.")
	}
	vodEngine, err := pipeline.NewCoordinator(pipeline.Strategy(*vodPipelineStrategy), mist, *externalTranscoderUrl, statusClient, metricsDB)
	if err != nil {
		log.Fatalf("Error creating VOD pipeline coordinator: %v", err)
	}

	if err := api.ListenAndServe(*port, *apiToken, vodEngine); err != nil {
		log.Fatal(err)
	}
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
