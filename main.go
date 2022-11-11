package main

import (
	"flag"
	"log"

	"github.com/livepeer/catalyst-api/api"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
)

func main() {
	port := flag.Int("port", 4949, "Port to listen on")
	mistPort := flag.Int("mist-port", 4242, "Port to listen on")
	mistHttpPort := flag.Int("mist-http-port", 8080, "Port to listen on")
	apiToken := flag.String("api-token", "IAmAuthorized", "Auth header value for API access")
	mistJson := flag.Bool("j", false, "Print application info as JSON. Used by Mist to present flags in its UI.")
	flag.StringVar(&config.RecordingCallback, "recording", "http://recording.livepeer.com/recording/status", "Callback URL for recording start&stop events")
	flag.Parse()

	if *mistJson {
		mistconnector.PrintMistConfigJson("catalyst-api", "HTTP API server for translating Catalyst API requests into Mist calls", "Catalyst API", config.Version, flag.CommandLine)
		return
	}

	if err := api.ListenAndServe(*port, *mistPort, *mistHttpPort, *apiToken); err != nil {
		log.Fatal(err)
	}
}
