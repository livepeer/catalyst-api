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
	mistJson := flag.Bool("j", false, "Print application info as JSON. Used by Mist to present flags in its UI.")
	flag.Parse()

	if *mistJson {
		mistconnector.PrintMistConfigJson("catalyst-api", "HTTP API server for translating Catalyst API requests into Mist calls", "Catalyst API", config.Version, flag.CommandLine)
		return
	}

	if err := api.ListenAndServe(*port, *mistPort); err != nil {
		log.Fatal(err)
	}
}
