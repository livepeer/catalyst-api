package handlers

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/livepeer/go-tools/drivers"
)

func storePlaylist(destination, data string) {
	log.Printf("storePlaylist %s %s", destination, data)
	storageDriver, err := drivers.ParseOSURL(destination, true)
	if err != nil {
		log.Printf("error drivers.ParseOSURL %v %s", err, destination)
	}
	session := storageDriver.NewSession("")
	ctx := context.Background()
	_, err = session.SaveData(ctx, "", bytes.NewBuffer([]byte(data)), nil, 3*time.Second)
	if err != nil {
		log.Printf("error session.SaveData %v %s", err, destination)
	}
}
