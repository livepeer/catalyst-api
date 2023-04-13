package main

import (
	"fmt"

	"github.com/livepeer/catalyst-api/events"
)

func main() {
	fmt.Printf("%s", events.Sign())
}
