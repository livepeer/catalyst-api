package handlers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/livepeer/catalyst-api/clients"
)

var CallbackClient = clients.NewCallbackClient()

type StreamInfo struct {
	callbackUrl string
}

type CatalystAPIHandlersCollection struct {
	MistClient  clients.MistAPIClient
	StreamCache map[string]StreamInfo
}

type MistCallbackHandlersCollection struct {
	MistClient  clients.MistAPIClient
	StreamCache map[string]StreamInfo
}

func randomStreamName(prefix string) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	const length = 8
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = charset[r.Intn(length)]
	}
	return fmt.Sprintf("%s%s", prefix, string(res))
}
