package misttriggers

import (
	"context"
	"fmt"
	"net/http"
)

func (d *MistCallbackHandlersCollection) TriggerPlaybackLog(ctx context.Context, w http.ResponseWriter, req *http.Request, payload MistTriggerBody) {
	fmt.Println("HELLO PLAYBACK LOG")
}
