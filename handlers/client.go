package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"
)

type MistClient struct {
	apiUrl         string
	triggerHandler string
}

func (mc *MistClient) AddStream(url string) (string, error) {
	name := randomStreamName("catalyst_vod_")
	command := commandAddStream(name, url)

	resp, err := mc.sendCommand(command)
	if err != nil {
		return "", err
	}
	if !validAddStreamResponse(name, resp) {
		return "", fmt.Errorf("stream '%s' could not be created", name)
	}
	return name, nil
}

func (mc *MistClient) PushStart(name, target string) (string, error) {
	command := commandPushStart(name, target)
	resp, err := mc.sendCommand(command)
	if err != nil {
		return "", err
	}
	return resp, nil
}

func (mc *MistClient) PushList(name string) (string, error) {
	// TODO:?
	return "", nil
}

func (mc *MistClient) DeleteStream(name string) error {
	command := commandDeleteStream(name)
	_, err := mc.sendCommand(command)
	return err
}

func (mc *MistClient) RegisterTrigger(streamName, triggerName string) (string, error) {
	command := commandRegisterTrigger(streamName, triggerName, mc.triggerHandler)
	return mc.sendCommand(command)
}

func toCommandString(command interface{}) (string, error) {
	res, err := json.Marshal(command)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

func validAddStreamResponse(name, resp string) bool {
	result := struct {
		Streams map[string]interface{} `json:"streams"`
	}{}
	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return false
	}
	_, ok := result.Streams[name]
	return ok
}

func payloadFor(command string) string {
	return fmt.Sprintf("command=%s", url.QueryEscape(command))
}

type Stream struct {
	Source string `json:"source"`
}

type addStreamCommand struct {
	Addstream map[string]Stream `json:"addstream"`
}

func commandAddStream(name, url string) interface{} {
	return addStreamCommand{
		Addstream: map[string]Stream{
			name: {
				Source: url,
			},
		},
	}
}

type deleteStreamCommand struct {
	Deletestream map[string]interface{} `json:"deletestream"`
}

func commandDeleteStream(name string) deleteStreamCommand {
	return deleteStreamCommand{
		Deletestream: map[string]interface{}{name: nil},
	}
}

type PushStart struct {
	Stream string `json:"stream"`
	Target string `json:"target"`
}

type pushStartCommand struct {
	PushStart PushStart `json:"push_start"`
}

func commandPushStart(streamName, target string) pushStartCommand {
	return pushStartCommand{
		PushStart: PushStart{
			Stream: streamName,
			Target: target,
		},
	}
}

type configCommand struct {
	Config Config `json:"config"`
}

type ConfigTrigger struct {
	Handler string   `json:"handler"`
	Streams []string `json:"streams"`
	Sync    bool     `json:"sync"`
}

type Config struct {
	Triggers map[string][]ConfigTrigger `json:"triggers"`
}

func commandRegisterTrigger(streamName, triggerName, handlerUrl string) configCommand {
	// TODO: Read current trigger and add new one instead of overriding all triggers
	triggersMap := map[string][]ConfigTrigger{}
	var triggers []ConfigTrigger
	triggers = append(triggers, ConfigTrigger{
		Handler: handlerUrl,
		Streams: []string{streamName},
		Sync:    false,
	})
	triggersMap[triggerName] = triggers
	return configCommand{Config{Triggers: triggersMap}}
}

func (mc *MistClient) sendCommand(command interface{}) (string, error) {
	c, err := toCommandString(command)
	if err != nil {
		return "", err
	}
	payload := payloadFor(auth(c))
	resp, err := http.Post(mc.apiUrl, "application/json", bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), err
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

// TODO: Delete auth, since DMS API will run on localhost, so it does not need authentation
func auth(command string) string {
	return fmt.Sprintf("{%s,%s}", `"authorize":{"username":"test","password":"5a898af10a385029922a0981db4eab6d"}`, command)
}
