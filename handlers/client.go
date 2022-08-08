package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type MistClient struct {
	apiUrl          string
	triggerCallback string
}

func (mc *MistClient) AddStream(streamName, url string) error {
	command := commandAddStream(streamName, url)

	resp, err := mc.sendCommand(command)
	if err != nil {
		return err
	}
	if !validAddStreamResponse(streamName, resp) {
		return fmt.Errorf("stream '%s' could not be created", streamName)
	}
	return nil
}

func (mc *MistClient) PushStart(streamName, targetURL string) error {
	command := commandPushStart(streamName, targetURL)
	_, err := mc.sendCommand(command)
	return err
}

func (mc *MistClient) PushList(streamName string) (string, error) {
	// TODO: Do we need it at all?
	return "", nil
}

func (mc *MistClient) DeleteStream(streamName string) error {
	command := commandDeleteStream(streamName)
	_, err := mc.sendCommand(command)
	return err
}

func (mc *MistClient) RegisterTrigger(streamName, triggerName string) error {
	command := commandRegisterTrigger(streamName, triggerName, mc.triggerCallback)
	_, err := mc.sendCommand(command)
	return err
}

func (mc *MistClient) DeleteTrigger(streamName, triggerName string) (string, error) {
	command := commandDeleteTrigger(streamName, triggerName, mc.triggerCallback)
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

type addStreamCommand struct {
	Addstream map[string]Stream `json:"addstream"`
}

type Stream struct {
	Source string `json:"source"`
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

type pushStartCommand struct {
	PushStart PushStart `json:"push_start"`
}

type PushStart struct {
	Stream string `json:"stream"`
	Target string `json:"target"`
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

func commandDeleteTrigger(streamName, triggerName, handlerUrl string) configCommand {
	// TODO: Change removing all triggers to deleting only one single trigger
	triggersMap := map[string][]ConfigTrigger{}
	var triggers []ConfigTrigger
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

// TODO: Delete auth, since DMS API will run on localhost, so it does not need authentation
func auth(command string) string {
	return fmt.Sprintf("{%s,%s}", `"authorize":{"username":"test","password":"5a898af10a385029922a0981db4eab6d"}`, command)
}
